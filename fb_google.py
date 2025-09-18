# fb_google.py (versão avançada e sincronizada)
import os, time, random, traceback, asyncio, aiohttp, base64, json
from collections import deque
from typing import List, Dict, Any, Optional
from queue import PriorityQueue
from prometheus_client import Counter, Gauge, Histogram

# =============================
# Imports da estrutura
# =============================
try:
    from utils import build_fb_event
    from utils import PIXEL_ID_1, PIXEL_ID_2, NEW_PIXELS_PARSED, ACCESS_TOKEN_1, ACCESS_TOKEN_2
except Exception:
    PIXEL_ID_1 = os.getenv("PIXEL_ID_1")
    PIXEL_ID_2 = os.getenv("PIXEL_ID_2")
    ACCESS_TOKEN_1 = os.getenv("ACCESS_TOKEN_1")
    ACCESS_TOKEN_2 = os.getenv("ACCESS_TOKEN_2")
    NEW_PIXELS_PARSED = [p.strip() for p in os.getenv("NEW_PIXEL_IDS", "").split(",") if p.strip()]

# DB opcional
try:
    from db import SessionLocal, EventLog
    _HAS_DB_EVENTLOG = True
except Exception:
    try:
        from db import SessionLocal
        _HAS_DB_EVENTLOG = False
    except Exception:
        SessionLocal = None
        _HAS_DB_EVENTLOG = False

# =============================
# Configurações
# =============================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v19.0")
GA_MEASUREMENT_ID = os.getenv("GA_MEASUREMENT_ID")
GA_API_SECRET = os.getenv("GA_API_SECRET")

FB_BATCH_MAX = int(os.getenv("FB_BATCH_MAX", "200"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))

MAX_QUEUE_SIZE = int(os.getenv("FB_EVENT_QUEUE_MAX", "5000"))
BATCH_PROCESS_INTERVAL = float(os.getenv("FB_BATCH_INTERVAL", "0.05"))
FB_RETRIES = int(os.getenv("FB_RETRIES", "3"))
GA_RETRIES = int(os.getenv("GA_RETRIES", "3"))

# =============================
# Métricas Prometheus
# =============================
events_sent = Counter("events_sent_total", "Eventos enviados", ["platform", "event_type", "pixel"])
events_failed = Counter("events_failed_total", "Eventos falhados", ["platform", "event_type", "pixel"])
queue_size_gauge = Gauge("event_queue_size", "Fila de eventos")
latency_histogram = Histogram("event_latency_seconds", "Latência", ["platform"])

# =============================
# Controle de fluxo
# =============================
class TokenBucket:
    def __init__(self, rate, capacity=None):
        self.rate = rate
        self.capacity = capacity or max(1.0, rate)
        self.tokens, self.last = self.capacity, time.time()
    def consume(self, tokens=1.0):
        now = time.time()
        self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    async def wait_for(self, tokens=1.0):
        while not self.consume(tokens):
            await asyncio.sleep(0.01)

class CircuitBreaker:
    def __init__(self, threshold=5, recovery=30):
        self.failures, self.last_fail, self.opened = 0, 0.0, False
        self.threshold, self.recovery = threshold, recovery
    def record_success(self): self.failures, self.opened = 0, False
    def record_failure(self):
        self.failures += 1; self.last_fail = time.time()
        if self.failures >= self.threshold: self.opened = True
    def allow(self): return not (self.opened and time.time()-self.last_fail < self.recovery)

_fb_bucket = TokenBucket(float(os.getenv("FB_RPS", "20")))
_google_bucket = TokenBucket(float(os.getenv("GOOGLE_RPS", "20")))
_fb_breaker, _google_breaker = CircuitBreaker(), CircuitBreaker()

# =============================
# Filas
# =============================
event_queue: "PriorityQueue[tuple[int, dict]]" = PriorityQueue()
dead_letter_queue: deque = deque(maxlen=2000)

async def enqueue_event(lead_data: dict, event_type="Lead", score=100):
    """Adiciona evento à fila com prioridade"""
    if event_queue.qsize() < MAX_QUEUE_SIZE:
        event_queue.put((-score, {"lead_data": lead_data, "event_type": event_type}))
    else:
        dead_letter_queue.append({"lead_data": lead_data, "event_type": event_type, "reason": "queue_full"})

# =============================
# Persistência opcional
# =============================
def save_event_db(ld: dict, et: str, status: str, platform: str, resp: Optional[dict] = None):
    try:
        if not SessionLocal: return
        db = SessionLocal()
        if _HAS_DB_EVENTLOG:
            log = EventLog(
                telegram_id=ld.get("telegram_id"),
                event_type=et,
                platform=platform,
                status=status,
                value=ld.get("value"),
                raw_data=ld,
                response=resp or {}
            )
            db.add(log); db.commit()
        db.close()
    except Exception:
        pass

# =============================
# POST com retry
# =============================
async def post_with_retry(session, url, payload, params=None, retries=3, platform="fb", et="Lead", px="", ld=None):
    backoff = 0.5
    for _ in range(retries+1):
        try:
            start = time.time()
            async with session.post(url, json=payload, params=params or {}, timeout=REQUEST_TIMEOUT) as r:
                txt = await r.text()
                latency_histogram.labels(platform).observe(time.time()-start)
                if r.status in (200, 201):
                    events_sent.labels(platform, et, px).inc()
                    save_event_db(ld or {}, et, "success", platform, {"status": r.status})
                    return {"ok": True}
                events_failed.labels(platform, et, px).inc()
        except Exception:
            traceback.print_exc()
        await asyncio.sleep(backoff); backoff *= 2
    dead_letter_queue.append({"url": url, "payload": payload, "pixel": px, "event": et})
    save_event_db(ld or {}, et, "failed", platform, {"reason": "max_retries"})
    return {"ok": False}

# =============================
# Envio para Facebook
# =============================
async def send_events_fb(events: List[Dict[str, Any]], et="Lead", ld=None, target_pixel=None):
    async with aiohttp.ClientSession() as s:
        results = {}
        pixels = []
        if PIXEL_ID_1 and ACCESS_TOKEN_1: pixels.append((PIXEL_ID_1, ACCESS_TOKEN_1))
        if PIXEL_ID_2 and ACCESS_TOKEN_2: pixels.append((PIXEL_ID_2, ACCESS_TOKEN_2))
        for p, tk in NEW_PIXELS_PARSED:
            pixels.append((p, tk))

        for pid, tk in pixels:
            if target_pixel and pid != target_pixel: 
                continue

            # regra estratégica: Fb1 nunca recebe Purchase
            if pid == PIXEL_ID_1 and et.lower() == "purchase":
                continue

            url = f"https://graph.facebook.com/{FB_API_VERSION}/{pid}/events"
            await _fb_bucket.wait_for()
            results[pid] = await post_with_retry(
                s, url, {"data": events}, {"access_token": tk},
                FB_RETRIES, "fb", et, pid, ld
            )
        return results

# =============================
# Envio para Google Analytics 4
# =============================
async def send_event_google(et: str, ld: dict):
    if not GA_MEASUREMENT_ID or not GA_API_SECRET: 
        return {"ga4": {"ok": False}}
    await _google_bucket.wait_for()

    payload = {
        "client_id": str(ld.get("telegram_id") or random.randint(1000, 9999)),
        "events": [{
            "name": et.lower(),
            "params": {
                "value": ld.get("value", 0),
                "currency": "BRL",
                "gclid": ld.get("gclid"),
                "wbraid": ld.get("wbraid"),
                "gbraid": ld.get("gbraid"),
                "telegram_id": ld.get("telegram_id")
            }
        }]
    }

    async with aiohttp.ClientSession() as s:
        return {"ga4": await post_with_retry(
            s,
            f"https://www.google-analytics.com/mp/collect?measurement_id={GA_MEASUREMENT_ID}&api_secret={GA_API_SECRET}",
            payload, retries=GA_RETRIES, platform="ga", et=et, ld=ld
        )}

# =============================
# Unified send (Facebook + GA4)
# =============================
async def send_event_to_all(ld: dict, et="Lead"):
    fb_event = [build_fb_event(et, ld, platform="fb2")]  # usa utils para payload enriquecido
    res = await send_events_fb(fb_event, et, ld, target_pixel=ld.get("retro_target_pixel"))
    res.update(await send_event_google(et, ld))
    return res

# =============================
# Processor
# =============================
async def process_event_queue():
    while True:
        try:
            queue_size_gauge.set(event_queue.qsize())
            batch = []
            while not event_queue.empty() and len(batch) < FB_BATCH_MAX:
                _, e = event_queue.get()
                batch.append(e)
            if batch:
                tasks = [send_event_to_all(b["lead_data"], b["event_type"]) for b in batch]
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(BATCH_PROCESS_INTERVAL)
        except Exception:
            traceback.print_exc()
            await asyncio.sleep(1)

# =============================
# Dry-run (debug)
# =============================
async def dry_run(ld: dict, et="Lead"):
    print("[DRY RUN]", build_fb_event(et, ld, platform="fb2"))