import os, aiohttp, asyncio, json, logging
from utils import build_fb_payload, build_ga4_payload

# =========================
# Configurações de ENV
# =========================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v20.0")
FB_PIXEL_ID = os.getenv("FB_PIXEL_ID")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")

GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "")
GA4_API_SECRET = os.getenv("GA4_API_SECRET", "")
GOOGLE_ENABLED = bool(GA4_MEASUREMENT_ID and GA4_API_SECRET)

FB_RETRY_MAX = int(os.getenv("FB_RETRY_MAX", "3"))

logger = logging.getLogger("fb_google")
logger.setLevel(logging.INFO)

# =========================
# Helper de retry
# =========================
async def post_with_retry(session, url, payload, retries=3, platform="fb", et=None):
    last_err = None
    for i in range(retries):
        try:
            async with session.post(url, json=payload, timeout=20) as resp:
                txt = await resp.text()
                if resp.status in (200, 201, 204):
                    return {"ok": True, "status": resp.status, "body": txt}
                else:
                    last_err = f"{resp.status}: {txt}"
        except Exception as e:
            last_err = str(e)
        await asyncio.sleep(2 * (i+1))
    return {"ok": False, "error": last_err, "platform": platform, "event": et}

# =========================
# Envio para Facebook CAPI
# =========================
async def send_event_fb(event_name: str, lead: dict):
    if not FB_PIXEL_ID or not FB_ACCESS_TOKEN:
        return {"skip": True, "reason": "fb creds missing"}

    payload = build_fb_payload(FB_PIXEL_ID, event_name, lead)
    url = f"https://graph.facebook.com/{FB_API_VERSION}/{FB_PIXEL_ID}/events?access_token={FB_ACCESS_TOKEN}"

    async with aiohttp.ClientSession() as session:
        return await post_with_retry(session, url, payload, retries=FB_RETRY_MAX, platform="facebook", et=event_name)

# =========================
# Envio para Google GA4
# =========================
async def send_event_google(event_name: str, lead: dict):
    if not GOOGLE_ENABLED:
        return {"skip": True, "reason": "google disabled"}

    payload = build_ga4_payload(event_name, lead)
    url = f"https://www.google-analytics.com/mp/collect?measurement_id={GA4_MEASUREMENT_ID}&api_secret={GA4_API_SECRET}"

    async with aiohttp.ClientSession() as session:
        return await post_with_retry(session, url, payload, retries=3, platform="ga4", et=event_name)

# =========================
# Função principal unificada
# =========================
async def send_event_to_all(lead: dict, et: str = "Lead"):
    """
    Dispara evento (Lead/Subscribe) para:
      - Facebook (sempre)
      - Google GA4 (se configurado)
    """
    results = {"facebook": await send_event_fb(et, lead)}
    if GOOGLE_ENABLED:
        results["google"] = await send_event_google(et, lead)
    return results

# =========================
# Retry wrapper (usado pelo bot.py)
# =========================
async def send_event_with_retry(event_type: str, lead: dict, retries=5, delay=2):
    attempt = 0
    while attempt < retries:
        try:
            results = await send_event_to_all(lead, et=event_type)
            if any(r.get("ok") for r in results.values() if isinstance(r, dict)):
                logger.info(json.dumps({
                    "event": event_type,
                    "telegram_id": lead.get("telegram_id"),
                    "status": "success"
                }))
                return {"status": "success", "results": results}
        except Exception as e:
            logger.warning(f"[send_event_with_retry] tentativa {attempt+1} falhou: {e}")
        attempt += 1
        await asyncio.sleep(delay ** attempt)
    logger.error(json.dumps({
        "event": event_type,
        "telegram_id": lead.get("telegram_id"),
        "status": "failed"
    }))
    return {"status": "failed", "event": event_type}

# =========================
# Queue de eventos
# =========================
_event_queue = asyncio.Queue()

async def enqueue_event(event_type: str, lead: dict):
    """Coloca evento na fila para envio posterior"""
    await _event_queue.put((event_type, lead))
    logger.info(f"[QUEUE_ENQ] {event_type} -> {lead.get('telegram_id')}")

async def process_event_queue():
    """Processa continuamente eventos da fila"""
    while True:
        event_type, lead = await _event_queue.get()
        await send_event_with_retry(event_type, lead)
        _event_queue.task_done()