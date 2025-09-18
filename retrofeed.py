# tools/retrofeed.py
"""
Retrofeed avançado — reenvia leads históricos não entregues para novos pixels.

Funcionalidades:
- Async-first (usa coroutines do db.py)
- Batch processing com tamanho configurável
- Descoberta automática de pixels (PIXEL_ID_1, PIXEL_ID_2, NEW_PIXELS_PARSED)
- Logs seguros (redaction)
- Métricas Prometheus
- Push para Redis Stream com maxlen/approximate
- Sincronização total com worker.py / utils.py
"""

import os
import time
import json
import traceback
import asyncio
from typing import Any, Dict, List, Optional

try:
    from redis import Redis
except Exception:
    Redis = None

# DB helpers assíncronos
from db import init_db, get_unsent_leads_for_pixel

# utils / configuração de pixels
from utils import NEW_PIXELS_PARSED, PIXEL_ID_1, PIXEL_ID_2

# ===========================
# Config / ENV
# ===========================
REDIS_URL = os.getenv("REDIS_URL", "")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")

BATCH_SIZE = int(os.getenv("RETROFEED_BATCH_SIZE", "500"))
SLEEP_INTERVAL = int(os.getenv("RETROFEED_INTERVAL", "60"))  # seconds
AUTO_MODE = os.getenv("RETROFEED_AUTO", "false").lower() in ("1", "true", "yes")
MAX_PUSH_PER_RUN = int(os.getenv("RETROFEED_MAX_PUSH", "5000"))

# Redis client
r = None
if Redis and REDIS_URL:
    try:
        r = Redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        r = None

# ===========================
# Prometheus metrics (opcional)
# ===========================
try:
    from prometheus_client import Counter
    metric_retrofeed_pushed = Counter("retrofeed_pushed_total", "Total retrofeed leads pushed", ["pixel"])
    metric_retrofeed_errors = Counter("retrofeed_errors_total", "Total retrofeed errors", ["pixel"])
except Exception:
    metric_retrofeed_pushed = None
    metric_retrofeed_errors = None

# ===========================
# Helpers
# ===========================
def _redact(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: ("REDACTED" if k.lower() in ("cookies", "_fbp", "_fbc", "email", "phone", "ph", "em") else _redact(vv)) for k, vv in v.items()}
    if isinstance(v, list):
        return [_redact(i) for i in v]
    return v

def _log(msg: str, obj: Any = None):
    try:
        if obj is not None:
            print(msg, json.dumps(_redact(obj), default=str))
        else:
            print(msg)
    except Exception:
        print(msg)

def _resolve_pixels() -> List[str]:
    """Resolve todos os pixels válidos da configuração."""
    pixels = []
    if PIXEL_ID_1:
        pixels.append(PIXEL_ID_1)
    if PIXEL_ID_2:
        pixels.append(PIXEL_ID_2)
    for entry in NEW_PIXELS_PARSED:
        pid = entry[0] if isinstance(entry, (list, tuple)) and entry else str(entry)
        if pid:
            pixels.append(pid)
    # fallback env RAW
    raw = os.getenv("NEW_PIXEL_IDS", "")
    for p in [x.strip() for x in raw.split(",") if x.strip()]:
        pid = p.split(":", 1)[0].strip() if ":" in p else p
        pixels.append(pid)
    return list(dict.fromkeys([p for p in pixels if p]))  # unique, preserve order

# ===========================
# Retrofeed core
# ===========================
async def retrofeed_for_pixel(pixel_id: str, batch_size: int = BATCH_SIZE, push_limit: Optional[int] = None) -> int:
    """Busca leads não entregues e envia para Redis stream."""
    if not r:
        _log("[RETRO_NO_REDIS]")
        return 0

    pushed = 0
    try:
        unsent = await get_unsent_leads_for_pixel(pixel_id, limit=batch_size)
    except Exception as e:
        _log("[RETRO_DB_ERROR]", {"pixel": pixel_id, "error": str(e)})
        if metric_retrofeed_errors:
            metric_retrofeed_errors.labels(pixel_id).inc()
        return 0

    if not unsent:
        _log("[RETRO_EMPTY]", {"pixel": pixel_id})
        return 0

    for lead in unsent:
        if push_limit and pushed >= push_limit:
            break
        try:
            payload = {}
            for k, v in lead.items():
                payload[k] = json.dumps(v, default=str) if isinstance(v, (dict, list)) else ("" if v is None else str(v))

            # indica pixel alvo específico para worker
            payload["retro_target_pixel"] = pixel_id

            r.xadd(STREAM, payload, maxlen=2_000_000, approximate=True)
            pushed += 1
        except Exception as e:
            _log("[RETRO_PUSH_ERROR]", {"pixel": pixel_id, "error": str(e), "event_key": lead.get("event_key")})
            if metric_retrofeed_errors:
                metric_retrofeed_errors.labels(pixel_id).inc()
            traceback.print_exc()

    _log("[RETRO_PUSH_OK]", {"pixel": pixel_id, "count": pushed})
    if metric_retrofeed_pushed:
        metric_retrofeed_pushed.labels(pixel_id).inc(pushed)
    return pushed

async def retrofeed_all(pixels: Optional[List[str]] = None, once: bool = True, batch_size: int = BATCH_SIZE, max_per_pixel: Optional[int] = None):
    """Executa retrofeed para todos pixels detectados ou fornecidos."""
    init_db()
    pixels = pixels or _resolve_pixels()
    if not pixels:
        _log("[RETRO_NO_PIXELS]")
        return

    _log("[RETRO_START]", {"pixels": pixels, "auto": not once, "batch_size": batch_size, "max_per_pixel": max_per_pixel})
    while True:
        total = 0
        for pid in pixels:
            pushed = await retrofeed_for_pixel(pid, batch_size=batch_size, push_limit=max_per_pixel)
            total += pushed
            await asyncio.sleep(0.05)  # throttle
        _log("[RETRO_CYCLE_DONE]", {"total_pushed": total})
        if once:
            break
        await asyncio.sleep(SLEEP_INTERVAL)

# ===========================
# CLI / env entrypoint
# ===========================
def _parse_args_from_env():
    pixels_env = os.getenv("RETRO_PIXELS", "")
    pixels = [p.strip() for p in pixels_env.split(",") if p.strip()] if pixels_env else None
    once_env = os.getenv("RETRO_ONCE", "")
    once = True if once_env.lower() in ("1", "true", "yes") else (not AUTO_MODE)
    batch_size = int(os.getenv("RETRO_BATCH_SIZE", str(BATCH_SIZE)))
    max_per_pixel = os.getenv("RETRO_MAX_PER_PIXEL")
    max_per_pixel_val = int(max_per_pixel) if max_per_pixel and max_per_pixel.isdigit() else None
    return pixels, once, batch_size, max_per_pixel_val

if __name__ == "__main__":
    try:
        pixels, once, batch_size, max_per_pixel_val = _parse_args_from_env()
        if AUTO_MODE and os.getenv("RETRO_ONCE", "") == "":
            once = False
        asyncio.run(retrofeed_all(pixels=pixels, once=once, batch_size=batch_size, max_per_pixel=max_per_pixel_val))
    except KeyboardInterrupt:
        _log("[RETRO_STOPPED]")
    except Exception as exc:
        _log("[RETRO_FATAL]", {"error": str(exc)})
        traceback.print_exc()