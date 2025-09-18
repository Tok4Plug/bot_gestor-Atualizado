# utils.py (versão avançada e sincronizada)
import os
import time
import json
import requests
import traceback
import hashlib
from typing import Dict, Any, List, Tuple, Optional

# =============================
# Configurações
# =============================
FB_API_VERSION = os.getenv("FB_API_VERSION", "v19.0")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "12"))
FB_RETRY_MAX = int(os.getenv("FB_RETRY_MAX", "3"))
FB_RETRY_BACKOFF_BASE = float(os.getenv("FB_RETRY_BACKOFF_BASE", "0.5"))
FB_BATCH_MAX = int(os.getenv("FB_BATCH_MAX", "200"))

PIXEL_ID_1 = os.getenv("PIXEL_ID_1")
ACCESS_TOKEN_1 = os.getenv("ACCESS_TOKEN_1")
PIXEL_ID_2 = os.getenv("PIXEL_ID_2")
ACCESS_TOKEN_2 = os.getenv("ACCESS_TOKEN_2")

NEW_PIXEL_IDS_RAW = os.getenv("NEW_PIXEL_IDS", "")
NEW_PIXELS = [p.strip() for p in NEW_PIXEL_IDS_RAW.split(",") if p.strip()]

TYPEBOT_API_URL = os.getenv("TYPEBOT_API_URL")
TYPEBOT_API_KEY = os.getenv("TYPEBOT_API_KEY")

# =============================
# Parsing de novos pixels
# =============================
def _parse_new_pixels() -> List[Tuple[str, Optional[str]]]:
    out = []
    for item in NEW_PIXELS:
        if ":" in item:
            pid, token = item.split(":", 1)
            out.append((pid.strip(), token.strip()))
        else:
            out.append((item, None))
    return out

NEW_PIXELS_PARSED = _parse_new_pixels()

# =============================
# Helpers
# =============================
def _hash_sha256(val: str) -> str:
    return hashlib.sha256(val.strip().lower().encode()).hexdigest()

def _normalize_and_hash(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza emails, telefones, Telegram IDs e adiciona parâmetros de tracking"""
    out = {}
    if "email" in data and data["email"]:
        out["em"] = _hash_sha256(data["email"])
    if "phone" in data and data["phone"]:
        out["ph"] = _hash_sha256(data["phone"])
    if "telegram_id" in data and data["telegram_id"]:
        out["external_id"] = _hash_sha256(str(data["telegram_id"]))

    # dados brutos
    if "ip" in data and data["ip"]:
        out["client_ip_address"] = data["ip"]
    if "ua" in data and data["ua"]:
        out["client_user_agent"] = data["ua"]
    if "fbp" in data and data["fbp"]:
        out["_fbp"] = data["fbp"]
    if "fbc" in data and data["fbc"]:
        out["_fbc"] = data["fbc"]

    # Google Ads IDs
    if "gclid" in data and data["gclid"]:
        out["gclid"] = data["gclid"]
    if "wbraid" in data and data["wbraid"]:
        out["wbraid"] = data["wbraid"]
    if "gbraid" in data and data["gbraid"]:
        out["gbraid"] = data["gbraid"]

    return out

def _redact_sensitive(obj: Any) -> Any:
    if isinstance(obj, dict):
        redacted = {}
        for k, v in obj.items():
            if k.lower() in ("cookies", "_fbp", "_fbc", "email", "phone", "ph", "em"):
                redacted[k] = "REDACTED"
            else:
                redacted[k] = _redact_sensitive(v)
        return redacted
    if isinstance(obj, list):
        return [_redact_sensitive(i) for i in obj]
    return obj

def _safe_log(msg: str, obj: Any = None):
    try:
        if obj is not None:
            print(msg, json.dumps(_redact_sensitive(obj), default=str))
        else:
            print(msg)
    except Exception:
        print(msg, "(log error)")

# =============================
# Integração com Typebot
# =============================
def fetch_typebot_user_data(session_id: str) -> Dict[str, Any]:
    """Busca dados adicionais do usuário direto do Typebot"""
    if not TYPEBOT_API_URL or not TYPEBOT_API_KEY:
        return {}

    try:
        url = f"{TYPEBOT_API_URL}/sessions/{session_id}"
        headers = {"Authorization": f"Bearer {TYPEBOT_API_KEY}"}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            data = r.json()
            return {
                "telegram_id": data.get("telegramId"),
                "ip": data.get("ip"),
                "ua": data.get("userAgent"),
                "email": data.get("email"),
                "phone": data.get("phone"),
                "fbp": data.get("fbp"),
                "fbc": data.get("fbc"),
                "click_id": data.get("clickId"),
                "gclid": data.get("gclid"),
                "wbraid": data.get("wbraid"),
                "gbraid": data.get("gbraid"),
            }
    except Exception as e:
        _safe_log("[TYPEBOT_FETCH_ERROR]", {"error": str(e)})
    return {}

# =============================
# POST síncrono com retry
# =============================
def _post_facebook_sync(pixel_id: str, access_token: str, payload: Dict[str, Any], retries: int = FB_RETRY_MAX) -> Dict[str, Any]:
    if not pixel_id or not access_token:
        return {"ok": False, "error": "pixel_or_token_missing", "pixel_id": pixel_id}

    url = f"https://graph.facebook.com/{FB_API_VERSION}/{pixel_id}/events"
    backoff = FB_RETRY_BACKOFF_BASE
    attempt = 0

    while attempt <= retries:
        attempt += 1
        try:
            r = requests.post(url, json=payload, params={"access_token": access_token}, timeout=REQUEST_TIMEOUT)
            status = r.status_code
            try:
                body = r.json()
            except Exception:
                body = r.text

            if 200 <= status < 300:
                _safe_log("[FB POST OK]", {"pixel": pixel_id, "attempt": attempt, "status": status})
                return {"ok": True, "status_code": status, "body": body, "attempts": attempt}
            else:
                _safe_log("[FB POST WARN]", {"pixel": pixel_id, "attempt": attempt, "status": status, "body": body})

        except Exception as e:
            _safe_log("[FB POST EXCEPTION]", {"pixel": pixel_id, "attempt": attempt, "error": str(e)})
            traceback.print_exc()

        time.sleep(backoff)
        backoff *= 2

    _safe_log("[FB POST FAILED AFTER RETRIES]", {"pixel": pixel_id, "attempts": attempt})
    return {"ok": False, "status_code": None, "body": "failed_after_retries", "attempts": attempt}

# =============================
# Envio para múltiplos pixels
# =============================
def send_to_pixels(event_payload: Dict[str, Any], platform: str = "fb2", pixels: Optional[List[Tuple[str, Optional[str]]]] = None) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    send_list: List[Tuple[str, Optional[str]]] = []

    if pixels:
        send_list.extend(pixels)

    def _add_if_not_present(pid: Optional[str], token: Optional[str]):
        if not pid:
            return
        for p, _ in send_list:
            if p == pid:
                return
        send_list.append((pid, token))

    _add_if_not_present(PIXEL_ID_1, ACCESS_TOKEN_1)
    _add_if_not_present(PIXEL_ID_2, ACCESS_TOKEN_2)
    for p in NEW_PIXELS_PARSED:
        _add_if_not_present(p[0], p[1])

    # Ajuste estratégico: Purchase no Fb2 sempre R$12
    if platform.lower() == "fb2" and event_payload["data"][0].get("event_name") == "Purchase":
        event_payload["data"][0]["custom_data"]["value"] = 12.0
        event_payload["data"][0]["custom_data"]["currency"] = "BRL"

    for pid, token in send_list:
        resolved_token = token
        if not resolved_token:
            if pid == PIXEL_ID_1 and ACCESS_TOKEN_1:
                resolved_token = ACCESS_TOKEN_1
            elif pid == PIXEL_ID_2 and ACCESS_TOKEN_2:
                resolved_token = ACCESS_TOKEN_2
            else:
                resolved_token = os.getenv(f"ACCESS_TOKEN_{pid}")

        _safe_log("[SENDING_EVENT_TO_PIXEL]", {
            "pixel": pid,
            "event_name": event_payload["data"][0].get("event_name"),
            "event_time": event_payload["data"][0].get("event_time")
        })

        try:
            res = _post_facebook_sync(pid, resolved_token, event_payload)
        except Exception as e:
            _safe_log("[SEND_EXCEPTION]", {"pixel": pid, "error": str(e)})
            res = {"ok": False, "error": str(e)}

        results[pid] = res

    return results

def send_event_to_all(event_payload: Dict[str, Any], platform: str = "fb2") -> Dict[str, Dict[str, Any]]:
    return send_to_pixels(event_payload, platform=platform)

# =============================
# Builder de payload FB
# =============================
def build_fb_event(event_name: str, lead_data: Dict[str, Any], platform: str = "fb2", custom_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    # Enriquecimento com dados do Typebot
    if "session_id" in lead_data:
        tb_data = fetch_typebot_user_data(lead_data["session_id"])
        lead_data["user_data"] = {**tb_data, **lead_data.get("user_data", {})}

    user_data = _normalize_and_hash(lead_data.get("user_data", {}))

    payload = {
        "data": [
            {
                "event_name": event_name,
                "event_time": int(time.time()),
                "event_source_url": lead_data.get("src_url"),
                "user_data": user_data,
                "custom_data": custom_data or {
                    "currency": lead_data.get("currency", "BRL"),
                    "value": lead_data.get("value") or 0,
                    "click_id": lead_data.get("click_id"),
                    "gclid": lead_data.get("gclid"),
                    "wbraid": lead_data.get("wbraid"),
                    "gbraid": lead_data.get("gbraid"),
                    "content_category": lead_data.get("category"),
                    "content_name": lead_data.get("content_name")
                }
            }
        ]
    }

    if platform.lower() == "fb2" and event_name == "Purchase":
        payload["data"][0]["custom_data"]["value"] = 12.0
        payload["data"][0]["custom_data"]["currency"] = "BRL"

    return payload