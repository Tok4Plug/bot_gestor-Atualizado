# db.py (versão 100% avançada com retroalimentação FB + GA4)
import os
import asyncio
import json
import time
import hashlib
import base64
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean,
    DateTime, Float, Text, func
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# ==============================
# Logging centralizado
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("db")

# ==============================
# Criptografia (Fernet com fallback base64)
# ==============================
CRYPTO_KEY = os.getenv("CRYPTO_KEY")
_use_fernet, _fernet = False, None
try:
    if CRYPTO_KEY:
        from cryptography.fernet import Fernet
        derived = base64.urlsafe_b64encode(hashlib.sha256(CRYPTO_KEY.encode()).digest())
        _fernet = Fernet(derived)
        _use_fernet = True
        logger.info("✅ Crypto: Fernet habilitado")
except Exception as e:
    logger.warning(f"⚠️ Fernet indisponível, usando fallback base64: {e}")

def _encrypt_value(s: str) -> str:
    if not s:
        return s
    try:
        return _fernet.encrypt(s.encode()).decode() if _use_fernet else base64.b64encode(s.encode()).decode()
    except Exception:
        return base64.b64encode(s.encode()).decode()

def _decrypt_value(s: str) -> str:
    if not s:
        return s
    try:
        return _fernet.decrypt(s.encode()).decode() if _use_fernet else base64.b64decode(s.encode()).decode()
    except Exception:
        try:
            return base64.b64decode(s.encode()).decode()
        except Exception:
            return s

def _safe_dict(d, decrypt: bool = False):
    """Devolve dict (opcionalmente descriptografado)."""
    if not isinstance(d, dict):
        return {}
    if not decrypt:
        return d
    out = {}
    for k, v in d.items():
        try:
            out[k] = _decrypt_value(v) if isinstance(v, str) else v
        except Exception:
            out[k] = v
    return out

# ==============================
# Configuração DB
# ==============================
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("POSTGRES_URL")
    or os.getenv("POSTGRESQL_URL")
)
if not DATABASE_URL:
    logger.error("⚠️ DATABASE_URL não configurado. DB desativado.")

engine = create_engine(
    DATABASE_URL,
    pool_size=int(os.getenv("DB_POOL_SIZE", 50)),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", 150)),
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True,
) if DATABASE_URL else None

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False) if engine else None
Base = declarative_base() if engine else None

# ==============================
# Modelo Buyer
# ==============================
class Buyer(Base):
    __tablename__ = "buyers"

    id = Column(Integer, primary_key=True, index=True)
    sid = Column(String(128), index=True, nullable=False)
    event_key = Column(String(128), unique=True, nullable=False, index=True)

    src_url = Column(Text, nullable=True)
    cid = Column(String(128), nullable=True)      # Client/Click ID (GA/Ads)
    gclid = Column(String(256), nullable=True)    # Google Ads
    fbclid = Column(String(256), nullable=True)   # Facebook Ads
    value = Column(Float, nullable=True)

    source = Column(String(32), nullable=True, default="bot")

    # Dados enriquecidos
    user_data = Column(JSONB, nullable=False, default={})
    custom_data = Column(JSONB, nullable=True, default={})

    # Rastreio
    cookies = Column(JSONB, nullable=True)
    postal_code = Column(String(32), nullable=True)
    device_info = Column(JSONB, nullable=True)
    session_metadata = Column(JSONB, nullable=True)

    # Envio/histórico
    sent_pixels = Column(JSONB, nullable=True, default=list)  # lista de pixel_ids já enviados
    event_history = Column(JSONB, nullable=True, default=list)

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    sent = Column(Boolean, default=False, index=True)
    attempts = Column(Integer, default=0)
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    last_sent_at = Column(DateTime(timezone=True), nullable=True)

# ==============================
# Init
# ==============================
def init_db():
    if not engine:
        return
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ DB inicializado e tabelas sincronizadas")
    except SQLAlchemyError as e:
        logger.error(f"Erro init DB: {e}")

# ==============================
# Priority Score
# ==============================
def compute_priority_score(user_data: Dict[str, Any], custom_data: Dict[str, Any]) -> float:
    score = 0.0
    user_data = user_data or {}
    custom_data = custom_data or {}
    if user_data.get("username"): score += 2
    if user_data.get("first_name"): score += 1
    if user_data.get("premium"): score += 2
    try:
        score += float(custom_data.get("purchase_count") or 0) * 3
    except Exception:
        pass
    return score

# ==============================
# Save Lead (merge + retries)
# ==============================
async def save_lead(data: dict, event_record: Optional[dict] = None, retries: int = 3) -> bool:
    if not SessionLocal:
        logger.warning("DB desativado - save_lead ignorado")
        return False

    loop = asyncio.get_event_loop()

    def db_sync():
        nonlocal retries
        while retries > 0:
            session = SessionLocal()
            try:
                ek = data.get("event_key")
                telegram_id = (data.get("user_data") or {}).get("telegram_id") or data.get("telegram_id")
                if not ek or not telegram_id:
                    return False

                buyer = session.query(Buyer).filter(Buyer.event_key == ek).first()

                normalized_ud = data.get("user_data") or {"telegram_id": telegram_id}
                for key in [
                    "premium", "lang", "origin", "user_agent",
                    "ip_address", "utm_source", "utm_medium",
                    "utm_campaign", "referrer"
                ]:
                    if data.get(key) is not None:
                        normalized_ud[key] = data.get(key)

                device_info = data.get("device_info") or {}
                session_metadata = data.get("session_metadata") or {"ts": int(time.time()), "source": data.get("source") or "bot"}

                custom = data.get("custom_data") or {}
                custom.setdefault("purchase_count", 0)
                custom["priority_score"] = compute_priority_score(normalized_ud, custom)

                cookies_in = data.get("cookies")
                enc_cookies = None
                if cookies_in:
                    enc_cookies = {k: _encrypt_value(str(v)) for k, v in (cookies_in.items() if isinstance(cookies_in, dict) else [])}

                if buyer:
                    # merge
                    buyer.user_data = {**(buyer.user_data or {}), **normalized_ud}
                    buyer.custom_data = {**(buyer.custom_data or {}), **custom}
                    buyer.src_url = data.get("src_url") or buyer.src_url
                    buyer.cid = data.get("cid") or buyer.cid
                    buyer.gclid = data.get("gclid") or buyer.gclid
                    buyer.fbclid = data.get("fbclid") or buyer.fbclid
                    buyer.value = data.get("value") or buyer.value
                    buyer.source = data.get("source") or buyer.source

                    if enc_cookies:
                        ec = buyer.cookies or {}
                        ec.update(enc_cookies)
                        buyer.cookies = ec

                    buyer.device_info = device_info or buyer.device_info
                    buyer.session_metadata = {**(buyer.session_metadata or {}), **session_metadata}

                    incoming_sent = data.get("sent_pixels") or []
                    if incoming_sent:
                        buyer.sent_pixels = list(set((buyer.sent_pixels or []) + incoming_sent))

                    if event_record:
                        eh = buyer.event_history or []
                        eh.append(event_record)
                        buyer.event_history = eh

                else:
                    # novo
                    buyer = Buyer(
                        sid=data.get("sid") or f"tg-{telegram_id}-{int(datetime.now().timestamp())}",
                        event_key=ek,
                        src_url=data.get("src_url"),
                        cid=data.get("cid"),
                        gclid=data.get("gclid"),
                        fbclid=data.get("fbclid"),
                        value=data.get("value"),
                        source=data.get("source") or "bot",
                        user_data=normalized_ud,
                        custom_data=custom,
                        cookies=enc_cookies,
                        postal_code=data.get("postal_code"),
                        device_info=device_info,
                        session_metadata=session_metadata,
                        sent_pixels=list(data.get("sent_pixels") or []),
                        event_history=[event_record] if event_record else []
                    )
                    session.add(buyer)

                if event_record:
                    if event_record.get("status") == "success":
                        buyer.last_sent_at = datetime.now(timezone.utc)
                        buyer.sent = True
                    elif event_record.get("status") == "failed":
                        buyer.last_attempt_at = datetime.now(timezone.utc)
                        buyer.attempts = (buyer.attempts or 0) + 1

                session.commit()
                return True
            except OperationalError as e:
                session.rollback()
                retries -= 1
                logger.warning(f"Conexão DB falhou, retry... ({retries} left) {e}")
                time.sleep(1)
            except Exception as e:
                session.rollback()
                logger.error(f"Erro save_lead: {e}")
                return False
            finally:
                session.close()
        return False

    return await loop.run_in_executor(None, db_sync)

# ==============================
# Retrofeed helpers usados pelo worker
# ==============================
async def get_unsent_leads_for_pixel(pixel_id: str, limit: int = 500) -> List[Dict[str, Any]]:
    """Todos os buyers cujo sent_pixels NÃO contém pixel_id."""
    if not SessionLocal:
        return []
    loop = asyncio.get_event_loop()

    def db_sync():
        session = SessionLocal()
        try:
            rows = (
                session.query(Buyer)
                .filter((Buyer.sent_pixels.is_(None)) | (func.not_(func.jsonb_contains(Buyer.sent_pixels, json.dumps([pixel_id])))))
                .order_by(Buyer.created_at.asc())
                .limit(limit)
                .all()
            )
            out = []
            for r in rows:
                out.append({
                    "event_key": r.event_key,
                    "telegram_id": (r.user_data or {}).get("telegram_id"),
                    "user_data": r.user_data or {},
                    "custom_data": r.custom_data or {},
                    "cookies": r.cookies or {},
                    "src_url": r.src_url,
                    "value": r.value,
                    "gclid": r.gclid,
                    "cid": r.cid,
                    "fbclid": r.fbclid,
                    "sid": r.sid,
                    "sent_pixels": r.sent_pixels or [],
                })
            return out
        except Exception as e:
            logger.error(f"Erro get_unsent_leads_for_pixel: {e}")
            return []
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)

async def mark_pixels_sent(event_key: str, pixels: List[str], event_record: Optional[Dict[str, Any]] = None) -> bool:
    """Marca pixels como enviados e registra histórico."""
    if not SessionLocal:
        return False
    loop = asyncio.get_event_loop()

    def db_sync():
        session = SessionLocal()
        try:
            buyer = session.query(Buyer).filter(Buyer.event_key == event_key).first()
            if not buyer:
                return False
            sp = set(buyer.sent_pixels or [])
            for p in pixels:
                if p:
                    sp.add(p)
            buyer.sent_pixels = list(sp)
            if event_record:
                eh = buyer.event_history or []
                eh.append(event_record)
                buyer.event_history = eh
                if event_record.get("status") == "success":
                    buyer.sent = True
                    buyer.last_sent_at = datetime.now(timezone.utc)
                else:
                    buyer.attempts = (buyer.attempts or 0) + 1
                    buyer.last_attempt_at = datetime.now(timezone.utc)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Erro mark_pixels_sent: {e}")
            return False
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)

# ==============================
# Histórico
# ==============================
async def get_historical_leads(limit: int = 50, order_by_priority: bool = True):
    if not SessionLocal:
        return []
    loop = asyncio.get_event_loop()

    def db_sync():
        session = SessionLocal()
        try:
            query = session.query(Buyer)
            if order_by_priority:
                query = query.order_by(
                    func.coalesce((Buyer.custom_data['priority_score']).astext.cast(Float), 0.0).desc(),
                    Buyer.created_at.desc()
                )
            else:
                query = query.order_by(Buyer.created_at.desc())

            rows = query.limit(limit).all()
            leads = []
            for r in rows:
                ud, cd = r.user_data or {}, r.custom_data or {}
                dec_cookies = _safe_dict(r.cookies or {}, decrypt=True) if r.cookies else {}
                leads.append({
                    "telegram_id": ud.get("telegram_id"),
                    "username": ud.get("username"),
                    "first_name": ud.get("first_name"),
                    "last_name": ud.get("last_name"),
                    "premium": ud.get("premium"),
                    "lang": ud.get("lang"),
                    "cookies": dec_cookies,
                    "event_key": r.event_key,
                    "sid": r.sid,
                    "src_url": r.src_url,
                    "cid": r.cid,
                    "gclid": r.gclid,
                    "fbclid": r.fbclid,
                    "value": r.value,
                    "sent_pixels": r.sent_pixels or [],
                    "event_history": r.event_history or [],
                    "priority_score": cd.get("priority_score") or 0.0
                })
            return leads
        except Exception as e:
            logger.error(f"Erro get_historical_leads: {e}")
            return []
        finally:
            session.close()

    return await loop.run_in_executor(None, db_sync)

# ==============================
# Retroalimentação FB + GA4 de pendentes
# ==============================
async def sync_pending_leads(batch_size: int = 20) -> int:
    """
    - Busca buyers com sent == False
    - Reconstroi lead_data com cookies/utm/ids
    - Dispara send_event_to_all (fb_google) respeitando regras de negócios
      (Fb1: Lead/Subscribe; Fb2: Lead/Subscribe/Purchase value=12 BRL)
    - Registra histórico via save_lead
    """
    if not SessionLocal:
        return 0

    loop = asyncio.get_event_loop()

    def fetch_pending():
        session = SessionLocal()
        try:
            return (
                session.query(Buyer)
                .filter(Buyer.sent == False)
                .order_by(Buyer.created_at.asc())
                .limit(batch_size)
                .all()
            )
        except Exception as e:
            logger.error(f"Erro query pending leads: {e}")
            return []
        finally:
            session.close()

    buyers = await loop.run_in_executor(None, fetch_pending)
    if not buyers:
        return 0

    # import lazy para evitar ciclos
    from fb_google import send_event_to_all

    processed = 0
    for b in buyers:
        try:
            ud = b.user_data or {}
            cd = b.custom_data or {}

            # Inferência do tipo de evento
            if bool(ud.get("subscribed") or cd.get("subscribed")):
                event_type = "Subscribe"
            elif (cd.get("purchase_count") or 0) > 0:
                event_type = "Purchase"
            else:
                event_type = "Lead"

            # Reconstroi lead_data (o fb_google faz o build do payload)
            lead_data = {
                "telegram_id": ud.get("telegram_id"),
                "user_agent": ud.get("user_agent"),
                "ip_address": ud.get("ip_address"),
                "utm_params": {
                    "utm_source": ud.get("utm_source"),
                    "utm_medium": ud.get("utm_medium"),
                    "utm_campaign": ud.get("utm_campaign"),
                },
                "referrer": ud.get("referrer"),
                "cookies": _safe_dict(b.cookies, decrypt=True),
                "src_url": b.src_url,
                "value": b.value or (12.0 if event_type == "Purchase" else 0),
                "currency": "BRL",
                "gclid": ud.get("gclid") or b.gclid,
                "wbraid": ud.get("wbraid"),
                "gbraid": ud.get("gbraid"),
            }

            # Dispara FB + GA4
            results = await send_event_to_all(lead_data, event_type=event_type)

            # Atualiza via save_lead (histórico + flags)
            await save_lead(
                {
                    "event_key": b.event_key,
                    "telegram_id": ud.get("telegram_id"),
                    "user_data": ud,
                    "custom_data": cd,
                    "cookies": _safe_dict(b.cookies, decrypt=False),
                    "src_url": b.src_url,
                    "value": lead_data["value"],
                    "sent_pixels": list(results.keys()),
                },
                event_record={
                    "event": event_type,
                    "status": "success" if any(v.get("ok") for v in results.values()) else "failed",
                    "response": results,
                    "ts": int(time.time()),
                },
            )

            processed += 1

        except Exception as e:
            logger.error(f"[SYNC_PENDING_ERROR] ek={b.event_key} err={e}")

    return processed