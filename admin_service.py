# admin_service.py (atualizado e sincronizado com a stack final)
import os, json, traceback
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Histogram, Gauge
from db import SessionLocal, Buyer, init_db
from redis import Redis
from datetime import datetime
from typing import Optional
from cryptography.fernet import Fernet
from utils import build_fb_event

# ===============================
# Configurações de ambiente
# ===============================
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')
REDIS_URL = os.getenv('REDIS_URL')
STREAM = os.getenv('REDIS_STREAM', 'buyers_stream')
RETRO_BATCH_SIZE = int(os.getenv('RETRO_BATCH_SIZE', '500'))
SECRET_KEY = os.getenv("SECRET_KEY", Fernet.generate_key().decode())
fernet = Fernet(SECRET_KEY.encode() if isinstance(SECRET_KEY, str) else SECRET_KEY)

NEW_PIXEL_IDS_RAW = os.getenv('NEW_PIXEL_IDS', '')
NEW_PIXEL_IDS = [p.strip() for p in NEW_PIXEL_IDS_RAW.split(',') if p.strip()]

# ===============================
# Métricas Prometheus
# ===============================
LEADS_REQUEUED = Counter('admin_leads_requeued_total', 'Leads reprocessados via admin')
PIXEL_RETROFEED = Counter('admin_pixel_retrofeed_total', 'Leads retroalimentados para novos pixels')
PIXELS_SENT = Counter('admin_retrofeed_pixel_sent_total', 'Eventos retroalimentados por pixel', ['pixel_id'])
PROCESS_LATENCY = Histogram('admin_retrofeed_latency_seconds', 'Tempo de retrofeed em massa')
BATCH_SIZE_GAUGE = Gauge('admin_retrofeed_batch_size', 'Tamanho do lote processado')

# ===============================
# Inicialização do app
# ===============================
app = FastAPI(title='Admin Service')

@app.on_event('startup')
def startup():
    init_db()

# ===============================
# Autenticação Bearer
# ===============================
def check_auth(request: Request):
    if ADMIN_TOKEN:
        auth = request.headers.get('Authorization', '')
        if not auth.startswith('Bearer ') or auth.split(' ', 1)[1] != ADMIN_TOKEN:
            raise HTTPException(status_code=401, detail='Unauthorized')

# ===============================
# Endpoints básicos
# ===============================
@app.get('/health')
def health():
    return {'status': 'ok'}

@app.get('/metrics')
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get('/stats')
def stats(auth=Depends(check_auth)):
    return {
        "new_pixel_ids": NEW_PIXEL_IDS,
        "retro_batch_size": RETRO_BATCH_SIZE,
        "leads_requeued_total": LEADS_REQUEUED._value.get(),
        "pixel_retrofeed_total": PIXEL_RETROFEED._value.get()
    }

# ===============================
# Criptografia utilitária
# ===============================
def encrypt_data(data: str) -> str:
    return fernet.encrypt(data.encode()).decode()

def decrypt_data(token: str) -> str:
    return fernet.decrypt(token.encode()).decode()

# ===============================
# Endpoint: reprocessar lead único
# ===============================
@app.post('/resend/{event_key}')
def resend(event_key: str, req: Request, auth=Depends(check_auth)):
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    session = SessionLocal()

    lead = session.query(Buyer).filter(Buyer.event_key == event_key).first()
    if not lead:
        raise HTTPException(status_code=404, detail='Lead não encontrado')

    # Reset para novo envio
    lead.sent = False
    lead.attempts = 0
    session.commit()
    LEADS_REQUEUED.inc()

    # Reconstroi payload no formato unificado
    fb_event = build_fb_event(
        event_name="Lead", 
        lead_data={
            "event_key": lead.event_key,
            "src_url": lead.src_url,
            "user_data": lead.user_data,
            "gclid": lead.gclid,
            "cid": lead.cid,
            "value": lead.value or 0,
        },
        platform="fb2", 
        custom_data=lead.custom_data
    )

    payload = {
        "event_key": lead.event_key,
        "sid": lead.sid,
        "payload": json.dumps(fb_event)
    }

    for pixel_id in NEW_PIXEL_IDS:
        if pixel_id not in (lead.sent_pixels or []):
            r.xadd(STREAM, {**payload, "pixel_id": pixel_id})
            PIXEL_RETROFEED.inc()
            PIXELS_SENT.labels(pixel_id=pixel_id).inc()

    return {"status": "queued", "event_key": event_key, "pixels_targeted": NEW_PIXEL_IDS}

# ===============================
# Endpoint: retroalimentação em massa
# ===============================
@app.post('/retrofeed_all')
def retrofeed_all(auth=Depends(check_auth)):
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    session = SessionLocal()
    start = datetime.utcnow()
    processed = 0
    last_id = 0

    while True:
        leads = (
            session.query(Buyer)
            .filter(Buyer.id > last_id)
            .order_by(Buyer.id)
            .limit(RETRO_BATCH_SIZE)
            .all()
        )
        if not leads:
            break

        batch_count = 0
        for lead in leads:
            fb_event = build_fb_event(
                event_name="Lead",
                lead_data={
                    "event_key": lead.event_key,
                    "src_url": lead.src_url,
                    "user_data": lead.user_data,
                    "gclid": lead.gclid,
                    "cid": lead.cid,
                    "value": lead.value or 0,
                },
                platform="fb2",
                custom_data=lead.custom_data
            )

            payload = {
                "event_key": lead.event_key,
                "sid": lead.sid,
                "payload": json.dumps(fb_event)
            }

            for pixel_id in NEW_PIXEL_IDS:
                if pixel_id not in (lead.sent_pixels or []):
                    r.xadd(STREAM, {**payload, "pixel_id": pixel_id})
                    processed += 1
                    batch_count += 1
                    PIXEL_RETROFEED.inc()
                    PIXELS_SENT.labels(pixel_id=pixel_id).inc()

            last_id = lead.id

        BATCH_SIZE_GAUGE.set(batch_count)

    elapsed = (datetime.utcnow() - start).total_seconds()
    return {
        "status": "done",
        "leads_processed": processed,
        "elapsed_seconds": elapsed,
        "batch_size": RETRO_BATCH_SIZE
    }