# bot.py (versão avançada, robusta e assíncrona)
import os, logging, json, asyncio, time
from datetime import datetime
from aiogram import Bot, Dispatcher, types
import redis, aiohttp
from cryptography.fernet import Fernet
from prometheus_client import Counter, Histogram

# DB / Pixel
from db import save_lead, init_db, get_historical_leads, sync_pending_leads
from fb_google import send_event_to_all, enqueue_event, process_event_queue

# =============================
# Logging estruturado
# =============================
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "time": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name
        }
        if record.exc_info:
            log["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log)

logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(JSONFormatter())
logger.addHandler(ch)

# Warmup logger
warmup_logger = logging.getLogger("warmup")
warmup_logger.setLevel(logging.INFO)
ch_warm = logging.StreamHandler()
ch_warm.setFormatter(JSONFormatter())
warmup_logger.addHandler(ch_warm)

# =============================
# ENV
# =============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
VIP_CHANNEL = os.getenv("VIP_CHANNEL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TYPEBOT_URL = os.getenv("TYPEBOT_URL")
SECRET_KEY = os.getenv("SECRET_KEY", Fernet.generate_key().decode())

fernet = Fernet(SECRET_KEY.encode() if isinstance(SECRET_KEY, str) else SECRET_KEY)

if not BOT_TOKEN or not VIP_CHANNEL:
    raise RuntimeError("BOT_TOKEN e VIP_CHANNEL são obrigatórios")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# DB init
init_db()

# =============================
# Métricas Prometheus
# =============================
LEADS_SENT = Counter('bot_leads_sent_total', 'Total de leads enviados')
EVENT_RETRIES = Counter('bot_event_retries_total', 'Retries em eventos')
PIXEL_RETROFEED = Counter('bot_pixel_retrofeed_total', 'Retrofeeds executados')
PROCESS_LATENCY = Histogram('bot_process_latency_seconds', 'Latência no processamento')

# =============================
# Segurança
# =============================
def encrypt_data(data: str) -> str:
    return fernet.encrypt(data.encode()).decode() if data else ""

def decrypt_data(token: str) -> str:
    return fernet.decrypt(token.encode()).decode() if token else ""

# =============================
# Integração Typebot
# =============================
async def forward_to_typebot(lead: dict):
    if not TYPEBOT_URL: return
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(TYPEBOT_URL, json=lead, timeout=8) as r:
                if r.status in (200, 201):
                    logger.info(json.dumps({"event": "TYPEBOT_OK", "telegram_id": lead.get("telegram_id")}))
                else:
                    logger.warning(json.dumps({"event": "TYPEBOT_FAIL", "status": r.status}))
    except Exception as e:
        logger.error(json.dumps({"event": "TYPEBOT_ERROR", "error": str(e)}))

# =============================
# VIP Link
# =============================
async def generate_vip_link(event_key: str, member_limit=1, expire_hours=24):
    try:
        invite = await bot.create_chat_invite_link(
            chat_id=int(VIP_CHANNEL),
            member_limit=member_limit,
            expire_date=int(time.time()) + expire_hours * 3600,
            name=f"VIP-{event_key}"
        )
        return invite.invite_link
    except Exception as e:
        logger.error(json.dumps({"event": "VIP_LINK_ERROR", "error": str(e)}))
        return None

# =============================
# Envio com retry exponencial
# =============================
async def send_event_with_retry(event_type: str, lead: dict, retries=5, delay=2, warmup=False):
    attempt = 0
    while attempt < retries:
        try:
            await send_event_to_all(lead, et=event_type)
            LEADS_SENT.inc()
            log = {"event": event_type, "telegram_id": lead.get("telegram_id"), "status": "success"}
            (warmup_logger if warmup else logger).info(json.dumps(log))
            return True
        except Exception as e:
            attempt += 1
            EVENT_RETRIES.inc()
            log = {"event": event_type, "telegram_id": lead.get("telegram_id"), "status": "retry", "attempt": attempt, "error": str(e)}
            (warmup_logger if warmup else logger).warning(json.dumps(log))
            await asyncio.sleep(delay ** attempt)
    logger.error(json.dumps({"event": event_type, "telegram_id": lead.get("telegram_id"), "status": "failed"}))
    return False

# =============================
# Processamento de novo lead
# =============================
async def process_new_lead(msg: types.Message):
    user_id, name = msg.from_user.id, msg.from_user.full_name

    args = {}
    try:
        if msg.get_args():
            args = json.loads(msg.get_args())
    except Exception:
        pass

    cookies = {k: encrypt_data(v) for k,v in {"_fbc": args.get("_fbc"), "_fbp": args.get("_fbp")}.items() if v}

    lead = {
        "telegram_id": user_id,
        "username": msg.from_user.username or "",
        "first_name": msg.from_user.first_name or "",
        "last_name": msg.from_user.last_name or "",
        "premium": getattr(msg.from_user, "is_premium", False),
        "lang": msg.from_user.language_code or "",
        "origin": "telegram",
        "user_agent": "TelegramBot/1.0",
        "ip_address": f"192.168.{user_id % 256}.{(user_id // 256) % 256}",
        "event_key": f"tg-{user_id}-{int(time.time())}",
        "cookies": cookies,
        "utm_source": args.get("utm_source"),
        "utm_medium": args.get("utm_medium"),
        "utm_campaign": args.get("utm_campaign"),
    }

    await save_lead(lead)
    await forward_to_typebot(lead)

    for evt in ["Lead", "Subscribe", "Purchase"]:
        await send_event_with_retry(evt, lead)

    vip_link = await generate_vip_link(lead["event_key"])
    return vip_link, lead

# =============================
# Warmup batches
# =============================
async def build_warmup_batches(batch_size=50):
    leads = await get_historical_leads(limit=batch_size)
    leads = [l for l in leads if l.get("telegram_id")]
    for l in leads:
        score = bool(l.get("username"))*2 + bool(l.get("first_name")) + (l.get("premium") and 2) + (l.get("purchase_count") or 0)*3
        l["priority_score"] = score
    high = [l for l in leads if l["priority_score"] >= 3]
    low = [l for l in leads if l["priority_score"] < 3]
    return high, low

async def warmup_loop(q, high, low):
    while True:
        while not q.empty():
            lead = await q.get()
            for evt in ["Lead","Subscribe","Purchase"]:
                await send_event_with_retry(evt, lead, warmup=True)
        for l in high:
            for evt in ["Lead","Subscribe","Purchase"]:
                await send_event_with_retry(evt, l, warmup=True)
            await asyncio.sleep(0.3)
        for l in low:
            for evt in ["Lead","Subscribe","Purchase"]:
                await send_event_with_retry(evt, l, warmup=True)
            await asyncio.sleep(1.5)
        await asyncio.sleep(60)

# =============================
# Handlers
# =============================
@dp.message_handler(commands=["start"])
async def start_cmd(msg: types.Message):
    await msg.answer("👋 Validando seu acesso VIP...")
    vip_link, lead = await process_new_lead(msg)
    if vip_link:
        await msg.answer(f"✅ {lead['first_name']} seu acesso VIP:\n{vip_link}")
    else:
        await msg.answer("⚠️ Seu acesso foi registrado, mas não foi possível gerar o link VIP.")

# =============================
# Runner
# =============================
if __name__ == "__main__":
    from asyncio import Queue
    q = Queue()

    async def main():
        logger.info(json.dumps({"event": "BOT_START"}))
        high, low = await build_warmup_batches()
        asyncio.create_task(warmup_loop(q, high, low))
        asyncio.create_task(sync_pending_leads())
        asyncio.create_task(process_event_queue())
        await dp.start_polling()

    asyncio.run(main())