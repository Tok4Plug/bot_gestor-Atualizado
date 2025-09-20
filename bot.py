import os, logging, json, asyncio, time
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.utils import exceptions as tg_exceptions
import redis
from cryptography.fernet import Fernet
from prometheus_client import Counter, Histogram

# DB / Pixels
from db import save_lead, init_db, get_historical_leads, sync_pending_leads
from fb_google import enqueue_event, process_event_queue, send_event_with_retry
from utils import now_ts

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

# =============================
# ENV
# =============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
VIP_CHANNEL = os.getenv("VIP_CHANNEL")  # chat_id do canal VIP (ex: -1001234567890)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SYNC_INTERVAL_SEC = int(os.getenv("SYNC_INTERVAL_SEC", "60"))  # frequência do retrofeed

SECRET_KEY = os.getenv("SECRET_KEY", Fernet.generate_key().decode())
fernet = Fernet(SECRET_KEY.encode() if isinstance(SECRET_KEY, str) else SECRET_KEY)

if not BOT_TOKEN or not VIP_CHANNEL:
    raise RuntimeError("BOT_TOKEN e VIP_CHANNEL são obrigatórios")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# DB init
init_db()

# =============================
# Métricas Prometheus
# =============================
LEADS_SENT = Counter('bot_leads_sent_total', 'Total de leads enfileirados/enviados')
EVENT_RETRIES = Counter('bot_event_retries_total', 'Retries em eventos')
PROCESS_LATENCY = Histogram('bot_process_latency_seconds', 'Latência no processamento')

# =============================
# Segurança
# =============================
def encrypt_data(data: str) -> str:
    return fernet.encrypt(data.encode()).decode() if data else ""

# =============================
# VIP Link
# =============================
async def generate_vip_link(event_key: str, member_limit=1, expire_hours=24):
    """
    Gera link de convite temporário para o canal VIP.
    Caso falhe (sem permissões, etc.), retorna None.
    """
    try:
        invite = await bot.create_chat_invite_link(
            chat_id=int(VIP_CHANNEL),
            member_limit=member_limit,
            expire_date=int(time.time()) + expire_hours * 3600,
            name=f"VIP-{event_key}"
        )
        return invite.invite_link
    except tg_exceptions.TelegramAPIError as e:
        logger.error(json.dumps({"event": "VIP_LINK_ERROR", "error": str(e)}))
        return None
    except Exception as e:
        logger.error(json.dumps({"event": "VIP_LINK_ERROR", "error": str(e)}))
        return None

# =============================
# Util: parse de argumentos (/start {json})
# =============================
def parse_start_args(msg: types.Message) -> dict:
    """
    Tenta extrair JSON dos argumentos do /start para enriquecer UTM/cookies/etc.
    Se não houver ou estiver inválido, retorna {}.
    """
    try:
        # aiogram 2.x: msg.get_args() contém texto após /start
        if hasattr(msg, "get_args"):
            raw = msg.get_args()
            if raw:
                return json.loads(raw)
    except Exception:
        pass
    return {}

# =============================
# Processamento de novo lead (alta escala)
# =============================
async def process_new_lead(msg: types.Message):
    user = msg.from_user
    user_id = user.id

    # 1) Enriquecimento por args/UTMs (se vier do Typebot, redirect, deep-link etc.)
    args = parse_start_args(msg)

    # 2) Cookies simulados (ou vindos dos args)
    fbp = args.get("_fbp") or f"fbp-{user_id}-{int(time.time())}"
    fbc = args.get("_fbc") or f"fbc-{user_id}-{int(time.time())}"
    cookies = {"_fbp": encrypt_data(fbp), "_fbc": encrypt_data(fbc)}

    # 3) Monta lead completo
    lead = {
        "telegram_id": user_id,
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "premium": getattr(user, "is_premium", False),
        "lang": user.language_code or "",
        "origin": "telegram",
        "user_agent": "TelegramBot/1.0",
        "ip_address": args.get("ip") or f"192.168.{user_id % 256}.{(user_id // 256) % 256}",
        "event_key": f"tg-{user_id}-{int(time.time())}",
        "event_time": now_ts(),

        # cookies e metadados
        "cookies": cookies,
        "device_info": {
            "platform": "telegram",
            "app": "aiogram",
            "device": args.get("device"),
            "os": args.get("os"),
            "url": args.get("landing_url") or args.get("event_source_url"),
        },
        "session_metadata": {
            "msg_id": msg.message_id,
            "chat_id": msg.chat.id
        },

        # UTM (args tem prioridade)
        "utm_source": args.get("utm_source") or "telegram",
        "utm_medium": args.get("utm_medium") or "botb",
        "utm_campaign": args.get("utm_campaign") or "vip_access",
        "utm_term": args.get("utm_term"),
        "utm_content": args.get("utm_content"),

        # Fonte da página
        "src_url": args.get("event_source_url") or args.get("landing_url"),
        "value": args.get("value") or 0,
        "currency": args.get("currency") or "BRL",
    }

    # 4) Persistência rápida (idempotência pelo event_key único)
    await save_lead(lead)

    # 5) Gera link VIP (se o bot estiver com permissão no canal)
    vip_link = await generate_vip_link(lead["event_key"])

    # 6) Enfileira eventos (ao invés de disparar direto) → mais seguro em escala
    await enqueue_event("Lead", lead)
    await enqueue_event("Subscribe", lead)
    LEADS_SENT.inc()

    return vip_link, lead

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
        await msg.answer(
            "⚠️ Seu acesso foi registrado, mas não foi possível gerar o link VIP.\n"
            "Tente novamente em instantes."
        )

# =============================
# Loops de background (alta escala)
# =============================
async def _sync_pending_loop():
    """Retroalimenta pixels para leads com sent=False em intervalos fixos."""
    while True:
        try:
            count = await sync_pending_leads()
            if count:
                logger.info(json.dumps({"event": "SYNC_PENDING", "processed": count}))
        except Exception as e:
            logger.error(json.dumps({"event": "SYNC_PENDING_ERROR", "error": str(e)}))
        await asyncio.sleep(SYNC_INTERVAL_SEC)

async def _event_queue_loop():
    """Processa continuamente a fila de eventos (fb_google.process_event_queue)."""
    try:
        await process_event_queue()
    except Exception as e:
        logger.error(json.dumps({"event": "QUEUE_LOOP_ERROR", "error": str(e)}))

# =============================
# Runner
# =============================
if __name__ == "__main__":
    async def main():
        logger.info(json.dumps({"event": "BOT_START"}))

        # Loops de infraestrutura em background
        asyncio.create_task(_sync_pending_loop())     # retrofeed periódico
        asyncio.create_task(_event_queue_loop())      # fila de eventos

        # Polling do Telegram
        await dp.start_polling()

    asyncio.run(main())