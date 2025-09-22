# bot.py — versão completa, robusta e integrada ao Bridge (Typebot)
import os, logging, json, asyncio, time
from datetime import datetime
from typing import Dict, Any, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.utils import exceptions as tg_exceptions
import redis
from cryptography.fernet import Fernet
from prometheus_client import Counter, Histogram

# DB / Pixels
from db import save_lead, init_db, get_historical_leads, sync_pending_leads
from fb_google import enqueue_event, process_event_queue, send_event_to_all
from utils import now_ts

# =============================
# Logging estruturado (JSON)
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
# Namespace das chaves criadas no Bridge (não mude sem alinhar com o Bridge)
BRIDGE_NS = os.getenv("BRIDGE_NS", "typebot")

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
VIP_LINK_ERRORS = Counter('bot_vip_link_errors_total', 'Falhas ao gerar link VIP')

# =============================
# Segurança (helpers)
# =============================
def encrypt_data(data: Optional[str]) -> str:
    return fernet.encrypt(data.encode()).decode() if data else ""

def decrypt_data(token: Optional[str]) -> str:
    return fernet.decrypt(token.encode()).decode() if token else ""

# =============================
# VIP Link
# =============================
async def generate_vip_link(event_key: str, member_limit=1, expire_hours=24) -> Optional[str]:
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
        VIP_LINK_ERRORS.inc()
        logger.error(json.dumps({"event": "VIP_LINK_ERROR", "error": str(e)}))
        return None
    except Exception as e:
        VIP_LINK_ERRORS.inc()
        logger.error(json.dumps({"event": "VIP_LINK_ERROR", "error": str(e)}))
        return None

# =============================
# Parser de argumentos do /start
# =============================
def parse_start_args(msg: types.Message) -> Dict[str, Any]:
    """
    Tenta extrair dados complementares do /start em duas formas:
    1) Token do Bridge: /start t_<token> → busca no Redis (chave f"{BRIDGE_NS}:{token}")
    2) JSON curto inline: /start {"utm_source":"..."} → parse direto
    Se não houver, retorna {}.
    """
    try:
        raw = msg.get_args() if hasattr(msg, "get_args") else None
        if not raw:
            return {}
        raw = raw.strip()

        # Caso 1: token curto "t_<...>"
        if raw.startswith("t_"):
            token = raw[2:]
            blob = redis_client.get(f"{BRIDGE_NS}:{token}")
            if blob:
                try:
                    data = json.loads(blob)
                    # opcional: descartar o token após uso (idempotência simples)
                    redis_client.delete(f"{BRIDGE_NS}:{token}")
                    return data
                except Exception:
                    return {}
            return {}

        # Caso 2: JSON pequeno diretamente no payload
        if raw.startswith("{") and raw.endswith("}"):
            return json.loads(raw)

    except Exception:
        pass
    return {}

# =============================
# Merge e normalização do lead
# =============================
def build_lead_from_context(user: types.User, msg: types.Message, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Constrói o dicionário do lead combinando:
    - Dados do Telegram (user, chat)
    - Payload vindo do Bridge/Typebot (args)
    - Defaults e saneamento (UTM/cookies/ids)
    """
    user_id = user.id
    now = int(time.time())

    # Cookies: prioriza o que vem do Bridge/Typebot
    fbp = args.get("_fbp") or f"fb.1.{now}.{user_id}"
    # _fbc real idealmente vem de fbclid -> gerado no bridge; fallback aqui é apenas segurança
    fbc = args.get("_fbc") or (f"fb.1.{now}.fbclid.{user_id}" if args.get("fbclid") else f"fbc-{user_id}-{now}")

    cookies = {
        "_fbp": encrypt_data(fbp),
        "_fbc": encrypt_data(fbc)
    }

    # Device info básica + complementos do Bridge
    device_info = {
        "platform": "telegram",
        "app": "aiogram",
        "device": args.get("device"),
        "os": args.get("os"),
        "url": args.get("landing_url") or args.get("event_source_url"),
    }

    session_metadata = {
        "msg_id": msg.message_id,
        "chat_id": msg.chat.id
    }

    # Base do lead
    lead: Dict[str, Any] = {
        "telegram_id": user_id,
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "premium": getattr(user, "is_premium", False),
        "lang": user.language_code or "",
        "origin": "telegram",
        "user_agent": args.get("user_agent") or "TelegramBot/1.0",
        "ip_address": args.get("ip") or f"192.168.{user_id % 256}.{(user_id // 256) % 256}",

        "event_key": f"tg-{user_id}-{now}",
        "event_time": now_ts(),

        "cookies": cookies,
        "device_info": device_info,
        "session_metadata": session_metadata,

        # UTM (args têm prioridade)
        "utm_source": args.get("utm_source") or "telegram",
        "utm_medium": args.get("utm_medium") or "botb",
        "utm_campaign": args.get("utm_campaign") or "vip_access",
        "utm_term": args.get("utm_term"),
        "utm_content": args.get("utm_content"),

        # Fonte / URL
        "src_url": args.get("event_source_url") or args.get("landing_url"),

        # IDs Ads / GA
        "gclid": args.get("gclid"),
        "gbraid": args.get("gbraid"),
        "wbraid": args.get("wbraid"),
        "cid": args.get("cid"),          # GA client_id (fallback vindo do bridge)
        "fbclid": args.get("fbclid"),

        # Valor/currency opcionais
        "value": args.get("value") or 0,
        "currency": args.get("currency") or "BRL",

        # user_data cru para o CAPI (utils normaliza e hasheia)
        "user_data": {
            "email": args.get("email"),
            "phone": args.get("phone"),
            "first_name": args.get("first_name") or user.first_name,
            "last_name": args.get("last_name") or user.last_name,
            "city": args.get("city"),
            "state": args.get("state"),
            "zip": args.get("zip"),
            "country": args.get("country"),
            "telegram_id": str(user_id),
            "external_id": str(user_id),
            "fbp": args.get("_fbp"),
            "fbc": args.get("_fbc"),
            "ip": args.get("ip"),
            "ua": args.get("user_agent")
        }
    }

    return lead

# =============================
# Envio com retry exponencial (direto, sem depender da fila)
# =============================
async def send_event_with_retry(event_type: str, lead: Dict[str, Any], retries: int = 5, base_delay: float = 1.5) -> bool:
    """
    Dispara o evento para todas as integrações (FB/GA4) com retry exponencial.
    Usa fb_google.send_event_to_all(lead, et=event_type).
    """
    attempt = 0
    while attempt < retries:
        try:
            res = await send_event_to_all(lead, et=event_type)
            ok_fb = bool(isinstance(res.get("facebook"), dict) and res["facebook"].get("ok"))
            ok_ga = True  # quando GA4 estiver desabilitado, não deve travar a avaliação global
            if "google" in res:
                ok_ga = bool(isinstance(res.get("google"), dict) and (res["google"].get("ok") or res["google"].get("skip")))
            if ok_fb and ok_ga:
                LEADS_SENT.inc()
                logger.info(json.dumps({"event": "EVENT_SENT", "type": event_type, "telegram_id": lead.get("telegram_id"), "status": "success"}))
                return True
            else:
                raise RuntimeError(f"send_event_to_all partial/failed: {res}")
        except Exception as e:
            attempt += 1
            EVENT_RETRIES.inc()
            logger.warning(json.dumps({
                "event": "EVENT_RETRY",
                "type": event_type,
                "attempt": attempt,
                "telegram_id": lead.get("telegram_id"),
                "error": str(e)
            }))
            await asyncio.sleep((base_delay ** attempt) + 0.1 * attempt)

    logger.error(json.dumps({"event": "EVENT_FAILED", "type": event_type, "telegram_id": lead.get("telegram_id")}))
    return False

# =============================
# Processamento de novo lead (alta escala, integrado ao Bridge)
# =============================
async def process_new_lead(msg: types.Message):
    user = msg.from_user

    # 1) Recupera/parseia args (Bridge: token t_<...> ou JSON pequeno)
    args = parse_start_args(msg)

    # 2) Constrói lead com merge completo
    lead = build_lead_from_context(user, msg, args)

    # 3) Persistência rápida (idempotência pelo event_key único)
    await save_lead(lead)

    # 4) Gera link VIP (se o bot estiver com permissão no canal)
    vip_link = await generate_vip_link(lead["event_key"])

    # 5) Enfileira em uma fila “lógica” (apenas logging/auditoria),
    #    mas já dispara imediatamente com retry (para não depender de worker externo)
    try:
        await enqueue_event({"event": "Lead", "lead": {"event_key": lead["event_key"], "telegram_id": lead["telegram_id"]}})
        await enqueue_event({"event": "Subscribe", "lead": {"event_key": lead["event_key"], "telegram_id": lead["telegram_id"]}})
    except Exception as e:
        # enqueue_event é opcional; falha aqui não deve interromper o fluxo
        logger.warning(json.dumps({"event": "QUEUE_ENQ_FAIL", "error": str(e)}))

    # Disparo direto (assíncrono, não bloqueia resposta ao usuário)
    asyncio.create_task(send_event_with_retry("Lead", lead))
    asyncio.create_task(send_event_with_retry("Subscribe", lead))

    return vip_link, lead

# =============================
# Handlers
# =============================
@dp.message_handler(commands=["start"])
async def start_cmd(msg: types.Message):
    await msg.answer("👋 Validando seu acesso VIP...")
    try:
        vip_link, lead = await process_new_lead(msg)
        if vip_link:
            await msg.answer(f"✅ {lead['first_name']} seu acesso VIP:\n{vip_link}")
        else:
            await msg.answer(
                "⚠️ Seu acesso foi registrado, mas não foi possível gerar o link VIP agora.\n"
                "Tente novamente em instantes."
            )
    except Exception as e:
        logger.error(json.dumps({"event": "START_HANDLER_ERROR", "error": str(e)}))
        await msg.answer("⚠️ Ocorreu um erro ao validar seu acesso. Tente novamente em alguns instantes.")

@dp.message_handler()
async def fallback(msg: types.Message):
    """
    Resposta padrão para mensagens fora do fluxo.
    """
    await msg.answer("Use /start para iniciar o fluxo de acesso VIP.")

# =============================
# Loops de background (alta escala)
# =============================
async def _sync_pending_loop():
    """
    Retroalimenta pixels para leads com sent=False em intervalos fixos (caso use retrofeed).
    Essa rotina usa db.sync_pending_leads(), que por sua vez utiliza send_event_to_all internamente (segundo sua versão).
    """
    while True:
        try:
            count = await sync_pending_leads()
            if count:
                logger.info(json.dumps({"event": "SYNC_PENDING", "processed": count}))
        except Exception as e:
            logger.error(json.dumps({"event": "SYNC_PENDING_ERROR", "error": str(e)}))
        await asyncio.sleep(SYNC_INTERVAL_SEC)

async def _event_queue_loop():
    """
    Processa continuamente a fila de eventos (fb_google.process_event_queue).
    O seu fb_google atual possui placeholders; mantemos por compatibilidade.
    """
    while True:
        try:
            await process_event_queue()
        except Exception as e:
            logger.error(json.dumps({"event": "QUEUE_LOOP_ERROR", "error": str(e)}))
        await asyncio.sleep(1)

# =============================
# Runner
# =============================
if __name__ == "__main__":
    async def main():
        logger.info(json.dumps({"event": "BOT_START"}))

        # Loops de infraestrutura em background
        asyncio.create_task(_sync_pending_loop())     # retrofeed periódico
        asyncio.create_task(_event_queue_loop())      # fila (placeholder)

        # Polling do Telegram
        await dp.start_polling()

    asyncio.run(main())