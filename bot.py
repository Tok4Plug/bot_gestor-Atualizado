import os, json
from aiogram import Bot, Dispatcher, executor, types
from redis import Redis
from utils import now_ts

# =========================
# Configurações
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # token do Bot B
VIP_CHANNEL = os.getenv("VIP_CHANNEL")        # pode ser link, @username ou chat_id
REDIS_URL = os.getenv("REDIS_URL")
STREAM = os.getenv("REDIS_STREAM", "buyers_stream")

bot = Bot(token=TELEGRAM_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)
redis = Redis.from_url(REDIS_URL, decode_responses=True)

# =========================
# Normalizador do link VIP
# =========================
def get_vip_link() -> str:
    if not VIP_CHANNEL:
        return "https://t.me"
    vip = str(VIP_CHANNEL).strip()
    if vip.startswith("http"):
        return vip
    if vip.startswith("@"):
        return f"https://t.me/{vip.replace('@','')}"
    if vip.startswith("-100"):  # chat_id de supergrupo/canal
        return f"https://t.me/c/{vip[4:]}"
    return f"https://t.me/{vip}"

VIP_LINK = get_vip_link()

# =========================
# Helper: montar lead enriquecido
# =========================
def enrich_lead(user: types.User, route_key: str, extra: dict = None) -> dict:
    lead = {
        "telegram_id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "language": user.language_code,
        "route_key": route_key,
        "event_time": now_ts()
    }

    user_data = {
        "first_name": user.first_name,
        "last_name": user.last_name,
        "external_id": str(user.id),
        "telegram_id": str(user.id),
    }

    if extra:
        lead.update(extra)
        user_data.update(extra)

    lead["user_data"] = {k: v for k, v in user_data.items() if v}
    return lead

def push_to_stream(lead: dict):
    payload = json.dumps(lead)
    redis.xadd(STREAM, {"payload": payload})

# =========================
# Handlers do Bot
# =========================
@dp.message_handler(commands=["start"])
async def start_cmd(msg: types.Message):
    lead = enrich_lead(msg.from_user, route_key="botb")
    push_to_stream(lead)

    text = (
        "👋 Bem-vindo!\n\n"
        "Você está prestes a liberar seu acesso 𝗥𝗘𝗗 𝗦𝗘𝗖𝗥𝗘𝗧 ❤️‍🔥.\n\n"
        "Clique no botão abaixo para continuar."
    )

    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🔑 Entrar no VIP", url=VIP_LINK))

    await msg.answer(text, reply_markup=kb)

@dp.message_handler(commands=["vip"])
async def vip_cmd(msg: types.Message):
    lead = enrich_lead(msg.from_user, route_key="vip")
    push_to_stream(lead)

    text = (
        "🎉 Seu acesso VIP foi liberado!\n\n"
        "Clique no botão abaixo para entrar agora 👇"
    )

    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🚪 Entrar no Canal VIP", url=VIP_LINK))

    await msg.answer(text, reply_markup=kb)

@dp.message_handler()
async def fallback(msg: types.Message):
    await msg.answer("⚠️ Não entendi...\nUse /start para iniciar ou /vip para acessar o canal exclusivo.")

# =========================
# Main
# =========================
if __name__ == "__main__":
    print("🤖 Bot B rodando...")
    executor.start_polling(dp, skip_updates=True)