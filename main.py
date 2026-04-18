import asyncio
import os
import hashlib
import random
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from collections import deque
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from filters import is_buy_lead
from bis_cx_stats import send_lead_batch, BIS_CX_CAMPAIGN_ID

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))

client = TelegramClient("bot_session", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

message_queue = asyncio.Queue()
filter_enabled = True
seen_hashes = deque(maxlen=10000)
startup_time = datetime.now(timezone.utc)
startup_counter = 0
leads_buffer = []
LEADS_BATCH_SIZE = 5

# Защитные параметры
PROCESS_DELAY = 1.5           # Минимальная задержка перед отправкой
RANDOM_DELAY = 1.0            # Случайное отклонение ±0.5 сек
MAX_RETRIES = 3               # Максимум попыток отправки

def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

# ========== КОМАНДЫ ДЛЯ ВЛАДЕЛЬЦА ==========
@client.on(events.NewMessage(chats=OWNER_ID))
async def owner_commands(event):
    global filter_enabled
    text = event.message.text
    
    if text == "/start":
        await event.reply(
            "🤖 *Бот сбора лидов*\n\n"
            "/on - включить фильтр\n"
            "/off - выключить\n"
            "/stats - статистика",
            parse_mode="markdown"
        )
    elif text == "/on":
        filter_enabled = True
        await event.reply("✅ Фильтр включён")
        logger.info("🔛 Фильтр ВКЛ")
    elif text == "/off":
        filter_enabled = False
        await event.reply("⛔️ Фильтр выключен")
        logger.info("🔕 Фильтр ВЫКЛ")
    elif text == "/stats":
        await event.reply(
            f"📊 *Статистика*\n\n"
            f"Фильтр: {'✅ ВКЛ' if filter_enabled else '❌ ВЫКЛ'}\n"
            f"Очередь: {message_queue.qsize()}\n"
            f"Буфер BIS: {len(leads_buffer)}/{LEADS_BATCH_SIZE}\n"
            f"Кэш: {len(seen_hashes)}",
            parse_mode="markdown"
        )

# ========== ЧТЕНИЕ КАНАЛОВ ==========
@client.on(events.NewMessage)
async def channel_reader(event):
    global filter_enabled, startup_counter
    
    if event.chat_id == OWNER_ID or event.out:
        return
    
    text = event.message.text
    if not text:
        return
    
    # Игнорируем служебные эмодзи
    if any(emoji in text for emoji in ["💬", "🔁", "🕒"]):
        return
    
    # Защита от дубликатов
    msg_hash = hash_text(text)
    if msg_hash in seen_hashes:
        logger.info(f"♻️ Дубликат: {text[:50]}...")
        return
    seen_hashes.append(msg_hash)
    
    # Защита при старте (первые 10 секунд игнорируем до 20 сообщений)
    if datetime.now(timezone.utc) - startup_time < timedelta(seconds=10):
        startup_counter += 1
        if startup_counter > 20:
            logger.info("⚠️ Лимит стартовых сообщений")
            return
    
    # Логируем всё из каналов
    if event.is_channel:
        logger.info(f"📩 {text[:80]}...")
    
    if not filter_enabled:
        logger.info(f"⏸️ Фильтр выключен, пропущено: {text[:50]}...")
        return
    
    if is_buy_lead(text):
        await message_queue.put((text, event.sender_id or event.chat_id, event.chat_id, event.message.id))
        logger.info(f"✅ ЛИД: {text[:80]}...")
    else:
        logger.info(f"❌ НЕ лид: {text[:50]}...")

# ========== ОТПРАВКА С ЗАДЕРЖКАМИ ==========
async def sender():
    global leads_buffer
    while True:
        try:
            text, user_id, chat_id, msg_id = await message_queue.get()
            
            # ИМИТАЦИЯ ЧЕЛОВЕКА: случайная задержка перед отправкой
            delay = PROCESS_DELAY + random.uniform(0, RANDOM_DELAY)
            logger.info(f"⏳ Пауза {delay:.1f} сек перед отправкой...")
            await asyncio.sleep(delay)
            
            # Отправка владельцу
            await client.send_message(OWNER_ID, f"🔔 {text}")
            logger.info("✉️ Отправлено в Telegram")
            
            # Буфер для BIS CX
            leads_buffer.append({
                "user_id": user_id,
                "external_id": f"{chat_id}_{msg_id}_{int(datetime.now().timestamp())}"
            })
            logger.info(f"📊 Буфер BIS: {len(leads_buffer)}/{LEADS_BATCH_SIZE}")
            
            # Отправка пачки в BIS CX
            if len(leads_buffer) >= LEADS_BATCH_SIZE:
                logger.info(f"📦 Отправка {len(leads_buffer)} лидов в BIS CX...")
                results = send_lead_batch(BIS_CX_CAMPAIGN_ID, leads_buffer)
                ok = sum(1 for r in results if r and r.get("status") == "ok")
                logger.info(f"✅ BIS CX: {ok}/{len(leads_buffer)}")
                leads_buffer = []
                
        except FloodWaitError as e:
            logger.warning(f"⏳ FloodWait: ждём {e.seconds} сек")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            await asyncio.sleep(5)

# ========== ПЕРИОДИЧЕСКАЯ ОТПРАВКА В BIS CX ==========
async def periodic():
    global leads_buffer
    while True:
        await asyncio.sleep(300)  # 5 минут
        if leads_buffer:
            logger.info(f"🕒 Периодическая отправка {len(leads_buffer)} лидов в BIS CX")
            results = send_lead_batch(BIS_CX_CAMPAIGN_ID, leads_buffer)
            ok = sum(1 for r in results if r and r.get("status") == "ok")
            logger.info(f"✅ BIS CX: {ok}/{len(leads_buffer)}")
            leads_buffer = []

# ========== ЗАПУСК ==========
async def main():
    await client.start()
    logger.info("🚀 БОТ ЗАПУЩЕН")
    logger.info(f"📱 @{(await client.get_me()).username}")
    logger.info(f"👤 Владелец: {OWNER_ID}")
    logger.info("📡 Ожидание сообщений из каналов...")
    logger.info("💬 Команды: /start, /on, /off, /stats")
    logger.info(f"🛡️ Защита: FloodWait, дубликаты, задержка {PROCESS_DELAY}±{RANDOM_DELAY} сек")
    
    await asyncio.gather(
        client.run_until_disconnected(),
        sender(),
        periodic()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Остановлен")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")











