import nest_asyncio
nest_asyncio.apply()
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

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
OWNER_ID = int(os.getenv("OWNER_ID"))

client = TelegramClient("user_session", API_ID, API_HASH).start()

message_queue = asyncio.Queue()
filter_enabled = True
seen_hashes = deque(maxlen=10000)
startup_time = datetime.now(timezone.utc)
startup_counter = 0

PROCESS_DELAY = 1.5
RANDOM_DELAY = 1.0

def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

@client.on(events.NewMessage)
async def handle_all_messages(event):
    global filter_enabled, startup_counter
    
    logger.info(f"📨 Сообщение от {event.chat_id}: {event.message.text}")
    
    if event.chat_id == OWNER_ID:
        text = event.message.text
        if text == "/start":
            await event.reply("🤖 Бот работает! /on - включить, /off - выключить, /stats - статистика")
        elif text == "/on":
            filter_enabled = True
            await event.reply("✅ Фильтр включён")
        elif text == "/off":
            filter_enabled = False
            await event.reply("⛔️ Фильтр выключен")
        elif text == "/stats":
            await event.reply(f"Фильтр: {'ВКЛ' if filter_enabled else 'ВЫКЛ'}\nОчередь: {message_queue.qsize()}")
        return
    
    if event.out or not event.is_channel:
        return
    
    text = event.message.text
    if not text or any(emoji in text for emoji in ["💬", "🔁", "🕒"]):
        return
    
    msg_hash = hash_text(text)
    if msg_hash in seen_hashes:
        return
    seen_hashes.append(msg_hash)
    
    if datetime.now(timezone.utc) - startup_time < timedelta(seconds=10):
        startup_counter += 1
        if startup_counter > 20:
            return
    
    logger.info(f"📩 {text[:80]}...")
    
    if not filter_enabled:
        return
    
    if is_buy_lead(text):
        await message_queue.put((text, event.sender_id or event.chat_id))
        logger.info(f"✅ ЛИД: {text[:80]}...")

async def sender():
    while True:
        try:
            text, user_id = await message_queue.get()
            await asyncio.sleep(PROCESS_DELAY + random.uniform(0, RANDOM_DELAY))
            await client.send_message(OWNER_ID, f"🔔 {text}")
            logger.info("✉️ Лид отправлен")
        except FloodWaitError as e:
            logger.warning(f"⏳ FloodWait: {e.seconds} сек")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            await asyncio.sleep(5)

async def main():
    await client.start()
    logger.info("🚀 БОТ ЗАПУЩЕН")
    await client.send_message(OWNER_ID, "🚀 Бот запущен и готов к работе!\n\n/start - приветствие\n/on - включить фильтр\n/off - выключить\n/stats - статистика")
    await asyncio.gather(client.run_until_disconnected(), sender())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Остановлен")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")











