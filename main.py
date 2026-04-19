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
from db import init_db, is_subscription_active, activate_subscription, add_payment_record

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

PROCESS_DELAY = 1.5
RANDOM_DELAY = 1.0

def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

# Инициализация БД
init_db()

# ========== КОМАНДЫ ДЛЯ ВЛАДЕЛЬЦА ==========
@client.on(events.NewMessage)
async def handle_all_messages(event):
    global filter_enabled
    
    logger.info(f"📨 Сообщение от {event.chat_id}: {event.message.text}")
    
    if event.chat_id == OWNER_ID:
        text = event.message.text
        
        if text == "/start":
            await event.reply(
                "🤖 *LidBot Pro*\n\n"
                "Бот собирает лиды из Telegram-каналов по ключевым словам.\n\n"
                "📌 *Команды:*\n"
                "/on - включить фильтр\n"
                "/off - выключить фильтр\n"
                "/stats - статистика\n"
                "/subscribe - купить подписку ($50/мес)",
                parse_mode="markdown"
            )
        
        elif text == "/on":
            filter_enabled = True
            await event.reply("✅ Фильтр включён")
        
        elif text == "/off":
            filter_enabled = False
            await event.reply("⛔️ Фильтр выключен")
        
        elif text == "/stats":
            await event.reply(
                f"📊 *Статистика*\n\n"
                f"Фильтр: {'✅ ВКЛ' if filter_enabled else '❌ ВЫКЛ'}\n"
                f"Очередь: {message_queue.qsize()}\n"
                f"Подписка: {'✅ Активна' if is_subscription_active(OWNER_ID) else '❌ Не активна'}",
                parse_mode="markdown"
            )
        
        elif text == "/subscribe":
            # Временная заглушка — позже заменим на ссылку Paddle
            await event.reply(
                "💳 *Оплата подписки*\n\n"
                "Стоимость: $50/месяц\n\n"
                "Ссылка для оплаты: https://paddle.com/checkout/...\n\n"
                "После оплаты подписка активируется автоматически.",
                parse_mode="markdown"
            )
        
        return
    
    # Чтение каналов (только если подписка активна)
    if not is_subscription_active(OWNER_ID):
        logger.warning("❌ Подписка не активна, лиды не обрабатываются")
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
        await message_queue.put((text, event.sender_id or event.chat_id, event.chat_id, event.message.id))
        logger.info(f"✅ ЛИД: {text[:80]}...")

# ========== ОТПРАВКА ЛИДОВ ==========
async def sender():
    while True:
        try:
            text, user_id, chat_id, msg_id = await message_queue.get()
            
            # Проверка подписки перед отправкой
            if not is_subscription_active(OWNER_ID):
                logger.warning("❌ Подписка не активна, лид не отправлен")
                continue
            
            await asyncio.sleep(PROCESS_DELAY + random.uniform(0, RANDOM_DELAY))
            await client.send_message(OWNER_ID, f"🔔 {text}")
            logger.info("✉️ Лид отправлен в Telegram")
            
        except FloodWaitError as e:
            logger.warning(f"⏳ FloodWait: {e.seconds} сек")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            await asyncio.sleep(5)

# ========== ЗАПУСК ==========
async def main():
    await client.start()
    logger.info("🚀 LIDBOT PRO ЗАПУЩЕН")
    await client.send_message(
        OWNER_ID,
        "🚀 *LidBot Pro запущен!*\n\n"
        "📌 Команды:\n"
        "/start - приветствие\n"
        "/on - включить фильтр\n"
        "/off - выключить фильтр\n"
        "/stats - статистика\n"
        "/subscribe - купить подписку",
        parse_mode="markdown"
    )
    await asyncio.gather(client.run_until_disconnected(), sender())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Остановлен")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")











