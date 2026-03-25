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
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from filters import is_buy_lead   # ваша логика фильтрации

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))

# Клиент Telethon (использует существующий файл сессии)
client = TelegramClient("keyword_session", API_ID, API_HASH)
bot = Bot(token=BOT_TOKEN)

# Очередь и буфер
message_queue = asyncio.Queue()
pending_messages = []
filter_enabled = True

# Хранилища для дубликатов
seen_ids = set()
seen_hashes = deque(maxlen=10000)

# Защита при старте
startup_time = datetime.now(timezone.utc)
startup_limit = 20
startup_counter = 0

# Задержки
PROCESS_DELAY = 1.0
RANDOM_DELAY_VARIATION = 0.5

def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

@client.on(events.NewMessage)
async def handle_telethon_message(event):
    global filter_enabled, startup_counter

    sender_id = event.message.sender_id
    chat_id = event.chat_id
    message_id = event.message.id
    original_text = event.message.message.strip()
    timestamp = event.message.date

    me = await client.get_me()
    if sender_id == me.id or chat_id == OWNER_ID:
        logger.debug("Игнорируем сообщение от себя или из канала пересылки")
        return

    if "💬" in original_text or "🔁" in original_text or "🕒" in original_text:
        logger.debug("Игнорируем пересланный текст")
        return

    msg_key = (chat_id, message_id)
    msg_hash = hash_text(original_text)

    logger.info(f"Получено: ID={message_id}, chat={chat_id}, hash={msg_hash}")

    if msg_key in seen_ids or msg_hash in seen_hashes:
        logger.info(f"Повтор: {msg_key}")
        return

    seen_ids.add(msg_key)
    seen_hashes.append(msg_hash)

    if timestamp < datetime.now(timezone.utc) - timedelta(seconds=30):
        logger.info("Старое сообщение, игнорируем")
        return

    if datetime.now(timezone.utc) - startup_time < timedelta(seconds=10):
        startup_counter += 1
        if startup_counter > startup_limit:
            logger.info("Превышен лимит стартовых сообщений")
            return

    if filter_enabled and is_buy_lead(original_text):
        await message_queue.put((original_text, sender_id, chat_id, message_id, timestamp))
        logger.info(f"Добавлено в очередь: {msg_key}")

async def relay_messages():
    while True:
        try:
            text, sender_id, chat_id, message_id, timestamp = await message_queue.get()
            delay = PROCESS_DELAY + random.uniform(0, RANDOM_DELAY_VARIATION)
            await asyncio.sleep(delay)

            username = None
            user_link = None
            if sender_id:
                try:
                    sender = await client.get_entity(sender_id)
                    username = getattr(sender, "username", None)
                except Exception as e:
                    logger.warning(f"Не удалось получить username: {e}")
                if username:
                    user_link = f"https://t.me/{username}"

            message_link = None
            if str(chat_id).startswith("-100"):
                try:
                    entity = await client.get_entity(chat_id)
                    channel_username = getattr(entity, "username", None)
                    if channel_username:
                        message_link = f"https://t.me/{channel_username}/{message_id}"
                except Exception:
                    pass

            buttons = []
            if user_link:
                buttons.append(InlineKeyboardButton("👤 Отправитель", url=user_link))
            if message_link:
                buttons.append(InlineKeyboardButton("📎 Оригинал", url=message_link))
            markup = InlineKeyboardMarkup([buttons]) if buttons else None

            await bot.send_message(
                chat_id=OWNER_ID,
                text=text,
                reply_markup=markup
            )
            logger.info("Сообщение успешно отправлено владельцу.")
        except FloodWaitError as e:
            wait_seconds = e.seconds
            logger.warning(f"Flood wait: нужно подождать {wait_seconds} сек.")
            await asyncio.sleep(wait_seconds)
        except Exception as e:
            logger.error(f"Ошибка отправки: {e}")
            pending_messages.append((text, sender_id, chat_id, message_id, timestamp))
            await asyncio.sleep(5)

async def retry_pending():
    while True:
        await asyncio.sleep(30)
        for item in pending_messages[:]:
            text, sender_id, chat_id, message_id, timestamp = item
            try:
                username = None
                user_link = None
                if sender_id:
                    try:
                        sender = await client.get_entity(sender_id)
                        username = getattr(sender, "username", None)
                    except Exception:
                        pass
                    if username:
                        user_link = f"https://t.me/{username}"

                message_link = None
                if str(chat_id).startswith("-100"):
                    try:
                        entity = await client.get_entity(chat_id)
                        channel_username = getattr(entity, "username", None)
                        if channel_username:
                            message_link = f"https://t.me/{channel_username}/{message_id}"
                    except Exception:
                        pass

                buttons = []
                if user_link:
                    buttons.append(InlineKeyboardButton("👤 Отправитель", url=user_link))
                if message_link:
                    buttons.append(InlineKeyboardButton("📎 Оригинал", url=message_link))
                markup = InlineKeyboardMarkup([buttons]) if buttons else None

                await bot.send_message(
                    chat_id=OWNER_ID,
                    text=text,
                    reply_markup=markup
                )
                pending_messages.remove(item)
                logger.info("Сообщение из буфера успешно отправлено.")
            except FloodWaitError as e:
                wait_seconds = e.seconds
                logger.warning(f"Flood wait при повторе: {wait_seconds} сек.")
                await asyncio.sleep(wait_seconds)
            except Exception as e:
                logger.error(f"Ошибка при повторе: {e}")
                if "PeerUser" in str(e):
                    pending_messages.remove(item)
                    logger.info("Удалено из буфера: невозможно получить entity")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["🟢 Включить фильтр"], ["🔴 Выключить фильтр"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        "Привет! Я бот для лидов по недвижимости.\nВыбери действие:",
        reply_markup=reply_markup
    )

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global filter_enabled
    text = update.message.text
    if "Включить фильтр" in text:
        filter_enabled = True
        await update.message.reply_text("✅ Фильтр включён.")
    elif "Выключить фильтр" in text:
        filter_enabled = False
        await update.message.reply_text("⛔️ Фильтр выключен.")

async def run_telegram_bot():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    while True:
        await asyncio.sleep(1)

async def main():
    await client.start()  # использует существующую сессию keyword_session.session
    logger.info("Telethon клиент запущен.")
    await asyncio.gather(
        client.run_until_disconnected(),
        relay_messages(),
        retry_pending(),
        run_telegram_bot()
    )

if __name__ == "__main__":
    asyncio.run(main())














