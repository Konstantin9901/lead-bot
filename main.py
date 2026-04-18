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
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from filters import is_buy_lead
from bis_cx_stats import send_lead_batch, BIS_CX_CAMPAIGN_ID

# --- НАСТРОЙКА ЛОГИРОВАНИЯ ---
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- ПЕРЕМЕННЫЕ ---
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))

# --- КЛИЕНТЫ ---
client = TelegramClient("keyword_session", API_ID, API_HASH).start(bot_token=BOT_TOKEN)
bot = Bot(token=BOT_TOKEN)

# --- ОЧЕРЕДИ И ХРАНИЛИЩА ---
message_queue = asyncio.Queue()
filter_enabled = True
seen_hashes = deque(maxlen=10000)
startup_time = datetime.now(timezone.utc)
startup_counter = 0
leads_buffer = []
LEADS_BATCH_SIZE = 5

def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

# ==================== ЧТЕНИЕ КАНАЛОВ (TELEGRAM) ====================
@client.on(events.NewMessage)
async def handle_channel_message(event):
    global filter_enabled, startup_counter
    
    # Игнорируем чат с владельцем и сообщения от себя
    if event.chat_id == OWNER_ID or event.out:
        return
    
    text = event.message.message.strip() if event.message.message else ""
    if not text:
        return
    
    # Игнорируем служебные эмодзи
    if any(emoji in text for emoji in ["💬", "🔁", "🕒"]):
        return
    
    # Проверка дубликатов
    msg_hash = hash_text(text)
    if msg_hash in seen_hashes:
        return
    seen_hashes.append(msg_hash)
    
    # Защита при старте
    if datetime.now(timezone.utc) - startup_time < timedelta(seconds=10):
        startup_counter += 1
        if startup_counter > 20:
            return
    
    # Логируем все сообщения из каналов
    if event.is_channel:
        logger.info(f"📩 Канал: {text[:80]}...")
    
    # Проверка на лида
    if filter_enabled and is_buy_lead(text):
        await message_queue.put((text, event.sender_id or event.chat_id, event.chat_id, event.message.id, event.message.date))
        logger.info(f"✅ ЛИД: {text[:80]}...")

# ==================== ОТПРАВКА В TELEGRAM И BIS CX ====================
async def process_queue():
    global leads_buffer
    while True:
        try:
            text, user_id, chat_id, msg_id, date = await message_queue.get()
            
            # Задержка
            await asyncio.sleep(random.uniform(1.0, 2.5))
            
            # Отправка в Telegram владельцу
            await bot.send_message(OWNER_ID, f"🔔 **Лид!**\n\n{text}", parse_mode="Markdown")
            logger.info("✉️ Отправлено в Telegram")
            
            # Отправка в BIS CX
            leads_buffer.append({
                "user_id": user_id,
                "external_id": f"{chat_id}_{msg_id}_{int(date.timestamp())}"
            })
            logger.info(f"📊 Буфер BIS: {len(leads_buffer)}/{LEADS_BATCH_SIZE}")
            
            # Отправляем пачкой если накопилось
            if len(leads_buffer) >= LEADS_BATCH_SIZE:
                logger.info(f"📦 Отправка {len(leads_buffer)} лидов в BIS CX...")
                results = send_lead_batch(BIS_CX_CAMPAIGN_ID, leads_buffer)
                ok = sum(1 for r in results if r and r.get("status") == "ok")
                logger.info(f"✅ BIS CX: {ok}/{len(leads_buffer)}")
                leads_buffer = []
                
        except FloodWaitError as e:
            logger.warning(f"⏳ FloodWait: {e.seconds} сек")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            await asyncio.sleep(5)

# ==================== ПЕРИОДИЧЕСКАЯ ОТПРАВКА В BIS CX ====================
async def periodic_flush():
    global leads_buffer
    while True:
        await asyncio.sleep(300)  # 5 минут
        if leads_buffer:
            logger.info(f"🕒 Периодическая отправка {len(leads_buffer)} лидов в BIS CX")
            results = send_lead_batch(BIS_CX_CAMPAIGN_ID, leads_buffer)
            ok = sum(1 for r in results if r and r.get("status") == "ok")
            logger.info(f"✅ BIS CX: {ok}/{len(leads_buffer)}")
            leads_buffer = []

# ==================== КОМАНДЫ БОТА В TELEGRAM ====================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["🟢 Включить фильтр", "🔴 Выключить фильтр"], ["📊 Статистика"]]
    await update.message.reply_text(
        "🤖 *Бот для сбора лидов по недвижимости*\n\n"
        "Я отслеживаю каналы и отправляю сообщения с ключевыми словами покупки.\n\n"
        "📌 *Управление:*\n"
        "🟢 Включить фильтр\n"
        "🔴 Выключить фильтр\n"
        "📊 Статистика",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global filter_enabled
    text = update.message.text
    
    if "Включить фильтр" in text:
        filter_enabled = True
        await update.message.reply_text("✅ Фильтр **включён**", parse_mode="Markdown")
        logger.info("🔛 Фильтр ВКЛ")
    elif "Выключить фильтр" in text:
        filter_enabled = False
        await update.message.reply_text("⛔️ Фильтр **выключен**", parse_mode="Markdown")
        logger.info("🔕 Фильтр ВЫКЛ")
    elif "Статистика" in text:
        await update.message.reply_text(
            f"📊 *Статистика*\n\n"
            f"🔹 Фильтр: {'✅ ВКЛ' if filter_enabled else '❌ ВЫКЛ'}\n"
            f"🔹 Очередь: {message_queue.qsize()}\n"
            f"🔹 Буфер BIS: {len(leads_buffer)}/{LEADS_BATCH_SIZE}\n"
            f"🔹 Кэш: {len(seen_hashes)}",
            parse_mode="Markdown"
        )

# ==================== ЗАПУСК TELEGRAM БОТА ====================
async def run_telegram_bot():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    while True:
        await asyncio.sleep(1)

# ==================== ОСНОВНОЙ ЗАПУСК ====================
async def main():
    await client.start()
    logger.info("🚀 БОТ ЗАПУЩЕН")
    logger.info(f"📱 @{(await client.get_me()).username}")
    logger.info(f"👤 Владелец: {OWNER_ID}")
    logger.info("📡 Ожидание сообщений...")
    
    await asyncio.gather(
        client.run_until_disconnected(),
        process_queue(),
        periodic_flush(),
        run_telegram_bot()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Остановлен")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")











