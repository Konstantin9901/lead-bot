import asyncio
import os
import hashlib
import random
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from collections import deque
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from filters import is_buy_lead
from bis_cx_api import send_lead_batch, BIS_CX_CAMPAIGN_ID

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

# Клиент Telethon
client = TelegramClient("keyword_session", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# Очередь и буфер
message_queue = asyncio.Queue()
pending_messages = []  # ← ВОССТАНОВЛЕН буфер для повторных отправок
filter_enabled = True

# Хранилища для дубликатов
seen_ids = set()
seen_hashes = deque(maxlen=10000)

# Защита при старте
startup_time = datetime.now(timezone.utc)
startup_limit = 20
startup_counter = 0

# Задержки (увеличены для безопасности)
PROCESS_DELAY = 1.5  # увеличено с 1.0
RANDOM_DELAY_VARIATION = 1.0  # увеличено с 0.5
MAX_RETRIES = 3  # максимум попыток отправки

# Буфер для лидов BIS CX
leads_buffer = []
LEADS_BATCH_SIZE = 5


def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


@client.on(events.NewMessage)
async def handle_telethon_message(event):
    global filter_enabled, startup_counter
    
    sender_id = event.sender_id
    chat_id = event.chat_id
    message_id = event.message.id
    original_text = event.message.message.strip() if event.message.message else ""
    timestamp = event.message.date
    
    # Обработка команд от владельца
    if chat_id == OWNER_ID:
        await handle_owner_commands(event)
        return
    
    # Игнорируем сообщения от самого бота
    me = await client.get_me()
    if sender_id == me.id:
        logger.debug("Игнорируем сообщение от себя")
        return
    
    # Игнорируем служебные сообщения
    if any(emoji in original_text for emoji in ["💬", "🔁", "🕒"]):
        logger.debug("Игнорируем пересланный текст")
        return
    
    # Проверка дубликатов
    msg_key = (chat_id, message_id)
    msg_hash = hash_text(original_text)
    
    logger.info(f"Получено: ID={message_id}, chat={chat_id}, hash={msg_hash[:8]}")
    
    if msg_key in seen_ids or msg_hash in seen_hashes:
        logger.info(f"Повтор: {msg_key}")
        return
    
    seen_ids.add(msg_key)
    seen_hashes.append(msg_hash)
    
    # Очистка seen_ids от старых записей (защита от утечки памяти)
    if len(seen_ids) > 20000:
        seen_ids.clear()
        logger.info("Очищен кэш seen_ids")
    
    # Игнорируем старые сообщения
    if timestamp < datetime.now(timezone.utc) - timedelta(seconds=30):
        logger.info("Старое сообщение, игнорируем")
        return
    
    # Защита от флуда при старте
    if datetime.now(timezone.utc) - startup_time < timedelta(seconds=10):
        startup_counter += 1
        if startup_counter > startup_limit:
            logger.info("Превышен лимит стартовых сообщений")
            return
    
    # Проверяем фильтр
    if filter_enabled and is_buy_lead(original_text):
        await message_queue.put((original_text, sender_id, chat_id, message_id, timestamp, 0))  # 0 = попыток
        logger.info(f"Добавлено в очередь: {msg_key}")


async def send_message_with_retry(text, sender_id, chat_id, message_id, timestamp, retry_count):
    """Отправка сообщения с повторными попытками"""
    try:
        # Формируем ссылки
        message_link = None
        user_link = None
        
        if str(chat_id).startswith("-100"):
            try:
                entity = await client.get_entity(chat_id)
                if hasattr(entity, 'username') and entity.username:
                    message_link = f"https://t.me/{entity.username}/{message_id}"
            except Exception as e:
                logger.warning(f"Не удалось получить ссылку на сообщение: {e}")
        
        if sender_id:
            try:
                sender = await client.get_entity(sender_id)
                if hasattr(sender, 'username') and sender.username:
                    user_link = f"https://t.me/{sender.username}"
            except Exception as e:
                logger.warning(f"Не удалось получить ссылку на отправителя: {e}")
        
        # Формируем кнопки
        buttons = []
        if user_link:
            buttons.append([f"👤 Отправитель", user_link])
        if message_link:
            buttons.append([f"📎 Оригинал", message_link])
        
        # Отправляем
        await client.send_message(
            OWNER_ID,
            text,
            buttons=buttons if buttons else None,
            link_preview=False
        )
        logger.info(f"✅ Сообщение отправлено (попытка {retry_count + 1})")
        return True
        
    except FloodWaitError as e:
        wait_seconds = min(e.seconds, 60)  # Ждём не более минуты
        logger.warning(f"FloodWait: ждём {wait_seconds} сек.")
        await asyncio.sleep(wait_seconds)
        return False
        
    except RPCError as e:
        logger.error(f"RPC ошибка: {e}")
        if "PEER_ID_INVALID" in str(e) or "USER_ID_INVALID" in str(e):
            logger.warning(f"Невалидный ID пользователя, сообщение пропущено")
            return True  # Считаем успехом, чтобы удалить из очереди
        return False
        
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        return False


async def relay_messages():
    """Отправка сообщений из очереди с ретраями"""
    global leads_buffer
    
    while True:
        try:
            text, sender_id, chat_id, message_id, timestamp, retry_count = await message_queue.get()
            
            # Случайная задержка
            delay = PROCESS_DELAY + random.uniform(0, RANDOM_DELAY_VARIATION)
            await asyncio.sleep(delay)
            
            # Пытаемся отправить
            success = await send_message_with_retry(text, sender_id, chat_id, message_id, timestamp, retry_count)
            
            if success:
                logger.info("Сообщение успешно отправлено")
                
                # Добавляем в буфер лидов
                leads_buffer.append({
                    "user_id": sender_id,
                    "external_id": f"{chat_id}_{message_id}_{int(timestamp.timestamp())}"
                })
                logger.info(f"Лид в буфере: {len(leads_buffer)}/{LEADS_BATCH_SIZE}")
                
                # Отправляем пачку если накопилось
                if len(leads_buffer) >= LEADS_BATCH_SIZE:
                    await flush_leads_to_bis()
            else:
                # Если не удалось и попыток меньше максимума - возвращаем в очередь
                if retry_count < MAX_RETRIES:
                    logger.warning(f"Возвращаем в очередь (попытка {retry_count + 1}/{MAX_RETRIES})")
                    await message_queue.put((text, sender_id, chat_id, message_id, timestamp, retry_count + 1))
                    await asyncio.sleep(10)  # Пауза перед повторной попыткой
                else:
                    logger.error(f"Сообщение не отправлено после {MAX_RETRIES} попыток, пропускаем")
        
        except Exception as e:
            logger.error(f"Критическая ошибка в relay_messages: {e}")
            await asyncio.sleep(5)


async def flush_leads_to_bis():
    """Отправка лидов в BIS CX"""
    global leads_buffer
    
    if not leads_buffer:
        return
    
    try:
        logger.info(f"Отправка {len(leads_buffer)} лидов в BIS CX")
        results = send_lead_batch(BIS_CX_CAMPAIGN_ID, leads_buffer)
        successful = sum(1 for r in results if r.get("status") == "ok")
        logger.info(f"Отправлено: {successful}/{len(leads_buffer)}")
        
        if successful < len(leads_buffer):
            logger.warning(f"Не удалось отправить {len(leads_buffer) - successful} лидов")
        
        leads_buffer = []
        
    except Exception as e:
        logger.error(f"Ошибка отправки в BIS CX: {e}")


async def periodic_flush_leads():
    """Каждые 5 минут отправляет накопившиеся лиды"""
    while True:
        await asyncio.sleep(300)  # 5 минут
        if leads_buffer:
            logger.info(f"Периодическая отправка {len(leads_buffer)} лидов")
            await flush_leads_to_bis()


async def health_check():
    """Проверка здоровья бота"""
    last_activity = datetime.now(timezone.utc)
    
    while True:
        await asyncio.sleep(3600)  # Каждый час
        
        now = datetime.now(timezone.utc)
        logger.info(f"🏥 Health check: queue={message_queue.qsize()}, buffer={len(leads_buffer)}, cache={len(seen_hashes)}")
        
        # Проверяем, не завис ли бот
        if message_queue.qsize() > 100:
            logger.warning(f"❗ Очередь переполнена: {message_queue.qsize()} сообщений")
        
        # Отправляем тест владельцу
        try:
            await client.send_message(OWNER_ID, "🟢 Бот работает, очередь: {message_queue.qsize()}")
        except Exception as e:
            logger.error(f"Health check failed: {e}")


async def handle_owner_commands(event):
    """Обработка команд от владельца"""
    global filter_enabled
    
    text = event.message.message.strip()
    
    if text == "/start":
        await event.reply(
            "🤖 *Бот для сбора лидов*\n\n"
            f"📊 Статус: фильтр {'✅ ВКЛ' if filter_enabled else '❌ ВЫКЛ'}\n"
            f"📦 Очередь: {message_queue.qsize()} сообщений\n"
            f"💾 Кэш: {len(seen_hashes)} уникальных хешей\n\n"
            "🔄 *Управление:*\n"
            "• Включить фильтр - вкл/выкл\n"
            "• Статистика - детальная информация\n"
            "• Помощь - инструкция",
            parse_mode="markdown",
            buttons=[
                [f"🟢 Включить фильтр ({ '✅' if filter_enabled else '❌' })"],
                ["📊 Статистика", "💬 Помощь"]
            ]
        )
    
    elif "Включить фильтр" in text:
        filter_enabled = not filter_enabled
        status = "включён ✅" if filter_enabled else "выключен ❌"
        await event.reply(
            f"Фильтр {status}",
            buttons=[[f"🟢 Включить фильтр ({ '✅' if filter_enabled else '❌' })"]]
        )
        logger.info(f"Фильтр {'ON' if filter_enabled else 'OFF'}")
    
    elif text == "📊 Статистика":
        await event.reply(
            f"📈 *Детальная статистика*\n\n"
            f"🔹 Фильтр: {'включён ✅' if filter_enabled else 'выключен ❌'}\n"
            f"🔹 В очереди: {message_queue.qsize()} сообщений\n"
            f"🔹 Кэш дублей: {len(seen_hashes)}/{seen_hashes.maxlen}\n"
            f"🔹 Лидов в буфере: {len(leads_buffer)}\n"
            f"🔹 Размер пачки: {LEADS_BATCH_SIZE}\n"
            f"🔹 Макс. попыток: {MAX_RETRIES}\n"
            f"🔹 Задержка: {PROCESS_DELAY}±{RANDOM_DELAY_VARIATION} сек",
            parse_mode="markdown"
        )
    
    elif text == "💬 Помощь":
        await event.reply(
            "📚 *Защита от блокировки:*\n\n"
            "✅ FloodWait обработка\n"
            "✅ Дубликаты (10000 хешей)\n"
            "✅ Защита при старте\n"
            "✅ Рандомные задержки\n"
            "✅ Повторные попытки (3 раза)\n"
            "✅ Автоочистка кэша\n"
            "✅ Health check каждый час\n\n"
            "🛡️ Бот защищён от блокировки Telegram",
            parse_mode="markdown"
        )


async def main():
    """Запуск бота"""
    logger.info("🚀 Запуск защищённого бота на Telethon...")
    
    await client.start()
    logger.info(f"✅ Бот запущен: @{(await client.get_me()).username}")
    
    # Приветствие владельцу
    try:
        await client.send_message(
            OWNER_ID,
            "🛡️ *Защищённый бот запущен*\n\n"
            f"Фильтр: {'✅ ВКЛ' if filter_enabled else '❌ ВЫКЛ'}\n"
            f"Защита: FloodWait, дубликаты, ретраи\n"
            f"Очередь: {message_queue.qsize()}\n\n"
            "Используй /start для управления",
            parse_mode="markdown"
        )
    except Exception as e:
        logger.warning(f"Не удалось отправить приветствие: {e}")
    
    # Запускаем все задачи
    await asyncio.gather(
        client.run_until_disconnected(),
        relay_messages(),
        periodic_flush_leads(),
        health_check()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")











