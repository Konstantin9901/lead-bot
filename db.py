import sqlite3
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DB_PATH = "subscriptions.db"

def init_db():
    """Создаёт таблицы, если их нет"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            subscription_active BOOLEAN DEFAULT 0,
            subscription_expires_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            paddle_transaction_id TEXT,
            amount DECIMAL(10,2),
            status TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована")

def is_subscription_active(user_id: int) -> bool:
    """Проверяет, активна ли подписка у пользователя"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT subscription_active, subscription_expires_at 
        FROM users WHERE user_id = ?
    """, (user_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return False
    
    active, expires_at = row
    if not active:
        return False
    
    if expires_at:
        expires_date = datetime.fromisoformat(expires_at)
        if datetime.now() > expires_date:
            return False
    
    return True

def activate_subscription(user_id: int, username: str = None, days: int = 30):
    """Активирует подписку на указанное количество дней"""
    expires_at = datetime.now() + timedelta(days=days)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO users (user_id, username, subscription_active, subscription_expires_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            subscription_active = 1,
            subscription_expires_at = excluded.subscription_expires_at,
            username = COALESCE(excluded.username, username)
    """, (user_id, username, expires_at.isoformat()))
    
    conn.commit()
    conn.close()
    logger.info(f"Подписка активирована для user_id={user_id} до {expires_at}")

def add_payment_record(user_id: int, transaction_id: str, amount: float, status: str):
    """Добавляет запись о платеже"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO payments (user_id, paddle_transaction_id, amount, status)
        VALUES (?, ?, ?, ?)
    """, (user_id, transaction_id, amount, status))
    
    conn.commit()
    conn.close()
    logger.info(f"Запись платежа добавлена: user_id={user_id}, amount={amount}")