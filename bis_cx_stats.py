import requests
import logging
import os
import uuid
from datetime import datetime
from functools import lru_cache

logger = logging.getLogger(__name__)

BIS_CX_BASE_URL = "https://bis.cx"
BIS_CX_EMAIL = os.getenv("BIS_CX_EMAIL")
BIS_CX_PASSWORD = os.getenv("BIS_CX_PASSWORD")
BIS_CX_CAMPAIGN_ID = int(os.getenv("BIS_CX_CAMPAIGN_ID", 7))

# Кэш для токена (живёт 1 час, как и сам токен)
_last_token = None
_last_token_time = None


def get_bis_cx_token():
    """Получение JWT-токена для авторизации (кэшируется на 1 час)"""
    global _last_token, _last_token_time
    
    # Проверяем, не истёк ли токен (55 минут)
    if _last_token and _last_token_time:
        if (datetime.now() - _last_token_time).seconds < 3300:
            return _last_token
    
    try:
        response = requests.post(
            f"{BIS_CX_BASE_URL}/api/auth/login",
            json={"email": BIS_CX_EMAIL, "password": BIS_CX_PASSWORD},
            timeout=10
        )
        if response.status_code == 200:
            _last_token = response.json()["access_token"]
            _last_token_time = datetime.now()
            logger.info("JWT-токен успешно получен")
            return _last_token
        else:
            logger.error(f"Ошибка получения токена: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Исключение при получении токена: {e}")
        return None


def send_lead(campaign_id: int, user_id: int, external_id: str = None):
    """
    Отправка лида (конверсии) на платформу BIS CX
    """
    token = get_bis_cx_token()
    if not token:
        logger.error("Не удалось получить токен, лид не отправлен")
        return None
    
    if external_id is None:
        external_id = str(uuid.uuid4())
    
    payload = {
        "user_id": user_id,
        "action_time": datetime.utcnow().isoformat(),
        "is_valid": True,
        "action_type": "lead",
        "external_id": external_id,
        "reward": None
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    try:
        response = requests.post(
            f"{BIS_CX_BASE_URL}/api/stats/campaigns/{campaign_id}/stats",
            json=payload,
            headers=headers,
            timeout=10
        )
        if response.status_code in (200, 201):
            logger.info(f"Лид отправлен: user_id={user_id}, campaign_id={campaign_id}")
            return response.json()
        else:
            logger.error(f"Ошибка отправки лида: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Исключение при отправке лида: {e}")
        return None


def send_lead_batch(campaign_id: int, leads: list):
    """
    Отправка нескольких лидов (конверсий) одной пачкой
    leads = [{"user_id": 123, "external_id": "..."}, ...]
    """
    token = get_bis_cx_token()
    if not token:
        logger.error("Не удалось получить токен, лиды не отправлены")
        return None
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    results = []
    for lead in leads:
        payload = {
            "user_id": lead["user_id"],
            "action_time": datetime.utcnow().isoformat(),
            "is_valid": True,
            "action_type": "lead",
            "external_id": lead.get("external_id", str(uuid.uuid4())),
            "reward": None
        }
        try:
            response = requests.post(
                f"{BIS_CX_BASE_URL}/api/stats/campaigns/{campaign_id}/stats",
                json=payload,
                headers=headers,
                timeout=10
            )
            if response.status_code in (200, 201):
                results.append({"user_id": lead["user_id"], "status": "ok"})
                logger.info(f"Лид отправлен: user_id={lead['user_id']}")
            else:
                results.append({"user_id": lead["user_id"], "status": "error", "error": response.text})
        except Exception as e:
            results.append({"user_id": lead["user_id"], "status": "error", "error": str(e)})
    
    return results