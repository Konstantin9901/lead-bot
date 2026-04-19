from flask import Flask, request
import logging
from db import activate_subscription, add_payment_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/webhook/paddle', methods=['POST'])
def paddle_webhook():
    data = request.json
    logger.info(f"Получен webhook от Paddle: {data}")
    
    alert_name = data.get('alert_name')
    
    if alert_name == 'payment_succeeded':
        user_id = int(data.get('passthrough'))  # Передаём user_id в passthrough
        transaction_id = data.get('order_id')
        amount = float(data.get('sale_gross', 0))
        
        # Активируем подписку на 30 дней
        activate_subscription(user_id, days=30)
        add_payment_record(user_id, transaction_id, amount, 'success')
        
        logger.info(f"Подписка активирована для user_id={user_id}")
    
    return 'OK', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)