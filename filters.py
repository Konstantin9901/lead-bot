BUY_KEYWORDS = [
    "куплю", "куплю квартиру", "куплю квартиру в Батуми", "покупка", "приобрести", "приобрету квартиру",
    "ищу студию", "ищу квартиру", "куплю студию", "покупку квартиры", "ищу для клиента 1+1",
    "ищу для клиента 2+1", "ищу для клиента 3+1", "интересует покупка"
    ]
RENT_KEYWORDS = ["аренда", "сниму", "сдаю", "сдаётся", "сдам"]
JOBS_KEYWORDS = ["работа", "вакансия", "ищу работу", "трудоустройство", "резюме"]
SPAM_KEYWORDS = [
    "tether", "тезер", "usdt", "u.s.d.t.", "юсдт", "ЮСДТ", "Ю.С.Д.Т.", "барахолках", "легализация", "крипта", "тез", "криптовалюта",
    "личная встреча", "лучшие условия", "конфиденциально",
    "обмен", "обменяю", "@th0mas_store", "продам tether", "куплю крипту", "куплю tether"
]

def is_buy_lead(text):
    text = text.lower()

    matched_buy = [word for word in BUY_KEYWORDS if word in text]
    matched_rent_job = [word for word in RENT_KEYWORDS + JOBS_KEYWORDS if word in text]
    matched_spam = [word for word in SPAM_KEYWORDS if word in text]

    if matched_buy and not (matched_rent_job or matched_spam):
        print(f"✅ Прошло фильтр. Найдены ключевые слова: {matched_buy}")
        return True
    else:
        print("❌ Не прошло фильтр.")
        if not matched_buy:
            print("⛔ Нет слов покупки.")
        if matched_rent_job:
            print(f"🚫 Найдены слова аренды/работы: {matched_rent_job}")
        if matched_spam:
            print(f"🧨 Найден спам: {matched_spam}")
        return False


