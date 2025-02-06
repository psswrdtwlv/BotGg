import os
import json
import logging
import asyncio
import datetime
import gspread
import base64
import pytz
import redis
from google.oauth2.service_account import Credentials
from telegram import Bot, error

# Настройки
SHEET_ID = os.getenv("SHEET_ID")
CHAT_ID = os.getenv("CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")

# Логирование
logging.basicConfig(level=logging.INFO)

# Подключение к Redis
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    # Тестовое подключение к Redis
    redis_client.set("test_key", "test_value")
    test_value = redis_client.get("test_key")
    if test_value == "test_value":
        logging.info("✅ Redis подключен и работает корректно!")
    else:
        logging.error("❌ Redis подключен, но значение не удалось сохранить/получить!")
        exit(1)
except Exception as e:
    logging.error(f"❌ Ошибка подключения к Redis: {e}")
    exit(1)

# Декодирование CREDENTIALS_JSON из Base64
try:
    credentials_base64 = os.getenv("CREDENTIALS_JSON")
    if not credentials_base64:
        raise ValueError("CREDENTIALS_JSON не задана!")

    missing_padding = len(credentials_base64) % 4
    if missing_padding:
        credentials_base64 += "=" * (4 - missing_padding)

    credentials_json = base64.b64decode(credentials_base64).decode("utf-8")
    CREDENTIALS_JSON = json.loads(credentials_json)
    logging.info("✅ CREDENTIALS_JSON успешно загружен!")
except Exception as e:
    logging.error(f"❌ Ошибка при загрузке CREDENTIALS_JSON: {e}")
    raise

# Подключение к Telegram API
try:
    bot = Bot(token=TELEGRAM_TOKEN)
    logging.info("✅ Telegram бот успешно инициализирован!")
except Exception as e:
    logging.error(f"❌ Ошибка при инициализации Telegram бота: {e}")
    raise

# Авторизация Google Sheets API
def authorize_google_sheets():
    try:
        creds = Credentials.from_service_account_info(
            CREDENTIALS_JSON,
            scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        logging.info("✅ Успешное подключение к Google Sheets!")
        return sheet
    except Exception as e:
        logging.error(f"❌ Ошибка при подключении к Google Sheets: {e}")
        raise

# Функции работы с Redis
def load_sent_data():
    sent_today = redis_client.get("sent_today")
    return json.loads(sent_today) if sent_today else {"sent_today": []}

def save_sent_data(sent_data):
    redis_client.set("sent_today", json.dumps(sent_data))

# Получение данных из Google Sheets
async def get_sheet_data():
    try:
        sheet = authorize_google_sheets()
        data = sheet.get_all_records()
        logging.info(f"✅ Загружено {len(data)} записей из Google Sheets")
        return data
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке данных из Google Sheets: {e}")
        return []

# Отправка сообщения в Telegram
async def send_telegram_message(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
        logging.info(f"✅ Сообщение отправлено: {message}")
    except error.TelegramError as e:
        logging.error(f"❌ Ошибка при отправке сообщения: {e}")

# Проверка и отправка уведомлений
async def check_and_notify(data, sent_data):
    today = datetime.date.today()
    new_notifications = []
    birthdays = []
    anniversaries = []

    for record in data:
        name = record.get("Сотрудник", "Неизвестно")
        birth_date_raw = record.get("Дата рождения", "").strip()
        hire_date_raw = record.get("Дата приема", "").strip()

        try:
            birth_date = datetime.datetime.strptime(birth_date_raw, "%d.%m.%Y").date() if birth_date_raw else None
            hire_date = datetime.datetime.strptime(hire_date_raw, "%d.%m.%Y").date() if hire_date_raw else None
        except ValueError:
            logging.warning(f"⚠ Ошибка парсинга даты у {name}: {birth_date_raw} | {hire_date_raw}")
            continue

        if hire_date and hire_date.day == today.day and hire_date.month == today.month:
            months_worked = (today.year - hire_date.year) * 12 + today.month - hire_date.month
            if months_worked > 0 and (months_worked == 1 or months_worked % 3 == 0):
                if name not in sent_data["sent_today"]:
                    years = months_worked // 12
                    months = months_worked % 12
                    anniversary_text = f"{years} лет {months} месяцев" if months else f"{years} лет"
                    anniversaries.append(f"🎊 {name}: {anniversary_text}")
                    new_notifications.append(name)

    message_parts = []
    if birthdays:
        message_parts.append("🎂 **Сегодня День Рождения** 🎂\n" + "\n".join(birthdays))
    if anniversaries:
        message_parts.append("🏆 **Годовщина работы** 🏆\n" + "\n".join(anniversaries))

    if message_parts:
        full_message = "\n\n".join(message_parts)
        await send_telegram_message(full_message)

    sent_data["sent_today"].extend(new_notifications)
    save_sent_data(sent_data)

if __name__ == "__main__":
    asyncio.run(check_and_notify(get_sheet_data(), load_sent_data()))
