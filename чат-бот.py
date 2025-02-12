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
SHEET_UCHET_GID = int(os.getenv("SHEET_UCHET_GID", 0))  # ГИД листа "Учёт"
SHEET_AUP_GID = int(os.getenv("SHEET_AUP_GID", 1393986014))  # ГИД листа "Учёт АУП"
CHAT_ID = os.getenv("CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")

# Логирование
logging.basicConfig(level=logging.INFO)

# Подключение к Redis
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
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
        logging.info("✅ Успешное подключение к Google Sheets!")
        return client
    except Exception as e:
        logging.error(f"❌ Ошибка при подключении к Google Sheets: {e}")
        raise

# Функции работы с Redis
def load_sent_data():
    sent_today = redis_client.get("sent_today")
    return json.loads(sent_today) if sent_today else {"sent_today": []}

def save_sent_data(sent_data):
    redis_client.set("sent_today", json.dumps(sent_data))

# Получение данных из вкладки Google Sheets
async def get_sheet_data(sheet_gid):
    try:
        client = authorize_google_sheets()
        sheet = client.open_by_key(SHEET_ID).get_worksheet_by_id(sheet_gid)
        data = sheet.get_all_records()
        logging.info(f"✅ Загружено {len(data)} записей с вкладки {sheet_gid}")
        return data
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке данных с вкладки {sheet_gid}: {e}")
        return []

# Функция для правильного склонения "год"
def format_years(years):
    if 11 <= years % 100 <= 14:
        return f"{years} лет"
    last_digit = years % 10
    if last_digit == 1:
        return f"{years} год"
    if 2 <= last_digit <= 4:
        return f"{years} года"
    return f"{years} лет"

# Проверка и отправка уведомлений о ДР в следующем месяце (25 числа)
async def check_and_notify_for_next_month():
    today = datetime.date.today()
    logging.info("🔍 Тестовая проверка загрузки вкладки 'Учёт АУП'")

    MONTH_NAMES_NOMINATIVE = {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель", 5: "май", 6: "июнь",
        7: "июль", 8: "август", 9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
    }

    MONTH_NAMES_GENITIVE = {
        1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня",
        7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"
    }

    data = await get_sheet_data(SHEET_AUP_GID)
    next_month = today.month % 12 + 1
    next_month_nominative = MONTH_NAMES_NOMINATIVE[next_month]  # "март"
    next_month_genitive = MONTH_NAMES_GENITIVE[next_month]  # "марта"

    birthdays_next_month = []

    for record in data:
        name = record.get("Сотрудник", "Неизвестно")
        birth_date_raw = record.get("Дата рождения", "").strip()
        position = record.get("Должность", "Неизвестно")

        try:
            birth_date = datetime.datetime.strptime(birth_date_raw, "%d.%m.%Y").date() if birth_date_raw else None
        except ValueError:
            continue

        if birth_date and birth_date.month == next_month:
            age = format_years(today.year - birth_date.year)
            birthdays_next_month.append(f"{name}, {birth_date.day} {next_month_genitive}, {age}, {position}")

    if birthdays_next_month:
        await send_telegram_message(f"🎂 **Дни рождения в {next_month_nominative}** 🎂\n" + "\n".join(birthdays_next_month))

async def main():
    while True:
        sent_data = load_sent_data()
        data = await get_sheet_data(SHEET_UCHET_GID)
        await check_and_notify_for_next_month()
        await asyncio.sleep(86400)

if __name__ == "__main__":
    asyncio.run(main())
