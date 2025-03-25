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
        logging.error("❌ Redis подключен, но данные не сохраняются!")
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

# Получение данных из Google Sheets
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

# Функция для отправки сообщений в Telegram
async def send_telegram_message(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
        logging.info(f"✅ Сообщение отправлено: {message}")
    except error.TelegramError as e:
        logging.error(f"❌ Ошибка при отправке сообщения: {e}")

# Функция преобразования месяцев в формат "X лет Y мес."
def format_tenure(months):
    years = months // 12
    remaining_months = months % 12
    if years > 0 and remaining_months > 0:
        return f"{years} г. {remaining_months} мес."
    elif years > 0:
        return f"{years} г."
    else:
        return f"{remaining_months} мес."

# Проверка дней рождения и годовщин (ежедневно)
async def check_birthdays_and_anniversaries():
    tz = pytz.timezone("Europe/Moscow")
    today = datetime.datetime.now(tz).date()
    
    # Загружаем данные с двух листов
    data_uchet = await get_sheet_data(SHEET_UCHET_GID)
    data_aup = await get_sheet_data(SHEET_AUP_GID)
    data = data_uchet + data_aup

    birthdays_today = []
    anniversaries_today = []

    for record in data:
        name = record.get("Сотрудник", "Неизвестно")
        birth_date_raw = record.get("Дата рождения", "").strip()
        hire_date_raw = record.get("Дата приема", "").strip()

        try:
            birth_date = datetime.datetime.strptime(birth_date_raw, "%d.%m.%Y").date() if birth_date_raw else None
            hire_date = datetime.datetime.strptime(hire_date_raw, "%d.%m.%Y").date() if hire_date_raw else None
        except ValueError:
            continue

        if birth_date and birth_date.day == today.day and birth_date.month == today.month:
            age = today.year - birth_date.year
            birthdays_today.append(f"{name}, {age} лет")

        if hire_date:
            months_diff = (today.year - hire_date.year) * 12 + today.month - hire_date.month
            if hire_date.day == today.day and (months_diff == 1 or months_diff % 3 == 0):
                formatted_tenure = format_tenure(months_diff)
                anniversaries_today.append(f"{name}, {formatted_tenure} стажа")

    if birthdays_today:
        await send_telegram_message(f"🎂 **Сегодня день рождения:** 🎂\n" + "\n".join(birthdays_today))
    if anniversaries_today:
        await send_telegram_message(f"🎉 **Годовщины стажа:** 🎉\n" + "\n".join(anniversaries_today))

# Проверка дней рождения на следующий месяц (25 числа)
async def check_birthdays_next_month():
    tz = pytz.timezone("Europe/Moscow")
    today = datetime.datetime.now(tz).date()

    if today.day != 25:
        return

    next_month = today.month % 12 + 1
    data = await get_sheet_data(SHEET_AUP_GID)
    birthdays_next_month = []

    for record in data:
        name = record.get("Сотрудник", "Неизвестно")
        birth_date_raw = record.get("Дата рождения", "").strip()

        try:
            birth_date = datetime.datetime.strptime(birth_date_raw, "%d.%m.%Y").date() if birth_date_raw else None
        except ValueError:
            continue

        if birth_date and birth_date.month == next_month:
            age = today.year - birth_date.year
            birthdays_next_month.append(f"{name}, {birth_date.day}.{next_month}, {age} лет")

    if birthdays_next_month:
        await send_telegram_message(f"🎂 **Дни рождения в следующем месяце:** 🎂\n" + "\n".join(birthdays_next_month))

# Ожидание до 9:00 по МСК
async def wait_until(target_hour, target_minute, timezone="Europe/Moscow"):
    tz = pytz.timezone(timezone)
    while True:
        now = datetime.datetime.now(tz)
        target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)

        if now >= target_time:
            target_time += datetime.timedelta(days=1)

        wait_time = (target_time - now).total_seconds()
        logging.info(f"⏳ Ожидание {wait_time / 60:.2f} минут до {target_hour}:{target_minute} по МСК")
        await asyncio.sleep(wait_time)

        await check_birthdays_and_anniversaries()
        await check_birthdays_next_month()

# Основной цикл
async def main():
    await wait_until(9, 0)

if __name__ == "__main__":
    asyncio.run(main())
