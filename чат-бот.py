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

# === Настройки ===
SHEET_ID = os.getenv("SHEET_ID")
SHEET_UCHET_GID = int(os.getenv("SHEET_UCHET_GID", 0))
SHEET_AUP_GID = int(os.getenv("SHEET_AUP_GID", 1393986014))
CHAT_ID = os.getenv("CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")

# === Логирование ===
logging.basicConfig(level=logging.INFO)

# === Redis ===
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.set("test_key", "test_value")
    if redis_client.get("test_key") == "test_value":
        logging.info("✅ Redis подключен и работает корректно!")
    else:
        logging.error("❌ Redis работает некорректно!")
        exit(1)
except Exception as e:
    logging.error(f"❌ Redis ошибка: {e}")
    exit(1)

# === Google Credentials ===
try:
    credentials_base64 = os.getenv("CREDENTIALS_JSON")
    if not credentials_base64:
        raise ValueError("CREDENTIALS_JSON не задана!")
    credentials_base64 += "=" * (-len(credentials_base64) % 4)
    credentials_json = base64.b64decode(credentials_base64).decode("utf-8")
    CREDENTIALS_JSON = json.loads(credentials_json)
    logging.info("✅ Ключи Google загружены")
except Exception as e:
    logging.error(f"❌ Ошибка загрузки CREDENTIALS_JSON: {e}")
    raise

# === Telegram Bot ===
try:
    bot = Bot(token=TELEGRAM_TOKEN)
    logging.info("✅ Telegram бот запущен")
except Exception as e:
    logging.error(f"❌ Ошибка Telegram бота: {e}")
    raise

# === Google Sheets авторизация ===
def authorize_google_sheets():
    try:
        creds = Credentials.from_service_account_info(
            CREDENTIALS_JSON,
            scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        return gspread.authorize(creds)
    except Exception as e:
        logging.error(f"❌ Ошибка Google Sheets: {e}")
        raise

# === Загрузка данных с листа ===
async def get_sheet_data(sheet_gid):
    try:
        client = authorize_google_sheets()
        sheet = client.open_by_key(SHEET_ID).get_worksheet_by_id(sheet_gid)
        data = sheet.get_all_records()
        logging.info(f"✅ Загружено {len(data)} записей с листа {sheet_gid}")
        return data
    except Exception as e:
        logging.error(f"❌ Ошибка чтения данных: {e}")
        return []

# === Отправка сообщения в Telegram ===
async def send_telegram_message(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
        logging.info(f"📨 Отправлено сообщение: {message}")
    except error.TelegramError as e:
        logging.error(f"❌ Ошибка отправки Telegram: {e}")

# === Форматирование стажа ===
def format_tenure(months):
    years = months // 12
    months %= 12
    if years and months:
        return f"{years} г. {months} мес."
    elif years:
        return f"{years} г."
    else:
        return f"{months} мес."

# === Проверка дней рождений и стажа ===
async def check_birthdays_and_anniversaries():
    tz = pytz.timezone("Europe/Moscow")
    today = datetime.datetime.now(tz).date()

    data_uchet = await get_sheet_data(SHEET_UCHET_GID)
    data_aup = await get_sheet_data(SHEET_AUP_GID)

    birthdays_today = []
    anniversaries_today = []

    for record in data_uchet:
        name = record.get("Сотрудник", "Неизвестно")
        birth_raw = record.get("Дата рождения", "").strip()
        hire_raw = record.get("Дата приема", "").strip()

        birth_date = None
        hire_date = None

        try:
            if birth_raw:
                birth_date = datetime.datetime.strptime(birth_raw, "%d.%m.%Y").date()
            if hire_raw:
                hire_date = datetime.datetime.strptime(hire_raw, "%d.%m.%Y").date()
        except ValueError:
            continue

        if birth_date and birth_date.day == today.day and birth_date.month == today.month:
            age = today.year - birth_date.year
            birthdays_today.append(f"{name}, {age} лет")

        if hire_date and hire_date.day == today.day:
            months_diff = (today.year - hire_date.year) * 12 + today.month - hire_date.month
            if months_diff > 0 and (months_diff == 1 or months_diff % 3 == 0):
                formatted = format_tenure(months_diff)
                anniversaries_today.append(f"{name}, {formatted} стажа")
                logging.info(f"🎯 Годовщина: {name}, {formatted}, дата приёма: {hire_date}")

    for record in data_aup:
        name = record.get("Сотрудник", "Неизвестно")
        birth_raw = record.get("Дата рождения", "").strip()
        try:
            birth_date = datetime.datetime.strptime(birth_raw, "%d.%m.%Y").date()
            if birth_date.day == today.day and birth_date.month == today.month:
                age = today.year - birth_date.year
                birthdays_today.append(f"{name}, {age} лет")
        except:
            continue

    if birthdays_today:
        await send_telegram_message("🎂 *Сегодня день рождения:* 🎂\n" + "\n".join(birthdays_today))
    if anniversaries_today:
        await send_telegram_message("🎉 *Годовщины стажа:* 🎉\n" + "\n".join(anniversaries_today))

# === Проверка дней рождения в следующем месяце ===
async def check_birthdays_next_month():
    tz = pytz.timezone("Europe/Moscow")
    today = datetime.datetime.now(tz).date()
    if today.day != 25:
        return

    next_month = today.month % 12 + 1
    data = await get_sheet_data(SHEET_AUP_GID)
    upcoming = []

    for record in data:
        name = record.get("Сотрудник", "Неизвестно")
        birth_raw = record.get("Дата рождения", "").strip()
        try:
            birth_date = datetime.datetime.strptime(birth_raw, "%d.%m.%Y").date()
            if birth_date.month == next_month:
                age = today.year - birth_date.year
                upcoming.append(f"{name}, {birth_date.day}.{next_month}, {age} лет")
        except:
            continue

    if upcoming:
        await send_telegram_message("📅 *Дни рождения в следующем месяце:*\n" + "\n".join(upcoming))

# === Ждать до 9:00 МСК ===
async def wait_until(hour, minute, tz_name="Europe/Moscow"):
    tz = pytz.timezone(tz_name)
    while True:
        now = datetime.datetime.now(tz)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if now >= target:
            target += datetime.timedelta(days=1)
        wait = (target - now).total_seconds()
        logging.info(f"⏳ Ждём {wait/60:.1f} минут до {target}")
        await asyncio.sleep(wait)
        await check_birthdays_and_anniversaries()
        await check_birthdays_next_month()

# === Запуск ===
async def main():
    logging.info("🚀 Моментальный запуск проверки при старте контейнера")
    await check_birthdays_and_anniversaries()
    await check_birthdays_next_month()
    await wait_until(9, 0)

if __name__ == "__main__":
    asyncio.run(main())
