import os
import json
import logging
import asyncio
import datetime
import gspread
import base64
import pytz
from google.oauth2.service_account import Credentials
from telegram import Bot, error

# Настройки
SHEET_ID = os.getenv("SHEET_ID")
CHAT_ID = os.getenv("CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# Логирование
logging.basicConfig(level=logging.INFO)

# Декодирование CREDENTIALS_JSON из Base64
try:
    credentials_base64 = os.getenv("CREDENTIALS_JSON")
    if not credentials_base64:
        raise ValueError("CREDENTIALS_JSON не задана!")

    logging.info(f"DEBUG: CREDENTIALS_JSON length: {len(credentials_base64)}")
    logging.info(f"DEBUG: CREDENTIALS_JSON first 50 chars: {credentials_base64[:50]}")

    missing_padding = len(credentials_base64) % 4
    if missing_padding:
        credentials_base64 += "=" * (4 - missing_padding)

    credentials_json = base64.b64decode(credentials_base64).decode("utf-8")
    CREDENTIALS_JSON = json.loads(credentials_json)
    logging.info("✅ CREDENTIALS_JSON успешно загружен!")
except Exception as e:
    logging.error(f"❌ Ошибка при загрузке CREDENTIALS_JSON: {e}")
    raise

SENT_DATA_FILE = "sent_data.json"

try:
    bot = Bot(token=TELEGRAM_TOKEN)
    logging.info("✅ Telegram бот успешно инициализирован!")
except Exception as e:
    logging.error(f"❌ Ошибка при инициализации Telegram бота: {e}")
    raise

# Авторизация Google Sheets API
def authorize_google_sheets():
    try:
        logging.info(f"DEBUG: Авторизация с SHEET_ID: {SHEET_ID}")
        logging.info(f"DEBUG: Авторизация с client_email: {CREDENTIALS_JSON.get('client_email')}")
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

# Чтение сохранённых данных
def load_sent_data():
    try:
        with open(SENT_DATA_FILE, "r") as file:
            data = json.load(file)
            if "sent_today" not in data:
                data["sent_today"] = []
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {"sent_today": []}

# Сохранение данных
def save_sent_data(sent_data):
    with open(SENT_DATA_FILE, "w") as file:
        json.dump(sent_data, file, indent=4, default=str)

# Получение данных из Google Sheets
async def get_sheet_data():
    try:
        sheet = authorize_google_sheets()
        raw_data = sheet.get_all_values()
        logging.info(f"DEBUG: Сырые данные из Google Sheets: {raw_data}")
        data = sheet.get_all_records()
        logging.info(f"DEBUG: Загружено {len(data)} записей из Google Sheets")
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
        birth_date_raw = record.get("Дата рождения", "").strip().replace("\xa0", " ")
        hire_date_raw = record.get("Дата приема", "").strip().replace("\xa0", " ")

        try:
            birth_date = datetime.datetime.strptime(birth_date_raw, "%d.%m.%Y").date() if birth_date_raw else None
            hire_date = datetime.datetime.strptime(hire_date_raw, "%d.%m.%Y").date() if hire_date_raw else None
        except ValueError:
            logging.warning(f"⚠ Ошибка парсинга даты у {name}: {birth_date_raw} | {hire_date_raw}")
            continue

        logging.info(f"Проверка ДР: {name}, дата рождения: {birth_date}")

        if birth_date and birth_date.day == today.day and birth_date.month == today.month:
            if name not in sent_data["sent_today"]:
                birthdays.append(f"🎉 {name} ({birth_date.strftime('%d.%m.%Y')})")
                new_notifications.append(name)

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

MOSCOW_TZ = pytz.timezone("Europe/Moscow")

async def periodic_check():
    sent_data = load_sent_data()
    while True:
        now = datetime.datetime.now(MOSCOW_TZ)
        next_check = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now.hour >= 14:
            next_check += datetime.timedelta(days=1)
        elif now.hour >= 9:
            next_check = now.replace(hour=14, minute=0, second=0, microsecond=0)
        wait_seconds = (next_check - now).total_seconds()
        logging.info(f"⏳ Ожидание {wait_seconds // 3600:.0f} ч {wait_seconds % 3600 // 60:.0f} мин")
        await asyncio.sleep(wait_seconds)
        data = await get_sheet_data()
        if data:
            await check_and_notify(data, sent_data)

if __name__ == "__main__":
    asyncio.run(periodic_check())
