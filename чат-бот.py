import os
import json
import logging
import asyncio
import datetime
import gspread
from google.oauth2.service_account import Credentials
from telegram import Bot, error

# Включаем логирование
logging.basicConfig(level=logging.INFO)

# Читаем переменные окружения
SHEET_ID = os.getenv("SHEET_ID")
CHAT_ID = os.getenv("CHAT_ID")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
credentials_json = os.getenv("CREDENTIALS_JSON")

# Проверяем переменные окружения
if not SHEET_ID:
    logging.error("❌ Ошибка: SHEET_ID не найден!")
    exit(1)
if not CHAT_ID:
    logging.error("❌ Ошибка: CHAT_ID не найден!")
    exit(1)
if not TELEGRAM_TOKEN:
    logging.error("❌ Ошибка: TELEGRAM_TOKEN не найден!")
    exit(1)
if not credentials_json:
    logging.error("❌ Ошибка: CREDENTIALS_JSON не найден!")
    exit(1)

# Загружаем учетные данные Google
try:
    CREDENTIALS_JSON = json.loads(credentials_json)
    logging.info("✅ CREDENTIALS_JSON успешно загружен!")
except json.JSONDecodeError as e:
    logging.error(f"❌ Ошибка декодирования CREDENTIALS_JSON: {e}")
    exit(1)

# Файл для хранения отправленных сообщений
SENT_DATA_FILE = "sent_data.json"

# Инициализация Telegram бота
bot = Bot(token=TELEGRAM_TOKEN)

# Авторизация в Google Sheets API
def authorize_google_sheets():
    try:
        creds = Credentials.from_service_account_info(
            CREDENTIALS_JSON,
            scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        logging.info("✅ Успешно подключено к Google Sheets!")
        return sheet
    except Exception as e:
        logging.error(f"❌ Ошибка при подключении к Google Sheets: {e}")
        exit(1)

# Безопасное парсинг даты
def safe_parse_date(date_value):
    if not date_value or date_value.strip() == "":
        return None
    try:
        return datetime.datetime.strptime(date_value.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None

# Чтение сохраненных данных
def load_sent_data():
    try:
        with open(SENT_DATA_FILE, "r") as file:
            data = json.load(file)
            if "sent_today" not in data:
                data["sent_today"] = []
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {"sent_today": []}

# Сохранение отправленных сообщений
def save_sent_data(sent_data):
    with open(SENT_DATA_FILE, "w") as file:
        json.dump(sent_data, file, indent=4, default=str)

# Получение данных из Google Sheets
async def get_sheet_data():
    try:
        sheet = authorize_google_sheets()
        return sheet.get_all_records()
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке данных из Google Sheets: {e}")
        return []

# Отправка сообщений в Telegram
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
        birth_date_raw = record.get("Дата рождения", "")
        hire_date_raw = record.get("Дата приема", "")

        birth_date = safe_parse_date(birth_date_raw)
        hire_date = safe_parse_date(hire_date_raw)

        # Проверка дня рождения
        if birth_date and birth_date.day == today.day and birth_date.month == today.month:
            if name not in sent_data["sent_today"]:
                birthdays.append(f"🎉 {name} ({birth_date.strftime('%d.%m.%Y')})")
                new_notifications.append(name)

        # Проверка годовщины работы
        if hire_date:
            months_worked = (today.year - hire_date.year) * 12 + today.month - hire_date.month
            if months_worked > 0 and (months_worked == 1 or months_worked % 3 == 0):
                if name not in sent_data["sent_today"]:
                    years = months_worked // 12
                    months = months_worked % 12
                    if years > 0:
                        anniversary_text = f"{years} лет" if months == 0 else f"{years} лет {months} месяцев"
                    else:
                        anniversary_text = f"{months} месяцев"
                    anniversaries.append(f"🎊 {name}: {anniversary_text}")
                    new_notifications.append(name)

    # Формируем сообщение
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

# Основной цикл проверки
async def periodic_check():
    sent_data = load_sent_data()
    while True:
        try:
            logging.info("🔹 Проверка новых событий...")
            data = await get_sheet_data()
            if data:
                await check_and_notify(data, sent_data)
        except Exception as e:
            logging.error(f"❌ Ошибка: {e}")

        logging.info("🔹 Ожидание 3 минуты перед следующей проверкой...")
        await asyncio.sleep(180)

if __name__ == "__main__":
    asyncio.run(periodic_check())
