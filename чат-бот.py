import datetime
import asyncio
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Bot, error

TELEGRAM_TOKEN = "7725100224:AAFkRGv7flv_k-FMbeOs0Jo1QzHY4Sbfj_E"
CHAT_ID = "-1002459442462"

SENT_DATA_FILE = "/home/nikita/sent_data.json"
CREDENTIALS_FILE = "/home/nikita/Рабочий стол/ботGg/botgg-448705-53d98bb82ea4.json"

SHEET_ID = "1Unw36FtjV6bXeO-15Qu2KpS8kjnkti3z2yXgsqHt1pk"

bot = Bot(token=TELEGRAM_TOKEN)

def safe_parse_date(date_value):
    if not date_value or date_value.strip() == "":
        return None
    try:
        parsed_date = datetime.datetime.strptime(date_value.strip(), "%d.%m.%Y").date()
        print(f"✅ Дата успешно преобразована: {parsed_date}")
        return parsed_date
    except ValueError:
        print(f"⚠ Невозможно преобразовать дату: {date_value}")
        return None

def load_sent_data():
    try:
        with open(SENT_DATA_FILE, "r") as file:
            data = json.load(file)
            if "sent_today" not in data:
                data["sent_today"] = []
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        print("⚠ Файл данных отсутствует или повреждён, создаём новый.")
        return {"sent_today": []}

def save_sent_data(sent_data):
    try:
        with open(SENT_DATA_FILE, "w") as file:
            json.dump(sent_data, file, indent=4, default=str)
    except Exception:
        pass

async def get_sheet_data():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        return sheet.get_all_records()
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных из Google Sheets: {e}")
        return []

async def send_telegram_message(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
        print(f"✅ Сообщение отправлено: {message}")
    except error.TelegramError as e:
        print(f"❌ Ошибка при отправке сообщения: {e}")

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

        # Отладочная информация
        print(f"Обрабатывается сотрудник: {name}")
        print(f"  Дата рождения: {birth_date} (сырые данные: {birth_date_raw})")
        print(f"  Дата приема: {hire_date} (сырые данные: {hire_date_raw})")
        print(f"  Сегодняшняя дата: {today}")

        # Проверка дня рождения
        if birth_date:
            if birth_date.day == today.day and birth_date.month == today.month:
                print(f"🎂 Найдено совпадение по дню рождения для {name}")
                if name not in sent_data["sent_today"]:
                    birthdays.append(f"🎉 {name} ({birth_date.strftime('%d.%m.%Y')})")
                    new_notifications.append(name)
                else:
                    print(f"⚠ {name} уже был поздравлен сегодня.")
            else:
                print(f"❌ У {name} не совпадает день рождения с сегодняшней датой.")

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

async def periodic_check():
    sent_data = load_sent_data()
    while True:
        try:
            print("🔹 Проверка новых событий...")
            data = await get_sheet_data()
            if data:
                await check_and_notify(data, sent_data)
        except Exception as e:
            print(f"❌ Ошибка: {e}")

        print("🔹 Ожидание 3 минуты перед следующей проверкой...")
        await asyncio.sleep(180)

if __name__ == "__main__":
    asyncio.run(periodic_check())

