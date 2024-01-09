import logging
import asyncio
import sqlite3
from datetime import datetime
from aiogram.dispatcher.filters import Command
from aiogram.types import ContentType
from aiogram import Bot, Dispatcher, types, executor
from collections import defaultdict
from datetime import datetime, timedelta

API_TOKEN = 'API_TOKEN'
logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

conn = sqlite3.connect('bot_database.db')
cursor = conn.cursor()

# Создаем таблицу для хранения chat_id, если она еще не существует
cursor.execute('''CREATE TABLE IF NOT EXISTS chat_last_message (
                    chat_id INTEGER PRIMARY KEY,
                    last_message_time TEXT)''')

conn.commit()


@dp.message_handler(Command("help"))
async def process_broadcast_command(message: types.Message):
    chat_id = message.chat.id
    conn = sqlite3.connect('bot_database.db')  # Подключаемся к базе данных
    c = conn.cursor()
    c.execute("SELECT * FROM chat_last_message WHERE chat_id = ?", (chat_id,))
    
    if (c.fetchone()):
        c.execute("REPLACE INTO chat_last_message (chat_id, last_message_time) VALUES (?, ?)",(chat_id, str(datetime.now())))  # Добавляем chat_id в таблицу
    else:
        c.execute("INSERT INTO chat_last_message (chat_id, last_message_time) VALUES (?, ?)", (chat_id, str(datetime.now())))
    conn.commit()  # Сохраняем изменения
    conn.close()  # Закрываем соединение
    
    await message.reply("Бот запущен.")


async def update_last_message_time(chat_id):
    conn = sqlite3.connect('bot_database.db')  # Подключаемся к базе данных
    c = conn.cursor()
    c.execute("SELECT * FROM chat_last_message WHERE chat_id = ?", (chat_id,))
    
    if (c.fetchone()):
        c.execute("REPLACE INTO chat_last_message (chat_id, last_message_time) VALUES (?, ?)",(chat_id, str(datetime.now())))  # Добавляем chat_id в таблицу
    else:
        c.execute("INSERT INTO chat_last_message (chat_id, last_message_time) VALUES (?, ?)", (chat_id, str(datetime.now())))
    conn.commit()  # Сохраняем изменения
    conn.close()  # Закрываем соединение
    
@dp.message_handler(content_types=types.ContentType.NEW_CHAT_MEMBERS)
async def greet_new_members(message: types.Message):
    chat_id = message.chat.id
    conn = sqlite3.connect('bot_database.db')  # Подключаемся к базе данных
    c = conn.cursor()
    c.execute("SELECT * FROM chat_last_message WHERE chat_id = ?", (chat_id,))
    
    if (c.fetchone()):
        c.execute("REPLACE INTO chat_last_message (chat_id, last_message_time) VALUES (?, ?)",(chat_id, str(datetime.now())))  # Добавляем chat_id в таблицу
    else:
        c.execute("INSERT INTO chat_last_message (chat_id, last_message_time) VALUES (?, ?)", (chat_id, str(datetime.now())))
    conn.commit()  # Сохраняем изменения
    conn.close()  # Закрываем соединение
    


async def get_last_message_time(chat_id):
    conn = sqlite3.connect('bot_database.db')  # Подключаемся к базе данных
    c = conn.cursor()
    c.execute("SELECT last_message_time FROM chat_last_message WHERE chat_id = ?",
    (chat_id,))
    result = c.fetchone()
    conn.commit()  # Сохраняем изменения
    conn.close()  # Закрываем соединение
    
    if result:
        return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
    return None

async def send_reminder(chat_id):
    await bot.send_message(chat_id, "Добрый день!\n\nКак ваши успехи в обучении питомца?\n\nПожалуйста, не забывайте давайте обратную связь по заданиям и динамике обучения. \
    Это важная часть обучения, благодаря которой мы быстрее придём к результату 🙏 \n\n(таймер 2 часа выставлен)")

@dp.message_handler()
async def reminder_handler(message: types.Message):
    chat_id = message.chat.id
    await update_last_message_time(chat_id)

async def check_last_message_times():
    while True:
        await asyncio.sleep(60)  # Проверка каждую минуту

        current_time = datetime.now()
        
        conn = sqlite3.connect('bot_database.db')  # Подключаемся к базе данных
        c = conn.cursor()
        c.execute("SELECT chat_id FROM chat_last_message")
        chat_ids = [record[0] for record in c.fetchall()]
        conn.commit()  # Сохраняем изменения
        conn.close()  # Закрываем соединение

        for chat_id in chat_ids:
            last_message_time = await get_last_message_time(chat_id)
            if last_message_time:
                time_difference = current_time - last_message_time
                if time_difference >= timedelta(minutes=120):
                    conn = sqlite3.connect('bot_database.db')  # Подключаемся к базе данных
                    c = conn.cursor()
                    c.execute("REPLACE INTO chat_last_message (chat_id, last_message_time) VALUES (?, ?)",(chat_id, str(datetime.now())))  # Добавляем chat_id в таблицу
                    conn.commit()  # Сохраняем изменения
                    conn.close()  # Закрываем соединение
                    
                    try:
                        await send_reminder(chat_id)
                    except:
                        continue


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(check_last_message_times())
    loop.create_task(dp.start_polling())
    loop.run_forever()
