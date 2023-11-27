from configparser import ConfigParser

import datetime

from loguru import logger

from asyncpg import create_pool

from pyrogram import Client, filters
from pyrogram.types import Message

config = ConfigParser()
config.read('config.ini')

# Параметры подключения к базе данных
DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'postgres'
DB_PASSWORD = 'savon'
DB_NAME = 'test'

# Параметры Telegram API
api_id = config.getint('pyrogram', 'api_id')
api_hash = config.get('pyrogram', 'api_hash')
bot_token = config.get('pyrogram', 'bot_token')


logger.add("bot.log", rotation="500 MB", compression="zip", backtrace=True, diagnose=True)


async def create_users_table():
    async with db_pool.acquire() as conn:
        query = '''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )'''
        await conn.execute(query)
        logger.info("Создана таблица 'users'")


async def init_db_pool():
    global db_pool
    db_pool = await create_pool(user=DB_USER, password=DB_PASSWORD,
                                host=DB_HOST, port=DB_PORT,
                                database=DB_NAME)
    await create_users_table()
    logger.info("Инициализирован пул подключений к базе данных")


async def check_user_in_db(user_id):
    async with db_pool.acquire() as conn:
        query = 'SELECT COUNT(*) FROM users WHERE user_id=$1'
        result = await conn.fetchval(query, user_id)
        logger.info(f"Проверка пользователя в БД: user_id={user_id}, result={result}")
        return result > 0


async def register_user(user_id):
    async with db_pool.acquire() as conn:
        query = 'INSERT INTO users (user_id) VALUES ($1)'
        await conn.execute(query, user_id)
        logger.info(f"Регистрация пользователя: user_id={user_id}")


async def send_good_day_message(user_id):
    await bot.send_message(chat_id=user_id, text='Добрый день!')
    logger.info(f"Отправка сообщения 'Добрый день!' пользователю: user_id={user_id}")


async def send_material_and_photo(user_id):
    await bot.send_message(chat_id=user_id, text='Подготовила для вас материал')
    with open('1234.jpg', 'rb') as file:
        await bot.send_photo(chat_id=user_id, photo=file)
    logger.info(f"Отправка материала и фотографии пользователю: user_id={user_id}")


async def send_return_message(user_id):
    await bot.send_message(chat_id=user_id, text='Скоро вернусь с новым материалом!')
    logger.info(f"Отправка сообщения 'Скоро вернусь с новым материалом!' пользователю: user_id={user_id}")


bot = Client('bot_session', api_id=api_id, api_hash=api_hash, bot_token=bot_token)
db_pool = None


@bot.on_message(filters.command('users_today') & filters.private)
async def count_users_today(_, message: Message):
    today = datetime.date.today()
    async with db_pool.acquire() as conn:
        query = 'SELECT COUNT(*) FROM users WHERE created_at::date = $1'
        result = await conn.fetchval(query, today)
    await message.reply(f'Количество зарегистрированных пользователей за сегодня: {result}')
    logger.info(f"Подсчет пользователей за сегодня: result={result}")


@bot.on_message(filters.private)
async def handle_new_message(_, message: Message):
    user_id = message.from_user.id
    logger.info(f"Обработка нового сообщения: user_id={user_id}")

    if db_pool is None:
        await init_db_pool()

    user_exists = await check_user_in_db(user_id)
    if not user_exists:
        await register_user(user_id)

    current_time = datetime.datetime.now()
    if current_time.minute == 10:
        await send_good_day_message(user_id)
    elif current_time.minute == 30:
        await send_material_and_photo(user_id)
    elif current_time.minute == 0 and current_time.hour % 2 == 0:
        found_trigger = False
        async for msg in bot.search_messages(chat_id=user_id, query='Хорошего дня'):
            if msg.from_user.username == 'specific_account':
                await send_return_message(user_id)
                found_trigger = True
                break
        if not found_trigger:
            await send_material_and_photo(user_id)
    else:
        await send_return_message(user_id)

bot.run()
