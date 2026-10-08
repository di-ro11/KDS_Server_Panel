import asyncio
import os
import logging
import math
import csv
import io
from datetime import datetime, timedelta
import uuid

import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BufferedInputFile
from dotenv import load_dotenv

from aiohttp import web
from aiocryptopay import AioCryptoPay, Networks
from yookassa import Configuration, Payment
from aiogram.client.default import DefaultBotProperties
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application


from keyboards.inline import *
from utils.crypto import *
from utils.ssh import *

# --- Конфигурация ---
load_dotenv('../.env')
logging.basicConfig(level=logging.INFO)
BOT_TOKEN, ADMIN_ID = os.getenv('BOT_TOKEN'), int(os.getenv('ADMIN_ID'))
SUPPORT_USERNAME = os.getenv('SUPPORT_USERNAME')
DB_USER, DB_PASS, DB_NAME, DB_HOST, DB_PORT = os.getenv('POSTGRES_USER'), os.getenv('POSTGRES_PASSWORD'), os.getenv('POSTGRES_DB'), os.getenv('DB_HOST'), os.getenv('DB_PORT')
CRYPTO_PAY_TOKEN, YK_SHOP_ID, YK_SECRET_KEY = os.getenv('CRYPTO_PAY_TOKEN'), os.getenv('YK_SHOP_ID'), os.getenv('YK_SECRET_KEY')
BOT_VERSION, VIP_PRICE = "2.1.0-stable", "49₽/месяц" # Версия обновлена
WEB_SERVER_HOST, WEB_SERVER_PORT = "0.0.0.0", 8080

# --- ПУТИ ВЕБХУКОВ ---
WEBHOOK_BASE_URL = "/webhook"
WEBHOOK_TELEGRAM_PATH = f"{WEBHOOK_BASE_URL}/telegram"
WEBHOOK_CRYPTO_PAY_PATH = f"{WEBHOOK_BASE_URL}/cryptopay"
WEBHOOK_YOOKASSA_PATH = f"{WEBHOOK_BASE_URL}/yookassa"


# --- Инициализация ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
db_pool = None
cryptopay = AioCryptoPay(token=CRYPTO_PAY_TOKEN, network=Networks.MAIN_NET)
if YK_SHOP_ID and YK_SECRET_KEY:
    Configuration.configure(YK_SHOP_ID, YK_SECRET_KEY)

# --- FSM Состояния ---
class AddServer(StatesGroup): name,ip,port,login,password = State(),State(),State(),State(),State()
class TerminalSession(StatesGroup): active = State()
class FileManagerSession(StatesGroup): browsing=State(); uploading=State()
class RenameServer(StatesGroup): new_name = State()
class ChangePassword(StatesGroup): waiting_for_password = State()
class Broadcast(StatesGroup): message = State(); confirmation = State()
class AdminSearchUser(StatesGroup): by_id = State()
class AdminMessageUser(StatesGroup): waiting_for_message = State()
class AdminSearchServer(StatesGroup): by_id = State()
class AdminEditContent(StatesGroup): waiting_for_text = State()


# --- Функции БД ---
async def create_db_pool():
    global db_pool
    for i in range(5):
        try:
            db_pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=DB_HOST, port=DB_PORT, timeout=10)
            logging.info("Пул подключений к базе данных успешно создан")
            return
        except Exception as e:
            logging.error(f"Попытка {i+1}/5: Не удалось создать пул подключений к БД: {e}")
            await asyncio.sleep(5)

async def get_or_create_user(telegram_id: int, username: str, first_name: str) -> asyncpg.Record:
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE telegram_id = $1", telegram_id)
        if user:
            return user
        is_admin = (telegram_id == ADMIN_ID)
        vip_expires = (datetime.now() + timedelta(days=365*100)) if is_admin else None
        return await conn.fetchrow("INSERT INTO users (telegram_id, username, first_name, is_admin, is_vip, vip_expires) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *", telegram_id, username, first_name, is_admin, is_admin, vip_expires)

async def get_db_user_id(telegram_id: int) -> int or None:
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE telegram_id = $1", telegram_id)
        return user['id'] if user else None

async def add_server_to_db(user_id: int, data: dict) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO servers (user_id, name, ip, port, login_user, password_encrypted) VALUES ($1, $2, $3, $4, $5, $6)", user_id, data['name'], data['ip'], data['port'], data['login'], encrypt_password(data['password']))

async def get_user_servers(user_id: int) -> list:
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT id, name FROM servers WHERE user_id = $1 ORDER BY name", user_id)

async def get_server_details(server_id: int, user_id: int) -> asyncpg.Record or None:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM servers WHERE id = $1 AND user_id = $2", server_id, user_id)

async def delete_server_from_db(server_id: int, user_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM servers WHERE id = $1 AND user_id = $2", server_id, user_id)

async def update_server_name(server_id: int, user_id: int, new_name: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE servers SET name = $1 WHERE id = $2 AND user_id = $3", new_name, server_id, user_id)

async def update_server_password(server_id: int, user_id: int, new_password_encrypted: str) -> None:
     async with db_pool.acquire() as conn:
        await conn.execute("UPDATE servers SET password_encrypted = $1 WHERE id = $2 AND user_id = $3", new_password_encrypted, server_id, user_id)

async def get_total_users_count() -> int:
    async with db_pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM users")

async def get_total_servers_count() -> int:
    async with db_pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM servers")

async def create_subscription_record(user_id: int, amount: float, provider: str, invoice_id: str, days: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO subscriptions (user_id, amount, provider, payment_id, status, duration_days) VALUES ($1, $2, $3, $4, 'pending', $5)", user_id, amount, provider, invoice_id, days)

async def get_all_users_ids() -> list:
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT telegram_id FROM users")

async def get_subscription_by_payment_id(payment_id: str) -> asyncpg.Record or None:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM subscriptions WHERE payment_id = $1", payment_id)

async def mark_subscription_paid(payment_id: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE subscriptions SET status = 'paid' WHERE payment_id = $1", payment_id)

async def activate_vip_for_user(user_id: int, days_to_add: int) -> None:
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT is_vip, vip_expires FROM users WHERE id = $1", user_id)
        if not user:
            return
        start_date = datetime.now()
        if user['is_vip'] and user['vip_expires'] and user['vip_expires'] > start_date:
            start_date = user['vip_expires']
        new_expires_date = start_date + timedelta(days=days_to_add)
        await conn.execute("UPDATE users SET is_vip = TRUE, vip_expires = $1 WHERE id = $2", new_expires_date, user_id)

async def get_user_by_telegram_id(telegram_id: int) -> asyncpg.Record or None:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE telegram_id = $1", telegram_id)

async def admin_delete_server(server_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM servers WHERE id = $1", server_id)

async def admin_set_vip_status(user_tg_id: int, status: bool, duration_days: int = 0):
    async with db_pool.acquire() as conn:
        if status:
            db_user_id = await get_db_user_id(user_tg_id)
            if db_user_id:
                user = await conn.fetchrow("SELECT is_vip, vip_expires FROM users WHERE id = $1", db_user_id)
                if not user:
                    return False
                start_date = datetime.now()
                if user['is_vip'] and user['vip_expires'] and user['vip_expires'] > start_date:
                    start_date = user['vip_expires']
                new_expires_date = start_date + timedelta(days=duration_days)
                await conn.execute("UPDATE users SET is_vip = TRUE, vip_expires = $1 WHERE id = $2", new_expires_date, db_user_id)
                return True
            return False
        else:
            await conn.execute("UPDATE users SET is_vip = FALSE, vip_expires = NULL WHERE telegram_id = $1", user_tg_id)
            return True

async def admin_get_server_by_id(server_id: int) -> asyncpg.Record or None:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT s.*, u.telegram_id as owner_tg_id, u.username as owner_username
            FROM servers s
            JOIN users u ON s.user_id = u.id
            WHERE s.id = $1
            """,
            server_id
        )

async def admin_get_all_vips_paginated(page: int, per_page: int = 5):
    offset = page * per_page
    async with db_pool.acquire() as conn:
        query = """
            SELECT telegram_id, username, vip_expires
            FROM users
            WHERE is_vip = TRUE AND vip_expires > NOW()
            ORDER BY vip_expires ASC
            LIMIT $1 OFFSET $2
        """
        vip_users = await conn.fetch(query, per_page, offset)

        total_vips_query = "SELECT COUNT(*) FROM users WHERE is_vip = TRUE AND vip_expires > NOW()"
        total_count = await conn.fetchval(total_vips_query)

        return vip_users, total_count

async def admin_get_all_users_for_export() -> list:
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM users ORDER BY id ASC")

async def admin_get_all_servers_for_export() -> list:
    async with db_pool.acquire() as conn:
        return await conn.fetch("""
            SELECT s.*, u.telegram_id as owner_telegram_id
            FROM servers s
            LEFT JOIN users u ON s.user_id = u.id
            ORDER BY s.id ASC
        """)

async def get_setting(key: str, default: str = None) -> str:
    async with db_pool.acquire() as conn:
        record = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
        return record['value'] if record else default

async def update_setting(key: str, value: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()
            """,
            key, value
        )

# --- Хелперы ---
async def get_full_welcome_text(user_record: asyncpg.Record) -> str:
    user_name = user_record['username'] or user_record['first_name']

    welcome_message_template = await get_setting('welcome_message', default="🚀 <b>Добро пожаловать в KDS Server Panel!</b>\n\nЭтот бот поможет вам управлять вашими серверами.")

    vip_status = "Бесплатный"
    if user_record['is_vip'] and user_record['vip_expires']:
        expires_delta = (user_record['vip_expires'] - datetime.now()).days
        if expires_delta > 365 * 50:
            vip_status = "Вечный 👑"
        else:
            vip_status = f"VIP до {user_record['vip_expires'].strftime('%d.%m.%Y')} 👑"

    rights = "Администратор 🛠️" if user_record['is_admin'] else "Пользователь"

    full_text = (
        f"{welcome_message_template}\n\n"
        f"✅ Подключайте серверы\n✅ Выполняйте команды\n✅ Управляйте файлами\n\n"
        f"👤 <b>Пользователь:</b> {user_name}\n"
        f"🆔 <b>Ваш ID:</b> <code>{user_record['telegram_id']}</code>\n"
        f"🆓 <b>Статус:</b> {vip_status}\n"
        f"🔧 <b>Права:</b> {rights}\n\n"
        f"💬 <b>Поддержка:</b> {SUPPORT_USERNAME}"
    )
    return full_text

async def get_status_message_text(user_record: asyncpg.Record, total_users: int, total_servers: int) -> str:
    now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    vip_status = "Бесплатный"
    if user_record and user_record['is_vip'] and user_record['vip_expires']:
        expires_delta = (user_record['vip_expires'] - datetime.now()).days
        if expires_delta > 365*50:
            vip_status = "Вечный"
        else:
            vip_status = f"VIP до {user_record['vip_expires'].strftime('%d.%m.%Y')}"
    admin_status = "Да" if user_record and user_record['is_admin'] else "Нет"
    reg_date = user_record['created_at'].strftime('%d.%m.%Y %H:%M') if user_record else "н/д"
    return (f"🤖 <b>Статус бота</b>\n\n⏰ <b>Время:</b> {now}\n👥 <b>Пользователи:</b> {total_users}\n🖥️ <b>Серверы:</b> {total_servers}\n"
            f"👑 <b>VIP цена:</b> {VIP_PRICE}\n🎯 <b>Версия:</b> {BOT_VERSION}\n✅ <b>Статус:</b> Работает\n\n"
            f"📊 <b>Ваш профиль:</b>\n• <b>ID:</b> <code>{user_record['telegram_id'] if user_record else 'н/д'}</code>\n"
            f"• <b>Статус:</b> {vip_status}\n• <b>Администратор:</b> {admin_status}\n• <b>Регистрация:</b> {reg_date}")

async def show_found_user_info(message_or_callback, user_tg_id: int):
    user_record = await get_user_by_telegram_id(user_tg_id)
    if not user_record:
        msg_target = message_or_callback if isinstance(message_or_callback, types.Message) else message_or_callback.message
        await msg_target.edit_text("Меню управления пользователями.", reply_markup=admin_users_keyboard())
        await message_or_callback.answer("❌ Пользователь с таким ID не найден.", show_alert=True)
        return

    db_user_id = user_record['id']
    servers = await get_user_servers(db_user_id)

    vip_status = "Нет"
    if user_record['is_vip'] and user_record['vip_expires']:
        expires_delta = (user_record['vip_expires'] - datetime.now()).days
        if expires_delta > 365 * 50:
            vip_status = "Вечный 👑"
        else:
            vip_status = f"VIP до {user_record['vip_expires'].strftime('%d.%m.%Y')}"

    text = (f"👤 <b>Информация о пользователе:</b>\n\n"
            f"<b>TG ID:</b> <code>{user_record['telegram_id']}</code>\n"
            f"<b>Username:</b> @{user_record['username']}\n"
            f"<b>Имя:</b> {user_record['first_name']}\n"
            f"<b>VIP Статус:</b> {vip_status}\n"
            f"<b>Админ:</b> {'Да' if user_record['is_admin'] else 'Нет'}\n"
            f"<b>Дата регистрации:</b> {user_record['created_at'].strftime('%d.%m.%Y %H:%M')}\n"
            f"<b>Серверов добавлено:</b> {len(servers)}")

    if isinstance(message_or_callback, types.Message):
        await message_or_callback.answer(text, reply_markup=admin_user_details_keyboard(servers, user_tg_id))
    else:
        await message_or_callback.message.edit_text(text, reply_markup=admin_user_details_keyboard(servers, user_tg_id))

async def show_admin_found_server_info(message_or_callback, server_id: int):
    server_record = await admin_get_server_by_id(server_id)

    msg_target = message_or_callback if isinstance(message_or_callback, types.Message) else message_or_callback.message

    if not server_record:
        if isinstance(message_or_callback, types.CallbackQuery):
             await message_or_callback.answer("❌ Сервер с таким ID не найден.", show_alert=True)
        await msg_target.edit_text("Меню управления серверами.", reply_markup=admin_servers_menu_keyboard())
        return

    text = (f"🖥️ <b>Информация о сервере (ID: {server_record['id']})</b>\n\n"
            f"<b>Имя:</b> {server_record['name']}\n"
            f"<b>Адрес:</b> <code>{server_record['ip']}:{server_record['port']}</code>\n"
            f"<b>Логин:</b> <code>{server_record['login_user']}</code>\n"
            f"<b>Дата добавления:</b> {server_record['created_at'].strftime('%d.%m.%Y %H:%M')}\n\n"
            f"👤 <b>Владелец</b>\n"
            f"<b>ID:</b> <code>{server_record['owner_tg_id']}</code>\n"
            f"<b>Username:</b> @{server_record['owner_username']}")

    await msg_target.edit_text(text, reply_markup=admin_server_details_keyboard(server_record['id'], server_record['owner_tg_id']))


# --- Вебхуки ---
async def yookassa_webhook_handler(request: web.Request) -> web.Response:
    try:
        event_json = await request.json()
        if event_json.get('event') == 'payment.succeeded':
            payment_object = event_json['object']
            sub = await get_subscription_by_payment_id(payment_object['id'])
            if sub and sub['status'] != 'paid':
                user_record = await get_user_by_telegram_id(int(payment_object['metadata']['telegram_id']))
                if user_record:
                    await activate_vip_for_user(sub['user_id'], sub['duration_days'])
                    await mark_subscription_paid(payment_object['id'])
                    await bot.send_message(user_record['telegram_id'], "✅ Оплата через ЮKassa прошла успешно! VIP активирован.")
        return web.Response(status=200)
    except Exception as e:
        logging.error(f"Ошибка в вебхуке ЮKassa: {e}")
        return web.Response(status=500)

async def cryptopay_webhook_handler(request: web.Request) -> web.Response:
    try:
        update = await request.json()
        if update.get('update_type') == 'invoice_paid':
            invoice_id = str(update['payload']['invoice_id'])
            sub = await get_subscription_by_payment_id(invoice_id)
            if sub and sub['status'] != 'paid':
                user_telegram_id = await db_pool.fetchval("SELECT telegram_id FROM users WHERE id=$1", sub['user_id'])
                if user_telegram_id:
                    await activate_vip_for_user(sub['user_id'], sub['duration_days'])
                    await mark_subscription_paid(invoice_id)
                    await bot.send_message(user_telegram_id, "✅ Оплата через CryptoPay прошла успешно! VIP активирован.")
        return web.Response(status=200)
    except Exception as e:
        logging.error(f"Ошибка в вебхуке CryptoPay: {e}")
        return web.Response(status=500)


# --- Основные обработчики команд и кнопок ---
@dp.message(CommandStart())
async def handle_start(message: types.Message, state: FSMContext):
    await state.clear()
    user = await get_or_create_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    welcome_text = await get_full_welcome_text(user)
    await message.answer(welcome_text, reply_markup=main_menu_keyboard(user['is_admin']))

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    total_users, total_servers = await get_total_users_count(), await get_total_servers_count()
    admin_record = await get_user_by_telegram_id(ADMIN_ID)
    if admin_record:
        await message.answer(await get_status_message_text(admin_record, total_users, total_servers))

@dp.message(Command("cancel"))
async def cancel_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return

    logging.info(f"Cancelling state {current_state} for user {message.from_user.id}")
    await state.clear()
    await message.answer("Действие отменено.", reply_markup=main_menu_keyboard(message.from_user.id == ADMIN_ID))

@dp.callback_query(F.data == "back_to_main_menu")
async def cq_back_to_main_menu(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user = await get_or_create_user(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)
    welcome_text = await get_full_welcome_text(user)
    await callback.message.edit_text(welcome_text, reply_markup=main_menu_keyboard(user['is_admin']))
    await callback.answer()

@dp.callback_query(F.data == "list_servers")
async def cq_list_servers(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    db_user_id = await get_db_user_id(callback.from_user.id)
    servers = await get_user_servers(db_user_id)
    await callback.message.edit_text("У вас нет серверов." if not servers else "🖥️ <b>Выберите сервер:</b>", reply_markup=servers_list_keyboard(servers))
    await callback.answer()

@dp.callback_query(F.data == "vip_subscription")
async def cq_vip_subscription(callback: types.CallbackQuery):
    vip_info_text = await get_setting('vip_info', default="👑 <b>VIP-Подписка</b>\n\nVIP-статус открывает доступ к расширенным возможностям и снимает все ограничения.")
    await callback.message.edit_text(vip_info_text, reply_markup=vip_menu_keyboard())

@dp.callback_query(F.data == "my_vip_status")
async def cq_my_vip_status(callback: types.CallbackQuery):
    user = await get_user_by_telegram_id(callback.from_user.id)
    expires_str = ""
    if user and user['is_vip'] and user['vip_expires']:
        expires_delta = (user['vip_expires'] - datetime.now()).days
        if expires_delta > 365*50:
            expires_str = "никогда"
        else:
            expires_str = user['vip_expires'].strftime('%d.%m.%Y')

    text = f"✅ Ваш VIP-статус активен.\nСрок окончания: <b>{expires_str}</b>" if user and user['is_vip'] else "❌ У вас нет активной VIP-подписки."
    await callback.answer(text, show_alert=True)

@dp.callback_query(F.data == "buy_vip")
async def cq_buy_vip(callback: types.CallbackQuery):
    await callback.message.edit_text("Выберите подходящий тариф:", reply_markup=choose_tariff_keyboard())

@dp.callback_query(F.data.startswith("choose_tariff:"))
async def cq_choose_tariff(callback: types.CallbackQuery):
    _, days_str, amount_str = callback.data.split(":")
    await callback.message.edit_text("Выберите удобный способ оплаты:", reply_markup=choose_payment_method_keyboard(int(days_str), float(amount_str)))

@dp.callback_query(F.data.startswith("pay:yookassa:"))
async def cq_pay_yookassa(callback: types.CallbackQuery):
    _, _, days_str, _ = callback.data.split(":")
    days, prices_rub = int(days_str), {30: 49, 90: 129, 180: 199, 365: 349}
    price_rub = prices_rub.get(days, 49)
    await callback.message.edit_text("⏳ Создаю счет в ЮKassa...")
    try:
        db_user_id = await get_db_user_id(callback.from_user.id)
        bot_info = await bot.get_me()
        payment = Payment.create({
            "amount": {"value": f"{price_rub}.00", "currency": "RUB"},
            "confirmation": {"type": "redirect", "return_url": f"https://t.me/{bot_info.username}"},
            "capture": True,
            "description": f"VIP подписка на {days} дней",
            "metadata": {'telegram_id': callback.from_user.id, 'user_id': db_user_id}
        }, str(uuid.uuid4()))
        await create_subscription_record(db_user_id, price_rub, 'yookassa', payment.id, days)
        await callback.message.edit_text(f"🥝 <b>Счет в ЮKassa создан</b>\n\n<b>Сумма:</b> {price_rub} RUB", reply_markup=payment_keyboard(payment.confirmation.confirmation_url, "yookassa", payment.id))
    except Exception as e:
        logging.error(f"Ошибка создания счета ЮKassa: {e}")
        await callback.message.edit_text("❌ Не удалось создать счет.", reply_markup=vip_menu_keyboard())

@dp.callback_query(F.data.startswith("pay:cryptobot:"))
async def cq_pay_cryptobot(callback: types.CallbackQuery):
    _, _, days_str, amount_str = callback.data.split(":")
    days, amount = int(days_str), float(amount_str)
    await callback.message.edit_text("⏳ Создаю счет в CryptoPay...")
    try:
        invoice = await cryptopay.create_invoice(asset='USDT', amount=amount, description=f"VIP на {days} дней", expires_in=900)
        db_user_id = await get_db_user_id(callback.from_user.id)
        await create_subscription_record(db_user_id, amount, 'cryptopay', str(invoice.invoice_id), days)
        await callback.message.edit_text(f"🤖 <b>Счет в CryptoPay создан</b>\n\n<b>Сумма:</b> {amount} USDT", reply_markup=payment_keyboard(invoice.bot_invoice_url, "cryptopay", str(invoice.invoice_id)))
    except Exception as e:
        logging.error(f"Ошибка создания счета CryptoPay: {e}")
        await callback.message.edit_text("❌ Не удалось создать счет.", reply_markup=vip_menu_keyboard())

@dp.callback_query(F.data.startswith("check_payment:yookassa:"))
async def cq_check_yookassa_payment(callback: types.CallbackQuery):
    payment_id = callback.data.split(":")[2]
    try:
        payment_info = Payment.find_one(payment_id)
        if payment_info.status == 'succeeded':
            await callback.answer("✅ Оплата прошла успешно! VIP уже должен быть активирован через вебхук.", show_alert=True)
            await cq_vip_subscription(callback)
        else:
            await callback.answer(f"Статус платежа: {payment_info.status}", show_alert=True)
    except Exception as e:
        logging.error(f"Ошибка проверки платежа ЮKassa: {e}")
        await callback.answer("Не удалось проверить платеж.", show_alert=True)

@dp.callback_query(F.data.startswith("check_payment:cryptopay:"))
async def cq_check_cryptopay_payment(callback: types.CallbackQuery):
    invoice_id_str = callback.data.split(":")[2]
    try:
        invoice_id_int = int(invoice_id_str)
    except ValueError:
        await callback.answer("Неверный формат ID счета.", show_alert=True)
        return
    invoices = await cryptopay.get_invoices(invoice_ids=[invoice_id_int])
    if invoices and invoices[0].status == 'paid':
        sub = await get_subscription_by_payment_id(invoice_id_str)
        if sub and sub['status'] != 'paid':
            await activate_vip_for_user(sub['user_id'], sub['duration_days'])
            await mark_subscription_paid(invoice_id_str)
            user = await get_user_by_telegram_id(callback.from_user.id)
            if user:
                await bot.send_message(user['telegram_id'], "✅ Оплата прошла успешно! Ваш VIP-статус активирован.")
                await cq_vip_subscription(callback)
        else:
            await callback.answer("✅ Оплата уже была обработана.", show_alert=True)
    else:
        await callback.answer("Платеж еще не получен.", show_alert=True)

@dp.callback_query(F.data.startswith("add_server"))
async def cq_add_server(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Начинаем...\nДля отмены в любой момент введите /cancel\n\n<b>Шаг 1: Название сервера</b>")
    await state.set_state(AddServer.name)

@dp.message(AddServer.name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("<b>Шаг 2: IP-адрес</b>")
    await state.set_state(AddServer.ip)

@dp.message(AddServer.ip)
async def process_ip(message: types.Message, state: FSMContext):
    await state.update_data(ip=message.text)
    await message.answer("<b>Шаг 3: SSH Порт (обычно 22)</b>")
    await state.set_state(AddServer.port)

@dp.message(AddServer.port)
async def process_port(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Порт должен быть числом.")
        return
    await state.update_data(port=int(message.text))
    await message.answer("<b>Шаг 4: Логин (например, root)</b>")
    await state.set_state(AddServer.login)

@dp.message(AddServer.login)
async def process_login(message: types.Message, state: FSMContext):
    await state.update_data(login=message.text)
    await message.answer("<b>Шаг 5: Пароль (сообщение будет удалено)</b>")
    await state.set_state(AddServer.password)

@dp.message(AddServer.password)
async def process_password(message: types.Message, state: FSMContext):
    data = await state.update_data(password=message.text)
    await message.delete()
    msg = await message.answer("⏳ Проверяю SSH-подключение...")
    is_conn, status = await check_ssh_connection(data['ip'], data['port'], data['login'], data['password'])
    if not is_conn:
        await msg.edit_text(f"❌ <b>Ошибка подключения:</b> {status}.\n\nПроверьте данные и попробуйте добавить сервер заново.")
        await state.clear()
        return
    try:
        user_id = await get_db_user_id(message.from_user.id)
        if user_id:
            await add_server_to_db(user_id, data)
            servers = await get_user_servers(user_id)
            await msg.edit_text(f"✅ Сервер <b>'{data['name']}'</b> успешно добавлен!", reply_markup=servers_list_keyboard(servers))
    except Exception as e:
        logging.error(f"Ошибка сохранения сервера в БД: {e}")
        await msg.edit_text("❌ Произошла ошибка при сохранении сервера в базу данных.")
    await state.clear()

@dp.callback_query(F.data.startswith("manage_server:"))
async def cq_manage_server(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    server_id = int(callback.data.split(":")[1])
    db_user_id = await get_db_user_id(callback.from_user.id)
    if not db_user_id:
        await callback.answer("Ошибка: не удалось определить пользователя.", show_alert=True)
        return
    server = await get_server_details(server_id, db_user_id)
    if not server:
        await callback.answer("Сервер не найден или у вас нет к нему доступа.", show_alert=True)
        return
    await callback.message.edit_text(f"⏳ Получаю информацию о сервере <b>{server['name']}</b>...")
    try:
        password = decrypt_password(server['password_encrypted'])
        success, info = await get_system_info(server['ip'], server['port'], server['login_user'], password)
    except Exception as e:
        logging.error(f"Ошибка проверки статуса сервера {server_id}: {e}")
        success, info = False, {'status': '🔴 Ошибка', 'uptime': 'н/д'}
    created_date = server['created_at'].strftime('%d.%m.%Y %H:%M')
    text = (f"<b>{server['name']}</b>\n\n<b>IP:</b> <code>{server['ip']}:{server['port']}</code>\n<b>Пользователь:</b> <code>{server['login_user']}</code>\n<b>Статус:</b> {info['status']} | <b>Uptime:</b> {info['uptime']}\n<b>Добавлен:</b> {created_date}")
    await callback.message.edit_text(text, reply_markup=server_management_keyboard(server_id))
    await callback.answer()

@dp.callback_query(F.data.startswith("terminal:"))
async def cq_terminal(callback: types.CallbackQuery, state: FSMContext):
    server_id = int(callback.data.split(":")[1])
    await state.set_state(TerminalSession.active)
    await state.update_data(server_id=server_id)
    await callback.message.edit_text("🖥️ <b>Терминал.</b> Введите команду. Для выхода введите /exit")
    await callback.answer()

@dp.message(TerminalSession.active, Command("exit"))
async def terminal_exit(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Выход из терминала.", reply_markup=main_menu_keyboard(message.from_user.id == ADMIN_ID))

@dp.message(TerminalSession.active)
async def terminal_command_handler(message: types.Message, state: FSMContext):
    cmd = message.text
    COMMAND_BLACKLIST = ["reboot", "shutdown", "rm ", "mkfs", "dd ", "fdisk", "mv "]
    if any(b in cmd.lower() for b in COMMAND_BLACKLIST):
        await message.answer(f"❌ <b>Опасная команда!</b> Используйте кнопки в настройках сервера для перезагрузки/выключения.")
        return
    data = await state.get_data()
    sid = data.get("server_id")
    uid = await get_db_user_id(message.from_user.id)
    if not uid:
        await message.answer("Ошибка: не удалось определить пользователя.")
        return
    srv = await get_server_details(sid, uid)
    if not srv:
        await message.answer("Сервер не найден.")
        await state.clear()
        return
    msg = await message.answer(f"⏳ Выполняю: <code>{cmd}</code>")
    try:
        password = decrypt_password(srv['password_encrypted'])
    except Exception:
        await msg.edit_text("❌ Ошибка расшифровки пароля.")
        await state.clear()
        return
    success, output = await execute_command(srv['ip'], srv['port'], srv['login_user'], password, cmd)
    if len(output) > 4000:
        output = output[:4000] + "\n..."
    await msg.edit_text(f"<b>Результат:</b>\n<pre>{output}</pre>")

@dp.callback_query(F.data.startswith("fm_enter:"))
async def cq_fm_enter(callback: types.CallbackQuery, state: FSMContext):
    _, sid, path = callback.data.split(":", 2)
    await state.set_state(FileManagerSession.browsing)
    await state.update_data(server_id=int(sid), current_path=path)
    await show_files(callback, int(sid), path)

@dp.callback_query(F.data.startswith("fm_nav:"))
async def cq_fm_nav(callback: types.CallbackQuery, state: FSMContext):
    _, sid, path = callback.data.split(":", 2)
    await state.update_data(current_path=path)
    await show_files(callback, int(sid), path)

async def show_files(cb_or_msg: types.CallbackQuery | types.Message, server_id: int, path: str):
    is_msg = isinstance(cb_or_msg, types.Message)
    uid = cb_or_msg.from_user.id
    edit_func = cb_or_msg.answer if is_msg else cb_or_msg.message.edit_text
    if not is_msg:
        await cb_or_msg.message.edit_text(f"⏳ Загружаю содержимое: <code>{path}</code>")
    db_uid = await get_db_user_id(uid)
    if not db_uid:
        await edit_func("Ошибка: не удалось определить пользователя.")
        return
    srv = await get_server_details(server_id, db_uid)
    if not srv:
        await edit_func("Ошибка: сервер не найден.")
        return
    try:
        password = decrypt_password(srv['password_encrypted'])
    except Exception:
        await edit_func("❌ Ошибка расшифровки пароля.")
        return
    success, result = await list_directory(srv['ip'], srv['port'], srv['login_user'], password, path)
    if not success:
        await edit_func(f"❌ Ошибка получения списка файлов: <code>{result}</code>", reply_markup=server_management_keyboard(server_id))
        return
    await edit_func(f"<b>Содержимое каталога:</b> <code>{path}</code>", reply_markup=file_manager_keyboard(server_id, path, result))

@dp.callback_query(F.data.startswith("fm_info:"))
async def cq_fm_info(callback: types.CallbackQuery, state: FSMContext):
    _, sid, path = callback.data.split(":", 2)
    msg = await callback.message.answer(f"⏳ Скачиваю файл <code>{os.path.basename(path)}</code>...")
    await callback.answer()
    uid = await get_db_user_id(callback.from_user.id)
    if not uid:
        await msg.edit_text("Ошибка: не удалось определить пользователя.")
        return
    srv = await get_server_details(int(sid), uid)
    if not srv:
        await msg.edit_text("Ошибка: сервер не найден.")
        return
    try:
        password = decrypt_password(srv['password_encrypted'])
    except Exception:
        await msg.edit_text("❌ Ошибка расшифровки пароля.")
        return
    success, result = await download_file(srv['ip'], srv['port'], srv['login_user'], password, path)
    if not success:
        await msg.edit_text(f"❌ Не удалось скачать файл.\n<b>Причина:</b> {result}")
        return
    file_to_send = BufferedInputFile(result, filename=os.path.basename(path))
    await bot.send_document(callback.from_user.id, file_to_send, caption=f"✅ Файл <code>{os.path.basename(path)}</code> успешно скачан.")
    await msg.delete()

@dp.callback_query(F.data.startswith("fm_upload_here:"))
async def cq_fm_upload_here(callback: types.CallbackQuery, state: FSMContext):
    _, sid, path = callback.data.split(":", 2)
    await state.set_state(FileManagerSession.uploading)
    await state.update_data(server_id=int(sid), current_path=path)
    await callback.message.edit_text(f"📤 <b>Загрузка в каталог</b>\n<code>{path}</code>\n\nПросто отправьте документ в этот чат.")
    await callback.answer()

@dp.message(FileManagerSession.uploading, F.document)
async def handle_document_upload(message: types.Message, state: FSMContext):
    if message.document.file_size > 20*1024*1024:
        await message.answer("❌ Размер файла не должен превышать 20 МБ!")
        return
    data = await state.get_data()
    sid, cpath = data.get('server_id'), data.get('current_path')
    if not sid or not cpath:
        await message.answer("Ошибка сессии. Попробуйте снова.")
        await state.clear()
        return
    msg = await message.answer(f"⏳ Скачиваю <code>{message.document.file_name}</code> с серверов Telegram...")
    f_io = await bot.download(message.document)
    f_content = f_io.read()
    await msg.edit_text(f"⏳ Загружаю файл на ваш сервер...")
    uid = await get_db_user_id(message.from_user.id)
    if not uid:
        await msg.edit_text("Ошибка: не удалось определить пользователя.")
        return
    srv = await get_server_details(sid, uid)
    if not srv:
        await msg.edit_text("Ошибка: не удалось найти сервер.")
        await state.clear()
        return
    try:
        pswd = decrypt_password(srv['password_encrypted'])
    except Exception:
        await msg.edit_text("❌ Ошибка расшифровки пароля.")
        await state.clear()
        return
    rpath = os.path.join(cpath, message.document.file_name)
    success, res = await upload_file(srv['ip'], srv['port'], srv['login_user'], pswd, f_content, rpath)
    if not success:
        await msg.edit_text(f"❌ Не удалось загрузить файл.\n<b>Причина:</b> {res}")
        await state.clear()
        return
    await msg.delete()
    await message.answer(f"✅ Файл успешно загружен в <code>{cpath}</code>")
    await state.set_state(FileManagerSession.browsing)
    await show_files(message, sid, cpath)

@dp.callback_query(F.data.startswith("delete_server_confirm:"))
async def cq_delete_server_confirm(callback: types.CallbackQuery):
    sid = int(callback.data.split(":")[1])
    uid = await get_db_user_id(callback.from_user.id)
    if not uid:
        await callback.answer("Ошибка: не удалось определить пользователя.", show_alert=True)
        return
    srv = await get_server_details(sid, uid)
    if not srv:
        await callback.answer("Сервер не найден.", show_alert=True)
        return
    await callback.message.edit_text(f"❓ Вы уверены, что хотите удалить сервер <b>{srv['name']}</b> ({srv['ip']})?\n\n<b>Это действие необратимо!</b>", reply_markup=confirm_delete_keyboard(sid))
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_server_run:"))
async def cq_delete_server_run(callback: types.CallbackQuery, state: FSMContext):
    sid = int(callback.data.split(":")[1])
    uid = await get_db_user_id(callback.from_user.id)
    if not uid:
        await callback.answer("Ошибка: не удалось определить пользователя.", show_alert=True)
        return
    await delete_server_from_db(sid, uid)
    await callback.answer("✅ Сервер удален!", show_alert=True)
    await cq_list_servers(callback, state)

@dp.callback_query(F.data.startswith("server_settings:"))
async def cq_server_settings(callback: types.CallbackQuery):
    sid = int(callback.data.split(":")[1])
    await callback.message.edit_text("⚙️ <b>Настройки сервера</b>", reply_markup=server_settings_keyboard(sid))

@dp.callback_query(F.data.startswith("rename_server:"))
async def cq_rename_server(callback: types.CallbackQuery, state: FSMContext):
    sid = int(callback.data.split(":")[1])
    await state.set_state(RenameServer.new_name)
    await state.update_data(server_id=sid)
    await callback.message.edit_text("✏️ Введите новое имя для сервера:")
    await callback.answer()

@dp.message(RenameServer.new_name)
async def process_rename_server(message: types.Message, state: FSMContext):
    data = await state.get_data()
    sid = data.get("server_id")
    uid = await get_db_user_id(message.from_user.id)
    if not uid:
        await message.answer("Ошибка: не удалось определить пользователя.")
        await state.clear()
        return
    await update_server_name(sid, uid, message.text)
    await message.answer(f"✅ Имя сервера успешно изменено на: <b>{message.text}</b>")
    await state.clear()
    cb_imitation = types.CallbackQuery(id=str(uuid.uuid4()), from_user=message.from_user, chat_instance="", data=f"manage_server:{sid}", message=message)
    await cq_manage_server(cb_imitation, state)

async def handle_power_command(callback: types.CallbackQuery, f, a: str):
    sid = int(callback.data.split(":")[1])
    uid = await get_db_user_id(callback.from_user.id)
    if not uid:
        await callback.answer("Ошибка: не удалось определить пользователя.", show_alert=True)
        return
    srv = await get_server_details(sid, uid)
    if not srv:
        await callback.answer("Сервер не найден.", show_alert=True)
        return
    await callback.message.edit_text(f"⏳ Отправляю команду на {a}...")
    try:
        pswd = decrypt_password(srv['password_encrypted'])
    except Exception:
        await callback.message.edit_text(f"❌ Ошибка расшифровки пароля.", reply_markup=server_settings_keyboard(sid))
        return
    success, msg = await f(srv['ip'], srv['port'], srv['login_user'], pswd)
    await callback.message.edit_text(f"✅ {msg}" if success else f"❌ {msg}", reply_markup=server_settings_keyboard(sid))

@dp.callback_query(F.data.startswith("reboot_server_confirm:"))
async def reboot_confirm(callback: types.CallbackQuery):
    await callback.message.edit_text("❓ Вы уверены, что хотите <b>перезагрузить</b> сервер?", reply_markup=confirm_action_keyboard("Reboot", int(callback.data.split(":")[1])))

@dp.callback_query(F.data.startswith("shutdown_server_confirm:"))
async def shutdown_confirm(callback: types.CallbackQuery):
    await callback.message.edit_text("❓ Вы уверены, что хотите <b>выключить</b> сервер?", reply_markup=confirm_action_keyboard("Shutdown", int(callback.data.split(":")[1])))

@dp.callback_query(F.data.startswith("reboot_server_run:"))
async def cq_reboot_run(callback: types.CallbackQuery):
    await handle_power_command(callback, reboot_server, "перезагрузку")

@dp.callback_query(F.data.startswith("shutdown_server_run:"))
async def cq_shutdown_run(callback: types.CallbackQuery):
    await handle_power_command(callback, shutdown_server, "выключение")

@dp.callback_query(F.data.startswith("change_password:"))
async def cq_change_password(callback: types.CallbackQuery, state: FSMContext):
    server_id = int(callback.data.split(":")[1])
    await state.set_state(ChangePassword.waiting_for_password)
    await state.update_data(server_id=server_id)
    await callback.message.edit_text(
        "🔑 <b>Смена пароля</b>\n\n"
        "Введите новый пароль для сервера. Ваше сообщение будет автоматически удалено для безопасности.",
        reply_markup=cancel_password_change_keyboard(server_id)
    )
    await callback.answer()

@dp.message(ChangePassword.waiting_for_password, F.text)
async def process_change_password(message: types.Message, state: FSMContext):
    new_password = message.text
    data = await state.get_data()
    server_id = data.get("server_id")

    await message.delete()

    uid = await get_db_user_id(message.from_user.id)
    if not uid:
        await message.answer("Ошибка: не удалось определить пользователя.")
        await state.clear()
        return

    srv = await get_server_details(server_id, uid)
    if not srv:
        await message.answer("Ошибка: не удалось найти сервер.")
        await state.clear()
        return

    msg = await message.answer("⏳ Проверяю подключение с новым паролем...")

    is_conn, conn_status = await check_ssh_connection(srv['ip'], srv['port'], srv['login_user'], new_password)

    if not is_conn:
        await msg.edit_text(f"❌ <b>Ошибка подключения с новым паролем:</b>\n<code>{conn_status}</code>\n\nПароль не был изменен. Пожалуйста, проверьте пароль и попробуйте снова.", reply_markup=cancel_password_change_keyboard(server_id))
        await state.clear()
        return

    try:
        new_encrypted_password = encrypt_password(new_password)
        await update_server_password(server_id, uid, new_encrypted_password)
        await msg.edit_text("✅ Пароль успешно изменен!", reply_markup=get_back_to_manage_keyboard(server_id))
    except Exception as e:
        logging.error(f"Ошибка обновления пароля в БД для сервера {server_id}: {e}")
        await msg.edit_text("❌ Произошла внутренняя ошибка при сохранении нового пароля.", reply_markup=get_back_to_manage_keyboard(server_id))

    await state.clear()

@dp.callback_query(F.data.startswith("server_info:"))
async def cq_server_info(callback: types.CallbackQuery):
    sid = int(callback.data.split(":")[1])
    uid = await get_db_user_id(callback.from_user.id)
    if not uid:
        await callback.answer("Ошибка: не удалось определить пользователя.", show_alert=True)
        return
    srv = await get_server_details(sid, uid)
    if not srv:
        await callback.answer("Сервер не найден.", show_alert=True)
        return
    await callback.message.edit_text(f"⏳ Получаю подробную информацию о <b>{srv['name']}</b>...")
    try:
        pswd = decrypt_password(srv['password_encrypted'])
        success, info = await get_system_info(srv['ip'], srv['port'], srv['login_user'], pswd)
    except Exception as e:
        logging.error(f"Ошибка получения инфо о сервере: {e}")
        success, info = False, {}
    if not success:
        text = "❌ Не удалось получить подробную информацию о сервере."
    else:
        text = f"<b>🖥️ Подробная информация</b>\n\n<b>Имя хоста:</b> <code>{info.get('hostname','н/д')}</code>\n<b>Операционная система:</b> {info.get('os','н/д')}\n<b>Версия ядра:</b> <code>{info.get('kernel','н/д')}</code>"
    await callback.message.edit_text(text, reply_markup=get_back_to_manage_keyboard(sid))
    await callback.answer()

@dp.callback_query(F.data.startswith("server_load:"))
async def cq_server_load(callback: types.CallbackQuery):
    sid = int(callback.data.split(":")[1])
    uid = await get_db_user_id(callback.from_user.id)
    if not uid:
        await callback.answer("Ошибка: не удалось определить пользователя.", show_alert=True)
        return
    srv = await get_server_details(sid, uid)
    if not srv:
        await callback.answer("Сервер не найден.", show_alert=True)
        return
    await callback.message.edit_text(f"⏳ Получаю данные о нагрузке на <b>{srv['name']}</b>...")
    try:
        pswd = decrypt_password(srv['password_encrypted'])
        success, info = await get_system_load(srv['ip'], srv['port'], srv['login_user'], pswd)
    except Exception as e:
        logging.error(f"Ошибка получения нагрузки: {e}")
        success, info = False, "Критическая ошибка"
    if not success:
        text = f"❌ Не удалось получить данные о нагрузке.\n<b>Причина:</b> <code>{info}</code>"
    else:
        text = f"<b>📊 Нагрузка на систему</b>\n\n<b>CPU:</b> {info['cpu']}\n<b>RAM:</b> {info['ram']}\n<b>Диск (/):</b> {info['disk']}"
    await callback.message.edit_text(text, reply_markup=get_load_keyboard(sid))
    await callback.answer()

@dp.callback_query(F.data == "support")
async def cq_support(callback: types.CallbackQuery):
    support_text = await get_setting('support_info', default=f"🆘 Для связи с поддержкой пишите: {SUPPORT_USERNAME}")
    await callback.answer(support_text, show_alert=True)

@dp.callback_query(F.data == "settings")
async def cq_settings(callback: types.CallbackQuery):
    await callback.answer("Настройки пока в разработке.", show_alert=True)


# ==========================================================
# ===                 АДМИН-ПАНЕЛЬ                       ===
# ==========================================================

@dp.callback_query(F.data == "admin_panel")
async def cq_admin_panel(callback: types.CallbackQuery):
    await callback.message.edit_text("🛠️ <b>Админ-панель</b>", reply_markup=admin_main_keyboard())

# --- Управление пользователями ---
@dp.callback_query(F.data == "admin_users_menu")
async def cq_admin_users_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("Меню управления пользователями.", reply_markup=admin_users_keyboard())

@dp.callback_query(F.data == "admin_find_user")
async def cq_admin_find_user(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminSearchUser.by_id)
    await callback.message.edit_text("Введите Telegram ID пользователя для поиска.")

@dp.message(AdminSearchUser.by_id)
async def admin_process_user_search(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("ID должен состоять только из цифр. Попробуйте еще раз.")
        return
    await state.clear()
    await show_found_user_info(message, int(message.text))

@dp.callback_query(F.data.startswith("admin_find_user_return:"))
async def cq_admin_find_user_return(callback: types.CallbackQuery, state: FSMContext):
    if await state.get_state() is not None:
        await state.clear()
    user_tg_id = int(callback.data.split(":")[1])
    await show_found_user_info(callback, user_tg_id)

@dp.callback_query(F.data.startswith("admin_give_vip:"))
async def cq_admin_give_vip(callback: types.CallbackQuery):
    user_tg_id = int(callback.data.split(":")[1])
    success = await admin_set_vip_status(user_tg_id, status=True, duration_days=30)
    if success:
        await callback.answer("✅ VIP-статус на 30 дней успешно выдан!", show_alert=True)
        try:
            await bot.send_message(user_tg_id, "🎉 Поздравляем! Администратор выдал вам VIP-статус на 30 дней.")
        except Exception as e:
            logging.warning(f"Не удалось отправить уведомление пользователю {user_tg_id}: {e}")
    else:
        await callback.answer("❌ Не удалось выдать VIP. Пользователь не найден.", show_alert=True)
    await show_found_user_info(callback, user_tg_id)

@dp.callback_query(F.data.startswith("admin_revoke_vip:"))
async def cq_admin_revoke_vip(callback: types.CallbackQuery):
    user_tg_id = int(callback.data.split(":")[1])
    await admin_set_vip_status(user_tg_id, status=False)
    await callback.answer("🗑 VIP-статус у пользователя отозван.", show_alert=True)
    try:
        await bot.send_message(user_tg_id, "ℹ️ Ваш VIP-статус был отозван администратором.")
    except Exception as e:
        logging.warning(f"Не удалось отправить уведомление пользователю {user_tg_id}: {e}")
    await show_found_user_info(callback, user_tg_id)

@dp.callback_query(F.data.startswith("admin_message_user:"))
async def cq_admin_message_user(callback: types.CallbackQuery, state: FSMContext):
    user_tg_id = int(callback.data.split(":")[1])
    await state.set_state(AdminMessageUser.waiting_for_message)
    await state.update_data(target_user_id=user_tg_id)
    await callback.message.edit_text(
        f"✍️ Отправка сообщения пользователю <code>{user_tg_id}</code>.\n\n"
        "Просто отправьте в чат всё, что хотите ему переслать (текст, фото, документ и т.д.).",
        reply_markup=admin_cancel_message_keyboard(user_tg_id)
    )

@dp.message(AdminMessageUser.waiting_for_message, F.content_type.in_({'text', 'photo', 'document', 'video', 'audio'}))
async def process_admin_message_to_user(message: types.Message, state: FSMContext):
    data = await state.get_data()
    target_user_id = data.get("target_user_id")
    await state.clear()

    if not target_user_id:
        await message.answer("Произошла ошибка сессии, попробуйте снова.")
        callback_imitation = types.CallbackQuery(id=str(uuid.uuid4()), from_user=message.from_user, chat_instance=message.chat.id, data="admin_panel")
        await cq_admin_panel(callback_imitation)
        return

    try:
        await bot.copy_message(
            chat_id=target_user_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id
        )
        await message.answer("✅ Сообщение успешно отправлено пользователю.")
        if message.text:
             await bot.send_message(target_user_id, "<i>👆 Это сообщение было отправлено вам от администратора.</i>")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить сообщение. Ошибка: {e}")
        logging.error(f"Ошибка отправки сообщения пользователю {target_user_id}: {e}")

    # Имитируем callback, чтобы вернуться в карточку пользователя
    callback_imitation = types.CallbackQuery(id=str(uuid.uuid4()), from_user=message.from_user, chat_instance=message.chat.id, data=f"admin_find_user_return:{target_user_id}", message=message)
    await cq_admin_find_user_return(callback_imitation, state)


@dp.callback_query(F.data.startswith("admin_delete_server_confirm:"))
async def cq_admin_delete_server_confirm(callback: types.CallbackQuery):
    _, server_id, user_tg_id = callback.data.split(":")
    await callback.message.edit_text(f"❓ Вы уверены, что хотите удалить этот сервер (ID: {server_id})?",
                                     reply_markup=admin_confirm_delete_server_keyboard(int(server_id), int(user_tg_id)))

@dp.callback_query(F.data.startswith("admin_delete_server_run:"))
async def cq_admin_delete_server_run(callback: types.CallbackQuery):
    _, server_id_str, user_tg_id_str = callback.data.split(":")
    await admin_delete_server(int(server_id_str))
    await callback.answer("✅ Сервер удален!", show_alert=True)
    await show_found_user_info(callback, int(user_tg_id_str))

# --- Управление серверами (общий раздел) ---
@dp.callback_query(F.data == "admin_servers_menu")
async def cq_admin_servers_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("Меню управления серверами.", reply_markup=admin_servers_menu_keyboard())

@dp.callback_query(F.data == "admin_find_server_by_id")
async def cq_admin_find_server(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminSearchServer.by_id)
    await callback.message.edit_text("Введите ID сервера для поиска.")

@dp.message(AdminSearchServer.by_id)
async def admin_process_server_search(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("ID должен состоять только из цифр. Попробуйте еще раз.")
        return
    await state.clear()
    await show_admin_found_server_info(message, int(message.text))

@dp.callback_query(F.data.startswith("admin_server_delete_confirm:"))
async def cq_admin_server_delete_confirm(callback: types.CallbackQuery):
    server_id = int(callback.data.split(":")[1])
    await callback.message.edit_text(
        f"❓ Вы уверены, что хотите <b>безвозвратно удалить</b> этот сервер (ID: {server_id}) из базы данных?",
        reply_markup=admin_server_confirm_delete_keyboard(server_id)
    )

@dp.callback_query(F.data.startswith("admin_server_delete_run:"))
async def cq_admin_server_delete_run(callback: types.CallbackQuery):
    server_id = int(callback.data.split(":")[1])
    await admin_delete_server(server_id)
    await callback.answer("✅ Сервер успешно удален!", show_alert=True)
    await callback.message.edit_text("Меню управления серверами.", reply_markup=admin_servers_menu_keyboard())


# --- VIP-управление ---
@dp.callback_query(F.data == "admin_vip_menu")
async def cq_admin_vip_menu(callback: types.CallbackQuery):
    await callback.message.edit_text("💎 <b>Меню управления VIP-статусами</b>", reply_markup=admin_vip_menu_keyboard())

@dp.callback_query(F.data.startswith("admin_list_vips:"))
async def cq_admin_list_vips(callback: types.CallbackQuery):
    page = int(callback.data.split(":")[1])
    per_page = 5

    vips, total_count = await admin_get_all_vips_paginated(page=page, per_page=per_page)

    if total_count == 0:
        await callback.message.edit_text("Активных VIP-пользователей нет.", reply_markup=admin_vip_menu_keyboard())
        return

    total_pages = math.ceil(total_count / per_page)

    text = "<b>📋 Список активных VIP-пользователей:</b>\n\n"
    for vip in vips:
        username = f"@{vip['username']}" if vip['username'] else "N/A"
        expires_date = vip['vip_expires'].strftime('%d.%m.%Y')
        text += f"▪️ <a href=\"tg://user?id={vip['telegram_id']}\">{vip['telegram_id']}</a> ({username}) - до {expires_date}\n"

    await callback.message.edit_text(
        text,
        reply_markup=admin_vips_list_keyboard(current_page=page, total_pages=total_pages)
    )

# --- Экспорт данных ---
@dp.callback_query(F.data == "admin_export_data")
async def cq_admin_export_data(callback: types.CallbackQuery):
    await callback.answer("⏳ Начинаю экспорт. Это может занять некоторое время...")

    try:
        users_data = await admin_get_all_users_for_export()
        if users_data:
            users_output = io.StringIO()
            users_writer = csv.writer(users_output)
            users_headers = users_data[0].keys()
            users_writer.writerow(users_headers)
            for user in users_data:
                users_writer.writerow(user.values())
            users_output.seek(0)
            users_file = BufferedInputFile(users_output.read().encode('utf-8'), filename=f"users_{datetime.now().strftime('%Y%m%d')}.csv")
            await bot.send_document(ADMIN_ID, users_file, caption="Backup таблицы `users`")
    except Exception as e:
        logging.error(f"Ошибка экспорта пользователей: {e}")
        await callback.message.answer(f"❌ Произошла ошибка при экспорте пользователей: {e}")

    try:
        servers_data = await admin_get_all_servers_for_export()
        if servers_data:
            servers_output = io.StringIO()
            servers_writer = csv.writer(servers_output)
            servers_headers = servers_data[0].keys()
            servers_writer.writerow(servers_headers)
            for server in servers_data:
                server_values = dict(server)
                server_values['password_encrypted'] = '***ENCRYPTED***'
                servers_writer.writerow(server_values.values())
            servers_output.seek(0)
            servers_file = BufferedInputFile(servers_output.read().encode('utf-8'), filename=f"servers_{datetime.now().strftime('%Y%m%d')}.csv")
            await bot.send_document(ADMIN_ID, servers_file, caption="Backup таблицы `servers`")
    except Exception as e:
        logging.error(f"Ошибка экспорта серверов: {e}")
        await callback.message.answer(f"❌ Произошла ошибка при экспорте серверов: {e}")

# --- Управление контентом ---
@dp.callback_query(F.data == "admin_content_menu")
async def cq_admin_content_menu(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("📝 <b>Управление контентом</b>\n\nЗдесь можно изменять тексты, которые видит пользователь.", reply_markup=admin_content_menu_keyboard())

@dp.callback_query(F.data.startswith("admin_edit_content:"))
async def cq_admin_edit_content(callback: types.CallbackQuery, state: FSMContext):
    content_key = callback.data.split(":")[1]

    content_map = {
        'welcome_message': {
            'title': 'приветственного сообщения',
            'default': '🚀 <b>Добро пожаловать в KDS Server Panel!</b>\n\nЭтот бот поможет вам управлять вашими серверами.'
        },
        'vip_info': {
            'title': 'информации о VIP',
            'default': '👑 <b>VIP-Подписка</b>\n\nVIP-статус открывает доступ к расширенным возможностям и снимает все ограничения.'
        },
        'support_info': {
            'title': 'текста поддержки',
            'default': f'🆘 Для связи с поддержкой пишите: {SUPPORT_USERNAME}'
        }
    }

    content_item = content_map.get(content_key)
    if not content_item:
        await callback.answer("Неизвестный ключ контента.", show_alert=True)
        return

    current_text = await get_setting(content_key, default=content_item['default'])

    await state.set_state(AdminEditContent.waiting_for_text)
    await state.update_data(content_key=content_key, content_title=content_item['title'])

    await callback.message.edit_text(
        f"<b>Редактирование {content_item['title']}</b>\n\n"
        f"Текущий текст:\n"
        "------------------------------------\n"
        f"{current_text}\n"
        "------------------------------------\n\n"
        "Отправьте новый текст.",
        reply_markup=admin_cancel_content_edit_keyboard()
    )

@dp.message(AdminEditContent.waiting_for_text, F.text)
async def process_new_content(message: types.Message, state: FSMContext):
    data = await state.get_data()
    content_key = data.get('content_key')
    content_title = data.get('content_title')

    if not content_key:
        await state.clear()
        await message.answer("Произошла ошибка сессии, попробуйте снова.", reply_markup=admin_content_menu_keyboard())
        return

    await update_setting(content_key, message.html_text)
    await state.clear()
    await message.answer(f"✅ Текст для <b>{content_title}</b> успешно обновлен!", reply_markup=admin_content_menu_keyboard())


# --- Заглушки для других разделов админки ---
@dp.callback_query(F.data.in_({"dev_placeholder", "admin_view_server"}))
async def cq_admin_dev_placeholder(callback: types.CallbackQuery):
    await callback.answer("Этот раздел находится в разработке.", show_alert=True)


# --- Рассылка ---
@dp.callback_query(F.data == "admin_broadcast")
async def cq_admin_broadcast(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(Broadcast.message)
    await callback.message.edit_text("Введите сообщение для рассылки (текст, фото, видео). Для отмены: /cancel")
    await callback.answer()

@dp.message(Broadcast.message)
async def broadcast_message_handler(message: types.Message, state: FSMContext):
    await state.update_data(broadcast_message_id=message.message_id, broadcast_chat_id=message.chat.id)
    total_users = await get_total_users_count()
    await state.set_state(Broadcast.confirmation)
    await message.answer(f"Вы уверены, что хотите отправить это сообщение? Его получат примерно {total_users} пользователей.", reply_markup=confirm_broadcast_keyboard())

@dp.callback_query(F.data == "start_broadcast", Broadcast.confirmation)
async def broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    message_id, chat_id = data.get("broadcast_message_id"), data.get("broadcast_chat_id")
    await state.clear()
    await callback.message.edit_text("✅ Рассылка начата...")
    users = await get_all_users_ids()
    success, error = 0, 0
    for user in users:
        try:
            await bot.copy_message(chat_id=user['telegram_id'], from_chat_id=chat_id, message_id=message_id)
            success += 1
        except Exception:
            error += 1
        await asyncio.sleep(0.1)
    await callback.message.answer(f"🏁 Рассылка завершена!\n\n✅ Успешно отправлено: {success}\n❌ Ошибок: {error}", reply_markup=admin_main_keyboard())


# --- Основная функция ---
# --- Основная функция ---
async def main():
    """Запуск бота в режиме polling (без вебхуков и веб-сервера)."""
    await create_db_pool()
    if not db_pool:
        logging.critical("Не удалось подключиться к базе данных. Запуск отменен.")
        return

    # Удаляем старый вебхук, если он был установлен
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("Старый вебхук удалён")
    except Exception as e:
        logging.warning(f"Не удалось удалить вебхук: {e}")

    # Уведомляем админа о запуске
    try:
        total_users = await get_total_users_count()
        total_servers = await get_total_servers_count()
        admin_rec = await get_user_by_telegram_id(ADMIN_ID)
        if admin_rec:
            await bot.send_message(
                ADMIN_ID,
                await get_status_message_text(admin_rec, total_users, total_servers)
            )
    except Exception as e:
        logging.warning(f"Не удалось отправить стартовое сообщение: {e}")

    logging.info("Бот запущен в режиме polling")

    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()
        await bot.session.close()
        logging.info("Бот остановлен.")


if __name__ == "__main__":
    asyncio.run(main())
