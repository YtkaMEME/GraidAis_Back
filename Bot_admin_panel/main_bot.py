import json
import asyncio
import subprocess
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram import Router
from werkzeug.security import generate_password_hash, check_password_hash
from aiogram.fsm.state import State, StatesGroup

from GraidAis_Back.Data_base.DataBase import DataBase
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, ReplyKeyboardRemove, KeyboardButton, ReplyKeyboardMarkup
from aiogram import F

from GraidAis_Back.config import API_TOKEN


class MainState(StatesGroup):
    change_users_list = State()
    add_users_list = State()
    put_away_users_list = State()

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
router = Router()


@dp.message(Command("get_id"))
async def get_id(message: Message):
    user_id = message.from_user.id
    await message.answer(f"Ваш ID: {user_id}")


@router.message(Command("register_user"))
async def admin_panel(message: Message, state: FSMContext):
    await state.clear()

    kb = [
        [
            KeyboardButton(text="Удалить", reply_markup=ReplyKeyboardRemove()),
            KeyboardButton(text="Добавить", reply_markup=ReplyKeyboardRemove()),
        ],
    ]
    keyboard = ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder="Будем что-то менять?",
    )

    with open('admins.json', 'r', encoding='utf-8') as file:
        admins_users = json.load(file)

    user_id = message.from_user.id
    if user_id not in admins_users:
        await message.answer("Вы не администратор, вам сюда нельзя!!!")
        return

    await state.set_state(MainState.change_users_list)

    await message.answer("Выберите действие которое вы хотите произвести", reply_markup=keyboard)


@router.message(MainState.change_users_list, F.text == "Удалить")
@router.message(MainState.change_users_list, F.text == "Добавить")
async def users_list(message: Message, state: FSMContext):
    if message.text == "Удалить":
        await state.set_state(MainState.put_away_users_list)
        await message.answer("Отправьте login пользователя, которого необходимо удалить", reply_markup=ReplyKeyboardRemove())
    elif message.text == "Добавить":
        await state.set_state(MainState.add_users_list)
        await message.answer("Отправьте login и пароль пользователя, которого необходимо добавить", reply_markup=ReplyKeyboardRemove())


@router.message(MainState.put_away_users_list)
@router.message(MainState.add_users_list)
async def add_put_users_list(message: Message, state: FSMContext):
    user_login = message.text.split(" ")[0]
    user_password = message.text.split(" ")[1]

    db = DataBase("../grade.db")
    current_state = await state.get_state()

    if current_state == MainState.put_away_users_list:
        user = db.get_user(user_login)

        if user:
            db.delete_user(user_login)
            await message.answer(f"Пользователь {user_login} был удален.")
        else:
            await message.answer(f"Пользователь {user_login} не найден в базе данных.")

    elif current_state == MainState.add_users_list:
        user = db.get_user(user_login)
        if user:
            await message.answer(f"Пользователь {user_login} уже существует.")
        else:
            hashed_password = generate_password_hash(user_password)
            db.insert_user(user_login, hashed_password)
            await message.answer(f"Пользователь {user_login} добавлен и теперь имеет доступ к сайту.")
    await state.clear()

async def run_git_pull():
    try:
        process = await asyncio.create_subprocess_shell(
            "/bin/bash /updating_repository.sh",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            return stdout.decode('utf-8')
        else:
            return f"Ошибка выполнения: {stderr.decode('utf-8')}"
    except Exception as e:
        return f"Ошибка: {str(e)}"


# Команда для запуска SSH-скрипта через Telegram
@dp.message(Command("git_pull"))
async def git_pull_command(message: Message):
    user_id = message.from_user.id
    with open('admins.json', 'r', encoding='utf-8') as file:
        admins_users = json.load(file)

    if user_id not in admins_users:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    # Запускаем SSH-скрипт
    await message.answer("Запускаю git pull...")
    result = await run_git_pull()

    # Отправляем результат пользователю
    await message.answer(result)


async def main():
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
