import json
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram import Router
from werkzeug.security import generate_password_hash, check_password_hash
from aiogram.fsm.state import State, StatesGroup
from GraidAis_Back.Data_base.Data_Base import Data_Base
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, ReplyKeyboardRemove, KeyboardButton, ReplyKeyboardMarkup
from aiogram import F

class MainState(StatesGroup):
    change_users_list = State()
    add_users_list = State()
    put_away_users_list = State()

def load_allowed_users():
    with open('allowed_users.json', 'r') as file:
        data = json.load(file)
        return data.get("allowed_users", [])

allowed_users = load_allowed_users()

API_TOKEN = ''

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
router = Router()

def is_allowed(user_id):
    return user_id in allowed_users

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
        await message.answer("Отправьте login пользователя которого необходимо удалить", reply_markup=ReplyKeyboardRemove())
    elif message.text == "Добавить":
        await state.set_state(MainState.add_users_list)
        await message.answer("Отправьте login и пароль пользователя которого необходимо добавить", reply_markup=ReplyKeyboardRemove())


@router.message(MainState.put_away_users_list)
@router.message(MainState.add_users_list)
async def add_put_users_list(message: Message, state: FSMContext):
    user_login = message.text.split(" ")[0]
    user_password = message.text.split(" ")[1]

    db = Data_Base("../grade.db")

    # current_state = await state.get_state()
    # if current_state == MainState.put_away_users_list:
    #     if user_id in user_list:
    #         user_list.remove(user_id)
    # elif current_state == MainState.add_users_list:
    #     user_list.append(user_id)

    hashed_password = generate_password_hash(user_password)

    db.insert_user(user_login, hashed_password)

    await message.answer(f"Пользователь добавлен и теперь имеет доступ к сайту")

    await state.clear()

async def main():
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
