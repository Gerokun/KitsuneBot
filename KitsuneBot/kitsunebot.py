import random
import string
import os
import re
from settings import boneses_ref, boneses_link, count_days_mess, count_bonus_timed, count_days_mess_out, admins, works
from settings import procent_my, procent_ref, procent_my2, check_sum_if2, procent_my3, check_sum_if3, procent_my4, check_sum_if4

import telebot
import sqlite3
import threading
import time
from time import sleep

from telebot import types
from telebot import apihelper
from datetime import datetime, timedelta

# Баги:
# Нужно сделать функцию на создании даты пользователя чтоб уменьшить кол-во кода

# Токен телеграм бота. Важно сохранить его в секрете
apihelper.proxy = {'http':'http://46.8.31.182:22'}
bot = telebot.TeleBot('6722805471:AAGU74P1PXwKQ4Cfo3LL_hXF00LdQ4Jyaz0') # Токен

# Нужная штука, чтобы не было багов
user_states = {} # сохраняет user states а именно для проверки что пользователь ожидает ответа от бота. так как без него бот может отвечать дважды
admin_states = {}
user_msg = []
user_not_days = {}
polls = {}

current_user_reg = {}

# Опросник и голосование! --------------------------------------------------------------------------------------------
# Словарь для хранения голосов пользователей
votes = {}
# Словарь для хранения текущих опросов
current_polls = {}
# Словарь для хранения результатов опросов, включая название опроса
poll_results = {}

# Установление соединения с базой данных SQLite
_conn = sqlite3.connect('Polls.kts', check_same_thread=False)
_cur = _conn.cursor()

# Создание таблиц, если они не существуют
_cur.execute('''CREATE TABLE IF NOT EXISTS polls (
                    poll_id TEXT PRIMARY KEY,
                    question TEXT
                 )''')

_cur.execute('''CREATE TABLE IF NOT EXISTS options (
                    poll_id TEXT,
                    option TEXT,
                    votes INTEGER,
                    FOREIGN KEY (poll_id) REFERENCES polls (poll_id)
                 )''')

_cur.execute('''CREATE TABLE IF NOT EXISTS user_votes (
                    user_id INTEGER,
                    poll_id TEXT,
                    vote TEXT,
                    PRIMARY KEY (user_id, poll_id),
                    FOREIGN KEY (poll_id) REFERENCES polls (poll_id)
                 )''')

_conn.commit()
# --------------------------------------------------------------------------------------------------------------------

# Очистка кеша
for filename in os.listdir('./Temp'):
    file_path = os.path.join('./Temp', filename)
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
    except Exception as e:
        print(f'Ошибка при удалении файла {file_path}. {e}')

# Действия при команде "/start"
@bot.message_handler(commands=['start'])
def start(message):
    #Исправления дублирования
    user_id = message.from_user.id
    if user_id in user_states and user_states[user_id]:
        return
    user_states[user_id] = True
    msgs_id = []
    msgs_id.append(user_id)
    msgs_id.append(message)

    markupr = types.InlineKeyboardMarkup()
    btn_reg = types.InlineKeyboardButton('Зарегистрироваться', callback_data='reg')
    markupr.add(btn_reg)

    markup = types.InlineKeyboardMarkup()
    btn_mn = types.InlineKeyboardButton('Меню', callback_data='menu')
    markup.add(btn_mn)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Создание таблицы
    cur.execute(
        'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(25), number varchar(12), sex varchar(3), date_born varchar(10), ref_link varchar(7), chat_id varchar(10), user_id varchar(33), bonus INT NOT NULL, date_register varchar(10), my_referer varchar(33), count_checks INT NOT NULL, sum_checks INT NOT NULL, procent_my REAL NOT NULL, procent_ref REAL NOT NULL)')
    cur.execute(
        'CREATE TABLE IF NOT EXISTS bonuses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INT NOT NULL, number INT NOT NULL, bonuses INT NOT NULL, count_day INT NOT NULL)')
    conn.commit()

    cur.execute('SELECT * FROM users')
    users = cur.fetchall()
    for el in users:
        if str(user_id) == str(el[7]):
            msg = bot.send_message(message.chat.id, 'Здравствуйте, я телеграм-бот Китсуне!\nДля работы с нами, воспользуйтесь меню.')
            # Исправления дублирования
            user_states[user_id] = False
            msgs_id.append(msg)
            cur.close()
            conn.close()
            user_msg.append(msgs_id)
            menu(message)
            return
    cur.close()
    conn.close()

    msg = bot.send_message(message.chat.id, 'Здравствуйте, я телеграм-бот Китсуне!\nДля работы с нами вам нужно зарегистрироваться.')
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    # Исправления дублирования
    user_states[user_id] = False
    register_user(message)

# Команда /registering
@bot.message_handler(commands=['registering'])
def register_user(message):
    # Исправления дублирования
    user_id = message.from_user.id
    if user_id in user_states and user_states[user_id]:
        return
    user_states[user_id] = True
    current_user_reg[user_id] = True
    msgs_id = []
    msgs_id.append(user_id)
    msgs_id.append(message)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Создание таблицы
    cur.execute(
        'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(25), number varchar(12), sex varchar(3), date_born varchar(10), ref_link varchar(7), chat_id varchar(10), user_id varchar(33), bonus INT NOT NULL, date_register varchar(10), my_referer varchar(33), count_checks INT NOT NULL, sum_checks INT NOT NULL, procent_my REAL NOT NULL, procent_ref REAL NOT NULL)')
    cur.execute(
        'CREATE TABLE IF NOT EXISTS bonuses (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INT NOT NULL, number INT NOT NULL, bonuses INT NOT NULL, count_day INT NOT NULL)')
    conn.commit()
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()
    for el in users:
        if str(user_id) == str(el[7]):
            msg = bot.send_message(message.chat.id, 'Вы уже зарегистрированы!')
            # Исправления дублирования
            user_states[user_id] = False
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            cur.close()
            conn.close()
            # Здесь должно запускаться меню
            menu(message)
            return

    cur.close()
    conn.close()
    #Реферальная система
    markup = types.InlineKeyboardMarkup()
    btn1 = types.InlineKeyboardButton(text='Да', callback_data='b_reg_yes')
    btn2 = types.InlineKeyboardButton(text='Нет', callback_data='b_reg_no')
    markup.add(btn1)
    markup.add(btn2)
    msg = bot.send_message(message.chat.id, 'Вас пригласил *друг*?\nУ вас есть *его* *реферальный* *номер*? ', parse_mode="Markdown", reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    user_states[user_id] = False
    # bot.register_next_step_handler(message, ref_link_accept)

# Команда /menu
@bot.message_handler(commands=['menu'])
def menu(message):
    # Исправления дублирования
    user_id = message.from_user.id
    if user_id in user_states and user_states[user_id]:
        return
    user_states[user_id] = True

    markup = types.InlineKeyboardMarkup()
    btn_bon = types.InlineKeyboardButton('🎁Бонусы🎁', callback_data='user_bonus')
    btn_cont = types.InlineKeyboardButton('📞Контакты📞', callback_data='contacts')
    btn_inst = types.InlineKeyboardButton('📸Инстаграм📸',
                                          url='https://www.instagram.com/kitsune.shemonaiha?igsh=NmpyZW9rcnVhMTUx')
    btn_menu = types.InlineKeyboardButton('📱Меню📱', callback_data='kitchen_menu')
    btn_ref = types.InlineKeyboardButton('💰Мои рефералы💰', callback_data='my_ref')
    btn_ans = types.InlineKeyboardButton('✏️Оставить отзыв✏️', callback_data='send_request')
    btn_edit_user = types.InlineKeyboardButton('📋Изменить профиль📋', callback_data='edit_profile')
    btn_queations = types.InlineKeyboardButton('❓Часто задаваемые вопросы❓', callback_data='queations')
    markup.add(btn_bon)
    markup.add(btn_cont, btn_inst)
    markup.add(btn_menu)
    markup.add(btn_ref)
    markup.add(btn_ans)
    markup.add(btn_edit_user)
    markup.add(btn_queations)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Создание таблицы
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()

    isnumber = False
    for el in users:
        if str(user_id) == str(el[7]):
            isnumber = True

    if isnumber == False:
        bot.send_message(message.chat.id, 'Вы не зарегистрированы(')
    else:
        bot.send_message(message.chat.id, 'Выберете интересующий вас раздел', reply_markup=markup)
    cur.close()
    conn.close()

    # Исправления дублирования
    user_id = message.from_user.id
    user_states[user_id] = False

# Подтверждение реф
def ref_link_accept(message, call):
    usid = call.from_user.id
    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if call.data == 'b_reg_yes':
        markup = types.ReplyKeyboardRemove()
        msg = bot.send_message(message.chat.id, 'Введите реферальный номер:\nДля отмены введите: Отмена', reply_markup=markup)
        msgs_id.append(msg)
        bot.register_next_step_handler(message, ref_link_activate)
    elif call.data == 'b_reg_no':
        markup = types.ReplyKeyboardRemove()
        # Ввод имени
        msg = bot.send_message(message.chat.id, 'Тогда предлагаю познакомиться.\nВведите ваше имя', reply_markup=markup)
        msgs_id.append(msg)
        bot.register_next_step_handler(message, user_name)
    else:
        markup = types.InlineKeyboardMarkup()
        btn1 = types.InlineKeyboardButton(text='Да', callback_data='b_reg_yes')
        btn2 = types.InlineKeyboardButton(text='Нет', callback_data='b_reg_no')
        markup.add(btn1)
        markup.add(btn2)

        msg = bot.send_message(message.chat.id, 'Воспользуйтесь кнопками или введите "Да" или "Нет"')
        msgs_id.append(msg)
        msg = bot.send_message(message.chat.id, 'Вас пригласил *друг*?\nУ вас есть *его* *реферальный* *номер*? ',
                               parse_mode="Markdown", reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
    user_msg.append(msgs_id)

# Активация реферального номера
def ref_link_activate(message, i = 1):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.content_type == 'text':
        number_link = message.text
        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Создание таблицы
        cur.execute('SELECT * FROM users')
        users = cur.fetchall()

        isnumber = False
        user_id_referer = 0
        for el in users:
            if number_link == el[5]:
                id_referer = el[0]
                user_id_referer = el[7]
                isnumber = True
        if isnumber == True:
            conn.commit()
            cur.close()
            conn.close()


            msg = bot.send_message(message.chat.id, 'Бонусы будут зачислены после регистрации!')
            msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Предлагаю познакомиться.\nВведите ваше имя')
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, user_name, True, user_id_referer)
        else:
            if message.text == 'Отмена':
                msg = bot.send_message(message.chat.id, 'Давайте тогда познакомиться.\nВведите ваше имя')
                msgs_id.append(msg)
                user_msg.append(msgs_id)
                bot.register_next_step_handler(message, user_name)
                return
            msg = bot.send_message(message.chat.id, 'Вы ввели не коректные данные, попробуйте ещё раз:')
            msgs_id.append(msg)
            if i < 15:
                msg = bot.send_message(message.chat.id, 'Если вы забыли или не знаете реферальный номер, то для продолжения напишите Отмена')
                msgs_id.append(msg)
                i += 1
                user_msg.append(msgs_id)
                bot.register_next_step_handler(message, ref_link_activate, i)
            else:
                msg = bot.send_message(message.chat.id, 'У вас кончились попытки ввода.')
                msgs_id.append(msg)
                msg = bot.send_message(message.chat.id, 'Давайте лучше познакомиться.\nВведите ваше имя')
                msgs_id.append(msg)
                user_msg.append(msgs_id)
                bot.register_next_step_handler(message, user_name)
    else:
        msg = bot.send_message(message.chat.id, 'Вы ввели не коректные данные, попробуйте ещё раз:')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        i += 1
        bot.register_next_step_handler(message, ref_link_activate, i)

# Ввод имени пользователя
def user_name(message, ref = False, id_ref = None):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if message.content_type == 'text':
        name = message.text
        if len(message.text) >= 25:
            msg = bot.send_message(message.chat.id, 'Ваше имя слишком длинное. Попробуйте ввести другое')
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, user_name, ref, id_ref)
        else:
            markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
            btn_send_nuber = types.KeyboardButton(text='Отправить номер', request_contact=True)
            markup.add(btn_send_nuber)

            msg = bot.send_message(message.chat.id, 'Введите ваш номер телефона при помощи кнопки ниже',
                                   reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, user_number, name, ref, id_ref)
    else:
        msg = bot.send_message(message.chat.id, 'Попробуйте ввести своё имя ещё раз:')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, user_name, ref, id_ref)

# Подтверждение имени
def accept_name(message, name, ref = False, id_ref = None):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
    btn_send_nuber = types.KeyboardButton(text='Отправить номер', request_contact=True)
    markup.add(btn_send_nuber)
    markup2 = types.ReplyKeyboardRemove()
    if message.text == 'Да':
        msg = bot.send_message(message.chat.id, 'Введите ваш номер телефона при помощи кнопки ниже', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, user_number, name, ref, id_ref)
    else:
        msg = bot.send_message(message.chat.id, 'Ваше имя?', reply_markup=markup2)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, user_name, ref, id_ref)

# Ввод номера пользователя
def user_number(message, name, ref = False, id_ref = None):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.contact is not None:
        number = message.contact.phone_number

        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
        btn_sexM = types.KeyboardButton(text='Муж')
        btn_sexJ = types.KeyboardButton(text='Жен')
        markup.add(btn_sexM)
        markup.add(btn_sexJ)
        msg = bot.send_message(message.chat.id, 'Ваш пол?', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, user_sex, name, number, ref, id_ref)
    else:
        bot.send_message(message.chat.id, 'Вы ввели некоректное сообщение, попробуйте ещё раз.')
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
        btn_send_nuber = types.KeyboardButton(text='Отправить номер', request_contact=True)
        markup.add(btn_send_nuber)

        msg = bot.send_message(message.chat.id, 'Если возникают трудности, можете воспользоваться кнопкой в меню.', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, user_number, name, ref, id_ref)

# Ввод секса пользователя
def user_sex(message, name, number, ref = False, id_ref = None):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    markup = types.ReplyKeyboardRemove()
    if message.text == 'Муж':
        sex = message.text
        msg = bot.send_message(message.chat.id,
                         'Введите вашу дату рожденияю, что бы мы могли поздравить вас и вручить подарки.\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]', reply_markup=markup)
        msgs_id.append(msg)
        bot.register_next_step_handler(message, user_date, name, number, sex, ref, id_ref)
    elif message.text == 'Жен':
        sex = message.text
        msg = bot.send_message(message.chat.id,
                         'Введите вашу дату рожденияю, что бы мы могли поздравить вас и вручить подарки.\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]', reply_markup=markup)
        msgs_id.append(msg)
        bot.register_next_step_handler(message, user_date, name, number, sex, ref, id_ref)
    else:
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
        btn_sexM = types.KeyboardButton(text='Муж')
        btn_sexJ = types.KeyboardButton(text='Жен')
        markup.add(btn_sexM)
        markup.add(btn_sexJ)

        msg = bot.send_message(message.chat.id, 'Введено некоректное сообщение, попробюуйте ещё раз\nпсс... воспользуйтесь кнопками', reply_markup=markup)
        msgs_id.append(msg)
        bot.register_next_step_handler(message, user_sex, name, number, ref, id_ref)
    user_msg.append(msgs_id)

# Ввод даты рождения пользователя
def user_date(message, name, number, sex, ref = False, id_ref = None):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.content_type == 'text':
        if len(message.text) == 10:
            i = 0
            user_dated = message.text.split('.')
            if len(user_dated) == 3:
                for date in user_dated:
                    for c in date:
                        if c == '0' or c == '1' or c == '2' or c == '3' or c == '4' or c == '5' or c == '6' or c == '7' or c == '8' or c == '9':
                            i += 1
            error = False
            if i == 8:
                #Проверка исключений
                if (int(user_dated[1]) == 2 and int(user_dated[0]) > 28) or (int(user_dated[0]) == 0) or (int(user_dated[1]) == 0) or (int(user_dated[2]) == 0):
                    error = True
                if (int(user_dated[1]) == 1 and int(user_dated[0]) > 31) or (int(user_dated[1]) == 3 and int(user_dated[0]) > 31) or (int(user_dated[1]) == 5 and int(user_dated[0]) > 31) or (int(user_dated[1]) == 7 and int(user_dated[0]) > 31) or (int(user_dated[1]) == 8 and int(user_dated[0]) > 31) or (int(user_dated[1]) == 10 and int(user_dated[0]) > 31) or (int(user_dated[1]) == 12 and int(user_dated[0]) > 31):
                    error = True
                if (int(user_dated[1]) == 4 and int(user_dated[0]) > 30) or (int(user_dated[1]) == 6 and int(user_dated[0]) > 30) or (int(user_dated[1]) == 9 and int(user_dated[0]) > 30) or (int(user_dated[1]) == 11 and int(user_dated[0]) > 30):
                    error = True
                if int(user_dated [1]) > 12:
                    error = True


            if i == 8 and error == False:
                date = datetime.strptime(message.text, '%d.%m.%Y').date()
                if date.year >= datetime.now().date().year - 6:
                    msg = bot.send_message(message.chat.id, 'Вам должно быть больше 6 лет.')
                    msgs_id.append(msg)

                    # Исправления дублирования
                    user_id = message.from_user.id
                    user_states[user_id] = False
                    bot.register_next_step_handler(message, user_date, name, number, sex, ref, id_ref)
                    return
                user_create_ac(message, name, number, sex, date, ref, id_ref)
                #bot.register_next_step_handler(message, user_create_ac, name, number, sex, date)
            else:
                msg = bot.send_message(message.chat.id,
                                     'Произошла ошибка\nПопробуйте ввести вашу дату рожденияю более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]')
                msgs_id.append(msg)
                bot.register_next_step_handler(message, user_date, name, number, sex, ref, id_ref)
        else:
            msg = bot.send_message(message.chat.id,
                             'Произошла ошибка\nПопробуйте ввести вашу дату рожденияю более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, user_date, name, number, sex, ref, id_ref)
    else:
        msg = bot.send_message(message.chat.id,
                         'Произошла ошибка\nПопробуйте ввести вашу дату рожденияю более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, user_date, name, number, sex, ref, id_ref)
    user_msg.append(msgs_id)

# Создание аккаунта пользователя
def user_create_ac (message, name, number, sex, date, ref = False, id_ref = None):
    usid = message.from_user.id
    for i in user_msg:
        if i != None and i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)

    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    # генерация реферального номера
    ref_link = find_regenerate_link(generate_ref_link())

    date_reg = datetime.now().date()
    chat_id = message.chat.id
    user_id = message.from_user.id
    if ref == True:
        bonus = boneses_link
    else:
        bonus = 0

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Создание таблицы
    if id_ref != None:
        cur.execute(f'UPDATE users SET bonus = bonus + {boneses_ref} WHERE user_id = {id_ref}')
        conn.commit()
    cur.execute("INSERT INTO users (name, number, sex, date_born, ref_link, chat_id, user_id, bonus, date_register, my_referer, count_checks, sum_checks, procent_my, procent_ref) VALUES ('%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (name, number, sex, date, ref_link, chat_id, user_id, bonus, date_reg, id_ref, 0, 0, procent_my, procent_ref))
    cur.execute(
        "INSERT INTO bonuses (user_id, number, bonuses, count_day) VALUES ('%s', '%s', '%s', '%s')" % (
            user_id, number, 0, 0))
    conn.commit()
    cur.close()
    conn.close()

    # Исправления дублирования
    user_id = message.from_user.id
    user_states[user_id] = False

    msg = bot.send_message(message.chat.id, 'Вы успешно зарегистрировались!')
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    menu(message)


# Генерация Реферального талона
def generate_ref_link(lenght = 7):
    all_symbols = string.digits
    result = ''.join(random.choice(all_symbols) for s in range(lenght))
    return result

# Проверка повтора реферального номера
def find_regenerate_link(ref_link):
    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()

    # Проверка повторов
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()
    isFound = False
    for el in users:
        if str(ref_link) == str(el[5]):
            isFound = True
    if isFound == True:
        bot.send_message(1028414646, 'Найдено повторение... генерация нового кода')
        ref_link = find_regenerate_link(generate_ref_link())
        bot.send_message(1028414646, f'Изменено на {ref_link}')
        conn.commit()
        cur.close()
        conn.close()
        return ref_link
    else:
        conn.commit()
        cur.close()
        conn.close()
        return ref_link

# Отправляет данные о бонусах и реф ссылки
def send_bonuses_user(call):
    uscid = call.message.chat.id
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Создание таблицы
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()

    for el in users:
        if str(uscid) == str(el[6]):
            msg = bot.send_message(call.message.chat.id, f'Ваши бонусы: {str(el[8])}\nВаш реферальный номер: {str(el[5])}')
            msgs_id.append(msg)
    cur.close()
    conn.close()
    user_msg.append(msgs_id)

# Отправляет контактные данные пользователю
def send_contact_user(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    msg = bot.send_message(call.message.chat.id, '📱Наш номер телефона: +77054816511\nМы находимся на улице Интернациональная 1Б.\nВы можете позвонить и совершить заказ, а также заказать доставку)')
    msgs_id.append(msg)
    user_msg.append(msgs_id)

# Отзыв - Получение
def send_request_user(call):
    usid = call.from_user.id

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

    b_cancel = types.KeyboardButton(text='Отмена')
    markup.row(b_cancel)

    msg = bot.send_message(call.message.chat.id, 'Оставьте свой отзыв, мы прочтем его в близжайшее время', reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, send_mess_to_admin)

# Отзыв - Отправление админам
def send_mess_to_admin(message):
    user_id = message.from_user.id
    for i in user_msg:
        if i[0] == user_id:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(user_id)
    msgs_id.append(message)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Создание таблицы
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()

    markup = types.ReplyKeyboardRemove()

    if message.text == 'Отмена' or message.text == '/menu':
        msg = bot.send_message(message.chat.id, 'Отзыв не был оставлен.', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)

        user_states[user_id] = False
        return

    for el in users:
        if str(user_id) == str(el[7]):
            for i in admins:
                bot.forward_message(i, message.chat.id, message.message_id)
                bot.send_message(i,
                                 f'Имя этого подонка: {el[1]}\nНомер этого ублюдка: {el[2]}')
    msg = bot.send_message(message.chat.id, 'Ваше сообщение было отправлено!', reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    cur.close()
    conn.close()

    # Исправления дублирования
    user_states[user_id] = False

def my_ref(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Изменение имени
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()
    info = 'У вас '
    infob = ''
    n = 0
    ref_number = ''
    for el in users:
        if str(call.from_user.id) == str(el[10]):
            infob += f'{el[1]}, '
            n += 1
        if str(call.message.chat.id) == str(el[6]):
            ref_number += str(el[5])
    if n == 0:
        msg = bot.send_message(call.message.chat.id, f'У вас нет рефералов.\nДля того чтобы добавить реферала пригласите друга. При регистрации ему нужно будет ввести этот номер {ref_number}.')
        msgs_id.append(msg)
    else:
        info += str(n) + ' рефералов:\n' + infob + f'\nВам начисляется 1% с каждой покупки ваших рефералов. Ваш номер: {ref_number}'
        msg = bot.send_message(call.message.chat.id, info)
        msgs_id.append(msg)
    user_msg.append(msgs_id)

# Инфо пользователя
def info_user(call):
    usid = call.from_user.id
    for i in user_msg:
        if i != None and i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Создание таблицы
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()


    for el in users:
        if str(usid) == str(el[7]):
            msg = bot.send_message(call.message.chat.id, f"Ваше имя {str(el[1])}\nНомер: {str(el[2])}\nВаш пол: {str(el[3])}\nВаша дата рождения: {str(el[4])}\nВаш реферальный номер: {str(el[5])}\nВаши бонусы: {str(el[8])}")
            msgs_id.append(msg)

    cur.close()
    conn.close()
    user_msg.append(msgs_id)

# Смена пола (винды)
def res(call):
    # Исправления дублирования
    user_id = call.from_user.id
    if user_id in user_states and user_states[user_id]:
        return
    user_states[user_id] = True

    for i in user_msg:
        if i!= None and i[0] == user_id:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(user_id)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Изменение имени
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()

    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
    b_yes = types.KeyboardButton('Муж')
    b_n = types.KeyboardButton('Жен')
    markup.add(b_yes, b_n)

    for el in users:
        if str(call.message.chat.id) == str(el[6]):
            msg = bot.send_message(call.message.chat.id, f'Нынешний пол: {el[3]}\nВыберете свой пол', reply_markup=markup)
            msgs_id.append(msg)

    cur.close()
    conn.close()
    user_msg.append(msgs_id)

    bot.register_next_step_handler(call.message, resex)
def resex(message):
    markup = types.ReplyKeyboardRemove()
    if message.text == 'Муж':
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    try:
                        bot.delete_message(message.chat.id, m.message_id)
                    except Exception as e:
                        print(f'Ошибка удаления сообщения: {e}')
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)

        sex = message.text

        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Изменение имени
        cur.execute(f"UPDATE users SET sex = '%s' WHERE user_id = '%s'" % (sex, usid))
        conn.commit()
        cur.close()
        conn.close()

        msg = bot.send_message(message.chat.id, f'Ваш пол был успешно изменен', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)

        # Исправления дублирования
        user_states[usid] = False
    elif message.text == 'Жен':
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    bot.delete_message(message.chat.id, m.id)
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)

        sex = message.text

        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Изменение имени
        cur.execute(f"UPDATE users SET sex = '%s' WHERE user_id = '%s'" % (sex, usid))
        conn.commit()
        cur.close()
        conn.close()

        msg = bot.send_message(message.chat.id, f'Ваш пол был успешно изменен', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)

        # Исправления дублирования
        user_states[usid] = False
    elif message.text == 'Отмена':
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    bot.delete_message(message.chat.id, m.id)
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)

        msg = bot.send_message(message.chat.id, f'Ваш пол не был измененен')

        msgs_id.append(msg)
        user_msg.append(msgs_id)

        # Исправления дублирования
        user_states[usid] = False
    else:
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    bot.delete_message(message.chat.id, m.id)
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)

        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
        btn_sexM = types.KeyboardButton(text='Муж')
        btn_sexJ = types.KeyboardButton(text='Жен')
        markup.add(btn_sexM)
        markup.add(btn_sexJ)

        msg = bot.send_message(message.chat.id,
                         'Введено некоректное сообщение, попробюуйте ещё раз\nпсс... воспользуйтесь кнопками (Для отмены действия напишите Отмена)',
                         reply_markup=markup)

        msgs_id.append(msg)
        user_msg.append(msgs_id)

# Смена имени
def ren(call):
    # Исправления дублирования
    user_id = call.from_user.id
    if user_id in user_states and user_states[user_id]:
        return
    user_states[user_id] = True

    for i in user_msg:
        if i[0] == user_id:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(user_id)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Изменение имени
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()

    msg = None

    for el in users:
        if str(call.message.chat.id) == str(el[6]):
            msg = bot.send_message(call.message.chat.id, f'Нынешнее имя: {el[1]}\nВведите новое имя')


    cur.close()
    conn.close()
    if msg != None:
        msgs_id.append(msg)
    user_msg.append(msgs_id)

    bot.register_next_step_handler(call.message, rename)
def rename(message):
    usid = message.from_user.id
    for i in user_msg:
        if i != None and i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.content_type == 'text':
        id = message.from_user.id
        name_new = message.text

        if len(message.text) >= 25:
            msg = bot.send_message(message.chat.id, 'Ваше имя слишком длинное. Попробуйте ввести другое')
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, rename)
        elif message.text == 'Отмена':
            msg = bot.send_message(message.chat.id, f'Ваше имя не было измененно')
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            # Исправления дублирования
            user_states[usid] = False
        else:
            # База данных
            conn = sqlite3.connect('ClientsBase.kts')
            cur = conn.cursor()
            # Изменение имени
            cur.execute(f"UPDATE users SET name = '%s' WHERE user_id = '%s'" % (name_new, id))
            conn.commit()
            cur.close()
            conn.close()

            msg = bot.send_message(message.chat.id, f'Ваше имя было успешно изменено на {name_new}')
            msgs_id.append(msg)
            user_msg.append(msgs_id)

            # Исправления дублирования
            user_states[usid] = False
    else:
        msg = bot.send_message(message.chat.id, 'Попробуйте ввести своё имя ещё раз.\nДля отмены действия напишите Отмена')
        msgs_id.append(msg)
        user_msg.append(msgs_id)

        bot.register_next_step_handler(message, rename)

# Смена даты рождения
def red(call):
    # Исправления дублирования
    user_id = call.from_user.id
    if user_id in user_states and user_states[user_id]:
        return
    user_states[user_id] = True

    for i in user_msg:
        if i != None and i[0] == user_id:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(user_id)

    # База данных
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()
    # Изменение имени
    cur.execute('SELECT * FROM users')
    users = cur.fetchall()

    date_old = None
    for el in users:
        if str(call.message.chat.id) == str(el[6]):
            date_old = el[4].split('-')

            msg = bot.send_message(call.message.chat.id, f'Нынешняя дата: {date_old[2]}.{date_old[1]}.{date_old[0]}\nВведите свою дату рождения.\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]')
            msgs_id.append(msg)
    cur.close()
    conn.close()

    user_msg.append(msgs_id)

    bot.register_next_step_handler(call.message, redate_born)
def redate_born(message):
    usid = message.from_user.id
    for i in user_msg:
        if i != None and i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.content_type == 'text':
        if len(message.text) == 10:
            i = 0
            user_dated = message.text.split('.')
            if len(user_dated) == 3:
                for date in user_dated:
                    for c in date:
                        if c == '0' or c == '1' or c == '2' or c == '3' or c == '4' or c == '5' or c == '6' or c == '7' or c == '8' or c == '9':
                            i += 1
            error = False
            if i == 8:
                # Проверка исключений
                if (int(user_dated[1]) == 2 and int(user_dated[0]) > 28) or (int(user_dated[0]) == 0) or (
                        int(user_dated[1]) == 0) or (int(user_dated[2]) == 0):
                    error = True
                if (int(user_dated[1]) == 1 and int(user_dated[0]) > 31) or (
                        int(user_dated[1]) == 3 and int(user_dated[0]) > 31) or (
                        int(user_dated[1]) == 5 and int(user_dated[0]) > 31) or (
                        int(user_dated[1]) == 7 and int(user_dated[0]) > 31) or (
                        int(user_dated[1]) == 8 and int(user_dated[0]) > 31) or (
                        int(user_dated[1]) == 10 and int(user_dated[0]) > 31) or (
                        int(user_dated[1]) == 12 and int(user_dated[0]) > 31):
                    error = True
                if (int(user_dated[1]) == 4 and int(user_dated[0]) > 30) or (
                        int(user_dated[1]) == 6 and int(user_dated[0]) > 30) or (
                        int(user_dated[1]) == 9 and int(user_dated[0]) > 30) or (
                        int(user_dated[1]) == 11 and int(user_dated[0]) > 30):
                    error = True
                if int(user_dated[1]) > 12:
                    error = True

            if i == 8 and error == False:
                date = datetime.strptime(message.text, '%d.%m.%Y').date()
                if date.year >= datetime.now().date().year - 6:
                    msg = bot.send_message(message.chat.id, 'Вам должно быть больше 6 лет.')
                    msgs_id.append(msg)



                    # Исправления дублирования
                    user_states[usid] = False
                    return

                # База данных
                conn = sqlite3.connect('ClientsBase.kts')
                cur = conn.cursor()
                # Изменение имени
                cur.execute(f"UPDATE users SET date_born = '%s' WHERE user_id = '%s'" % (date, usid))
                conn.commit()
                cur.close()
                conn.close()

                msg = bot.send_message(message.chat.id, f'Ваша дата рождения было успешно изменено')
                msgs_id.append(msg)


                # Исправления дублирования
                user_states[usid] = False
            else:
                msg = bot.send_message(message.chat.id,
                                 'Произошла ошибка\nПопробуйте ввести вашу дату рожденияю более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
                msgs_id.append(msg)

                bot.register_next_step_handler(message, redate_born)
        elif message.text == 'Отмена':
            msg = bot.send_message(message.chat.id, 'Дата рождения не была изменена.')
            msgs_id.append(msg)
            user_states[usid] = False
        else:
            msg = bot.send_message(message.chat.id,
                             'Произошла ошибка\nПопробуйте ввести вашу дату рожденияю более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, redate_born)
    else:
        msg = bot.send_message(message.chat.id,
                         'Произошла ошибка\nПопробуйте ввести вашу дату рожденияю более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, redate_born)
    user_msg.append(msgs_id)

# Команда /clients
@bot.message_handler(commands=['clients'])
def base_data_clients(message):
    chat_id = None
    for ad in admins:
        if message.chat.id == ad:
            chat_id = ad
    if chat_id != None:
        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Создание таблицы
        cur.execute('SELECT * FROM users')
        users = cur.fetchall()

        info = ''
        for el in users:
            info += f'*ID:* {el[0]} Имя: {el[1]} *Номер:* {el[2]} Пол: {el[3]} Дата рождения: {el[4]} Реферальный номер: {el[5]} CHAT ID: {el[6]} USER ID: {el[7]} Бонусы: {el[8]} Дата регистрации; {el[9]} Реферер: {el[10]} Количество чеков: {el[11]} Сумма выкупа: {el[12]}, Его процент: {el[13]}, Реферальный процент: {el[14]}\n'

        send_long_message(message.chat.id, info, parse_mode="Markdown")
    else:
        bot.send_message(message.chat.id, f'Ваш код {message.chat.id}')

def send_long_message(chat_id, text, parse_mode=None):
    """Отправляет длинное сообщение, разбивая его на части, сохраняя форматирование Markdown."""
    max_length = 4096

    def safe_split(text, max_length):
        parts = []
        while len(text) > max_length:
            # Найти последнее безопасное место для разрыва перед max_length
            split_pos = max_length
            while split_pos > 0 and not re.match(r'[\s\n]', text[split_pos]):
                split_pos -= 1
            if split_pos == 0:
                split_pos = max_length
            parts.append(text[:split_pos])
            text = text[split_pos:]
        parts.append(text)
        return parts

    parts = safe_split(text, max_length)

    for part in parts:
        bot.send_message(chat_id, part, parse_mode=parse_mode)


# Команда /timer
@bot.message_handler(commands=['timer'])
def base_bonuses_clients(message):
    chat_id = None
    for ad in admins:
        if message.chat.id == ad:
            chat_id = ad
    if chat_id != None:
        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Создание таблицы
        cur.execute('SELECT * FROM bonuses')
        bonuses_list = cur.fetchall()

        info = ''
        for el in bonuses_list:
            info += f'ID: {el[0]} USID: {el[1]} Номер: {el[2]} Бонусы: {el[3]} Сколько дней: {el[4]}\n'

        if len(info) >= 4096:
            for i in range(0, len(info), 4096):
                bot.send_message(message.chat.id, str(info[i:i + 4096]))
        else:
            bot.send_message(message.chat.id, info)
    else:
        bot.send_message(message.chat.id, f'Ваш код {message.chat.id}')

# Команда /orders
@bot.message_handler(commands=['orders'])
def base_date_orders(message):
    chat_id = None
    for ad in admins:
        if message.chat.id == ad:
            chat_id = ad
    if chat_id != None:
        # Подключаю библеотеку чеков
        conn = sqlite3.connect('OrdersBase.kts')
        cur = conn.cursor()
        cur.execute('SELECT * FROM orders')
        orders = cur.fetchall()

        for el in orders:
            bot.send_message(message.chat.id,
                             f'ID: {el[0]}, Кто заказал: {el[1]}, Сумма чека: {el[2]}, Тенге: {el[3]}, Бонусами: {el[4]}, Дата: {el[5]}, Время: {el[6]}')
        cur.close()
        conn.close()
    else:
        bot.send_message(message.chat.id, f'Ваш код {message.chat.id}')


# Команда для обработки времени
def foo():
    while True:
        send_mess_date_born()
        time.sleep(1)
# -----------------------------------------------------------------------------
def send_mess_date_born():
    Date_Now = datetime.now() + timedelta(hours=5)
    date_now = Date_Now.date()
    time_now = Date_Now.time()
    if str(time_now)[:9] == '12:00:00.000000'[:9]:
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()

        cur.execute('SELECT * FROM users')
        users = cur.fetchall()
        cur.execute('SELECT * FROM bonuses')
        bonuses_list = cur.fetchall()

        for el in users:
            if str(el[4])[4:] == str(date_now)[4:]:
                bot.send_message(int(el[6]), 'С днем рождения!!!\nСегодня при заказе, вы получите темпуру с курицей в подарок!\nНе забудьте взять с собой удостоверение)')
            for i in bonuses_list:
                if str(el[7]) == str(i[1]) and int(i[3]) < count_days_mess:  # пока не наступил наш день
                    cur.execute(f"UPDATE bonuses SET count_day = count_day + {1} WHERE number = {i[2]}"
                                )
                    conn.commit()
                elif str(el[7]) == str(i[1]) and int(i[3]) == count_days_mess:  # 40
                    cur.execute(f"UPDATE bonuses SET count_day = count_day + {1} WHERE number = {i[2]}"
                                )
                    cur.execute(f"UPDATE bonuses SET bonuses = bonuses + {count_bonus_timed} WHERE number = {i[2]}"
                                )
                    cur.execute(f'UPDATE users SET bonus = bonus + {count_bonus_timed} WHERE number = {el[2]}')
                    conn.commit()
                    bot.send_message(int(el[6]),
                                     f'Здравствуйте {el[1]}!\nМы дарим вам {count_bonus_timed} бонусов. Успейте потратить)')
                elif str(el[7]) == str(i[1]) and int(i[3]) <= (count_days_mess + count_days_mess_out):
                    cur.execute(f"UPDATE bonuses SET count_day = count_day + {1} WHERE number = {i[2]}"
                                )
                    conn.commit()
                elif str(el[7]) == str(i[1]) and int(i[3]) > (count_days_mess + count_days_mess_out):
                    cur.execute(f"UPDATE bonuses SET count_day = {0} WHERE number = {i[2]}"
                                )
                    cur.execute(f"UPDATE bonuses SET bonuses = bonuses - {count_bonus_timed} WHERE number = {i[2]}"
                                )
                    cur.execute(f'UPDATE users SET bonus = bonus - {count_bonus_timed} WHERE number = {el[2]}')
                    conn.commit()
        cur.close()
        conn.close()
# -----------------------------------------------------------------------------

#Для работников
@bot.message_handler(commands=['work'])
def work(message):
    chat_id = None
    for ad in works:
        if message.chat.id == ad:
            chat_id = ad
    if chat_id != None:
        markup = types.InlineKeyboardMarkup()
        b_newcheck = types.InlineKeyboardButton('Новый заказ', callback_data='new_check')
        b_delcheck = types.InlineKeyboardButton('Удалить заказ', callback_data='del_check')
        b_info_client = types.InlineKeyboardButton('Информация о клиенте', callback_data='info_client')
        b_end = types.InlineKeyboardButton('Итог дня', callback_data='end_day')
        markup.add(b_newcheck)
        markup.add(b_delcheck)
        markup.add(b_info_client)
        markup.add(b_end)

        bot.send_message(chat_id, 'Вы вошли в рабочий аккаунт!', reply_markup=markup)
    else:
        bot.send_message(message.chat.id, f'Ваш код {message.chat.id}')

def new_check(call):
    user_id = call.from_user.id
    if user_id in user_states and user_states[user_id]:
        return
    user_states[user_id] = True

    for i in user_msg:
        if i and i[0] == user_id:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(user_id)

    # Новая база данных для Заказов
    conn = sqlite3.connect('OrdersBase.kts')
    cur = conn.cursor()
    # Создание таблицы
    cur.execute(
        'CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id varchar(33), sum_check INT NOT NULL, sum_in_tenge INT NOT NULL, sum_bonus INT NOT NULL, date_check varchar(10), time_check varchar(20))')
    conn.commit()

    msg = bot.send_message(call.message.chat.id,
                     'Напоминания пользования:\nСначала введите номер клиента сотовый в формате:\n7 (ххх) ххх хх хх\nПлюсик не нужно!\nДля отмены действия введите Отмена')
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, register_bonus_number)
def register_bonus_number(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.content_type == 'text':
        number = message.text
        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Создание таблицы
        cur.execute('SELECT * FROM users')
        users = cur.fetchall()

        isnumber = False
        my_ref = None
        client_bonus = 0
        user = []
        for el in users:
            if number == el[2]:
                my_ref = el[10]
                client_bonus = el[8]
                isnumber = True
                user = el
        if isnumber == True:
            msg = bot.send_message(message.chat.id, f'Номер найден! У клиента {user[11]} заказов и {client_bonus} бонусов.\nВведите сумму чека:\nДля отмены действия введите Отмена')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, sum_check, number, my_ref)
        elif message.text == 'Отмена':
            msg = bot.send_message(message.chat.id, f'Вы отменили команду - Новый чек')
            msgs_id.append(msg)

            # Исправления дублирования
            user_states[usid] = False
            user_msg.append(msgs_id)
            return
        else:
            msg = bot.send_message(message.chat.id, 'Номер не найден( и камень я не дам\nПопробуйте ещё раз')
            msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                             'Напоминания пользования:\nСначала введите номер клиента сотовый в формате:\n7 (ххх) ххх хх хх\nПлюсик не нужно!\nДля отмены действия введите Отмена')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, register_bonus_number)
    else:
        msg = bot.send_message(message.chat.id, 'Вы ввели не номер, попробуйте ещё раз')
        msgs_id.append(msg)
        msg = bot.send_message(message.chat.id,
                         'Напоминания пользования:\nСначала введите номер клиента сотовый в формате:\n7 (ххх) ххх хх хх\nДля отмены действия введите Отмена')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, register_bonus_number)
    user_msg.append(msgs_id)

def need_to_bonus(message, sum, number, my_ref):
    markup = types.ReplyKeyboardRemove()
    if message.text == 'Да':
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    try:
                        bot.delete_message(message.chat.id, m.message_id)
                    except Exception as e:
                        print(f'Ошибка удаления сообщения: {e}')
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)

        msg = bot.send_message(message.chat.id, f'Введите количество бонусов которое хотите потратить\nЕсли нужно все, то введите Все', reply_markup=markup)
        msgs_id.append(msg)

        bot.register_next_step_handler(message, relize_check, sum, number, my_ref, True)

        user_msg.append(msgs_id)
    elif message.text == 'Нет':
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    bot.delete_message(message.chat.id, m.id)
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)
        msg = bot.send_message(message.chat.id, f'Хорошо, происходит регистрация чека', reply_markup=markup)
        relize_check(message, sum, number, my_ref)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
    elif message.text == 'Отмена':
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    bot.delete_message(message.chat.id, m.id)
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)
        msg = bot.send_message(message.chat.id, f'Вы отменили команду - Новый чек', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        # Исправления дублирования
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    else:
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    bot.delete_message(message.chat.id, m.id)
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
        btn_sexM = types.KeyboardButton(text='Да')
        btn_sexJ = types.KeyboardButton(text='Нет')
        markup.add(btn_sexM)
        markup.add(btn_sexJ)

        msg = bot.send_message(message.chat.id,
                               'Введено некоректное сообщение, попробюуйте ещё раз\nпсс... воспользуйтесь кнопками (Для отмены действия напишите Отмена)',
                               reply_markup=markup)

        msgs_id.append(msg)
        user_msg.append(msgs_id)


def sum_check(message, number, my_ref):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.content_type == 'text':
        isCheckTrue = any(character.isdigit() for character in message.text)
        if isCheckTrue == True:
            sum = int(message.text)

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
            b_y = types.KeyboardButton('Да')
            b_n = types.KeyboardButton('Нет')
            markup.add(b_y)
            markup.add(b_n)

            msg = bot.send_message(message.chat.id, 'Желает ли пользователь потратить бонусы?', reply_markup=markup)
            msgs_id.append(msg)
            bot.register_next_step_handler(message, need_to_bonus, sum, number, my_ref)




        elif message.text == 'Отмена':
            msg = bot.send_message(message.chat.id, f'Вы отменили команду - Новый чек')
            msgs_id.append(msg)

            # Исправления дублирования
            user_states[usid] = False
            user_msg.append(msgs_id)
            return
        else:
            msg = bot.send_message(message.chat.id, 'Вы ввели не цифры попробуйте ещё раз. Ещё я думаю вы недалекий\nВведите сумму чека:\nДля отмены действия введите Отмены')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, sum_check, number, my_ref)
    else:
        msg = bot.send_message(message.chat.id, 'Вы ввели не цифры попробуйте ещё раз\nВведите сумму чека:\nДля отмены действия введите Отмены')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, sum_check, number, my_ref)

    user_msg.append(msgs_id)

def relize_check(message, sum, number, my_ref, y_b = False):
    if y_b == True:
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    bot.delete_message(message.chat.id, m.id)
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msgs_id.append(message)
        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Создание таблицы
        cur.execute('SELECT * FROM users')
        users = cur.fetchall()
        client_id = None
        client_sum_checks = None
        chat_id = None
        chat_id_ref = None
        percent_us = None
        percent_ref = None
        client_bonus = 0
        for el in users:
            if number == el[2]:
                client_bonus = el[8]
                client_id = el[7]
                chat_id = el[6]
                percent_us = el[13]
                client_sum_checks = el[12]
            if str(my_ref) == str(el[7]):
                chat_id_ref = el[6]
                percent_ref = el[14]
        user = cur.execute(f'SELECT * FROM users WHERE user_id = {client_id}').fetchone()
        if check_sum_if2 <= user[12] < check_sum_if3 and user[13] != procent_my2:
            cur.execute(f"UPDATE users SET procent_my = {procent_my2} WHERE user_id = {client_id}")
            conn.commit()
            bot.send_message(chat_id, 'Ваш процент бонусов был увеличен!\nПродолжайте в том же духе!')
            percent_us = procent_my2
        elif check_sum_if3 <= user[12] < check_sum_if4 and user[13] != procent_my3:
            cur.execute(f"UPDATE users SET procent_my = {procent_my3} WHERE user_id = {client_id}")
            conn.commit()
            bot.send_message(chat_id, 'Ваш процент бонусов был увеличен!\nПродолжайте в том же духе!')
            percent_us = procent_my3
        elif check_sum_if4 <= user[12] and user[13] != procent_my4:
            cur.execute(f"UPDATE users SET procent_my = {procent_my4} WHERE user_id = {client_id}")
            conn.commit()
            bot.send_message(chat_id, 'Ваш процент бонусов был увеличен!\nПродолжайте в том же духе!')
            percent_us = procent_my4
        isCheckTrue = any(character.isdigit() for character in message.text)
        if message.text == 'Все':
            if client_bonus >= sum:
                bonus_minus = sum
            elif client_bonus <= 0:
                bonus_minus = 0
            else:
                bonus_minus = client_bonus
            cur.execute(f'UPDATE users SET bonus = bonus - {bonus_minus} WHERE number = {number}')
            sun_tenge = int(sum - bonus_minus)
            boneses = int(sun_tenge * percent_us)  # проценты от покупки
            bot.send_message(chat_id, f'Здравствуйте, Вы совершили заказ на {sum}тг и получили {boneses} бонусов')

            cur.execute(f'UPDATE users SET bonus = bonus + {boneses} WHERE number = {number}')
            cur.execute(f'UPDATE users SET count_checks = count_checks + {1} WHERE number = {number}')
            cur.execute(f'UPDATE users SET sum_checks = sum_checks + {sum} WHERE number = {number}')
            conn.commit()

            info = cur.execute('SELECT * FROM bonuses WHERE number=?', (number, )).fetchone()
            if info[3] > 0:
                cur.execute(f"UPDATE bonuses SET count_day = {0} WHERE number = {info[2]}")
                if bonus_minus >= count_bonus_timed:
                    cur.execute(f"UPDATE bonuses SET bonuses = {0} WHERE number = {info[2]}")
                else:
                    cur.execute(f"UPDATE bonuses SET bonuses = bonuses - {bonus_minus} WHERE number = {info[2]}")
            else:
                cur.execute(f"UPDATE bonuses SET count_day = {0} WHERE number = {info[2]}")

            if my_ref != 'None':
                bonus_ref = int(sun_tenge * percent_ref)  # проценты от покупки
                if bonus_ref != 0:
                    bot.send_message(chat_id_ref, f'Ваш друг, совершил покупку. Вам были начислены бонусы {bonus_ref}')
                cur.execute(f'UPDATE users SET bonus = bonus + {bonus_ref} WHERE user_id = {my_ref}')
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            conn2 = sqlite3.connect('OrdersBase.kts')
            cur2 = conn2.cursor()
            Date_Now = datetime.now() + timedelta(hours=5)
            date_order = Date_Now.date()
            time_order = Date_Now.time()
            cur2.execute("INSERT INTO orders (user_id, sum_check, sum_in_tenge, sum_bonus, date_check, time_check) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % (client_id, sum, sun_tenge, bonus_minus, date_order, time_order))
            conn2.commit()
            cur2.close()
            conn2.close()

            info = ''
            for el in users:
                if number == el[2] or my_ref == el[7]:
                    info += f'ID: {el[0]} Имя: {el[1]} Номер: {el[2]} Бонусы: {el[8]}\n'

            msg = bot.send_message(message.chat.id, info)
            msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Сумма введена!')
            msgs_id.append(msg)
            user_states[usid] = False
        elif isCheckTrue:
            if int(message.text) > client_bonus:
                msg = bot.send_message(message.chat.id, 'У клента нет столько бонусов\nПопробуйте ещё раз\nДля отмены действия введите Отмена')
                msgs_id.append(msg)
                bot.register_next_step_handler(message, relize_check, sum, number, my_ref, True)
            else:
                bonus_minus = int(message.text)
                cur.execute(f'UPDATE users SET bonus = bonus - {bonus_minus} WHERE number = {number}')
                sun_tenge = int(sum - bonus_minus)
                boneses = int(sun_tenge * percent_us)  # проценты от покупки

                bot.send_message(chat_id, f'Здравствуйте вы совершили заказ на {sum}тг и получили {boneses} бонусов')
                cur.execute(f'UPDATE users SET bonus = bonus + {boneses} WHERE number = {number}')
                cur.execute(f'UPDATE users SET count_checks = count_checks + {1} WHERE number = {number}')
                cur.execute(f'UPDATE users SET sum_checks = sum_checks + {sum} WHERE number = {number}')
                conn.commit()

                info = cur.execute('SELECT * FROM bonuses WHERE number=?', (number,)).fetchone()
                if info[3] > 0:
                    cur.execute(f"UPDATE bonuses SET count_day = {0} WHERE number = {info[2]}")
                    if bonus_minus >= count_bonus_timed:
                        cur.execute(f"UPDATE bonuses SET bonuses = {0} WHERE number = {info[2]}")
                    else:
                        cur.execute(f"UPDATE bonuses SET bonuses = bonuses - {bonus_minus} WHERE number = {info[2]}")
                else:
                    cur.execute(f"UPDATE bonuses SET count_day = {0} WHERE number = {info[2]}")

                if my_ref != 'None':
                    bonus_ref = int(sun_tenge * percent_ref)  # проценты от покупки
                    if bonus_ref != 0:
                        bot.send_message(chat_id_ref,
                                     f'Ваш друг, совершил покупку. Вам были начислены бонусы {bonus_ref}')
                    cur.execute(f'UPDATE users SET bonus = bonus + {bonus_ref} WHERE user_id = {my_ref}')
                cur.execute('SELECT * FROM users')
                users = cur.fetchall()

                conn2 = sqlite3.connect('OrdersBase.kts')
                cur2 = conn2.cursor()
                Date_Now = datetime.now() + timedelta(hours=5)
                date_order = Date_Now.date()
                time_order = Date_Now.time()
                cur2.execute(
                    "INSERT INTO orders (user_id, sum_check, sum_in_tenge, sum_bonus, date_check, time_check) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % (
                    client_id, sum, sun_tenge, bonus_minus, date_order, time_order))
                conn2.commit()
                cur2.close()
                conn2.close()

                info = ''
                for el in users:
                    if number == el[2] or my_ref == el[7]:
                        info += f'ID: {el[0]} Имя: {el[1]} Номер: {el[2]} Бонусы: {el[8]}\n'

                msg = bot.send_message(message.chat.id, info)
                msgs_id.append(msg)
                msg = bot.send_message(message.chat.id, 'Сумма введена!')
                msgs_id.append(msg)
                user_states[usid] = False

        elif message.text == 'Отмена':
            msg = bot.send_message(message.chat.id, f'Вы отменили команду - Новый чек')
            msgs_id.append(msg)

            # Исправления дублирования
            user_states[usid] = False
            user_msg.append(msgs_id)
            conn.commit()
            cur.close()
            conn.close()
            return
        else:
            msg = bot.send_message(message.chat.id,
                                   'Вы ввели не цифры попробуйте ещё раз. Ещё я думаю вы недалекий\nВведите Количество бонусов:\nДля отмены действия введите Отмены')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, relize_check, sum, number, my_ref, True)
        conn.commit()
        cur.close()
        conn.close()

        user_msg.append(msgs_id)
    else:
        usid = message.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    bot.delete_message(message.chat.id, m.id)
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)

        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Создание таблицы
        cur.execute('SELECT * FROM users')
        users = cur.fetchall()
        client_id = None
        chat_id = None
        chat_id_ref = None
        percent_us = None
        percent_ref = None
        for el in users:
            if number == el[2]:
                client_id = el[7]
                chat_id = el[6]
                percent_us = el[13]
            if str(my_ref) == str(el[7]):
                chat_id_ref = el[6]
                percent_ref = el[14]
        user = cur.execute(f'SELECT * FROM users WHERE user_id = {client_id}').fetchone()
        if check_sum_if2 <= user[12] < check_sum_if3 and user[13] != procent_my2:
            cur.execute(f"UPDATE users SET procent_my = {procent_my2} WHERE user_id = {client_id}")
            conn.commit()
            bot.send_message(chat_id, 'Ваш процент бонусов был увеличен!\nПродолжайте в том же духе!')
            percent_us = procent_my2
        elif check_sum_if3 <= user[12] < check_sum_if4 and user[13] != procent_my3:
            cur.execute(f"UPDATE users SET procent_my = {procent_my3} WHERE user_id = {client_id}")
            conn.commit()
            bot.send_message(chat_id, 'Ваш процент бонусов был увеличен!\nПродолжайте в том же духе!')
            percent_us = procent_my3
        elif check_sum_if4 <= user[12] and user[13] != procent_my4:
            cur.execute(f"UPDATE users SET procent_my = {procent_my4} WHERE user_id = {client_id}")
            conn.commit()
            bot.send_message(chat_id, 'Ваш процент бонусов был увеличен!\nПродолжайте в том же духе!')
            percent_us = procent_my4
        boneses = int(sum * percent_us)  # проценты от покупки

        bot.send_message(chat_id, f'Здравствуйте вы совершили заказ на {sum}тг и получили {boneses} бонусов')
        cur.execute(f'UPDATE users SET bonus = bonus + {boneses} WHERE number = {number}')
        cur.execute(f'UPDATE users SET count_checks = count_checks + {1} WHERE number = {number}')
        cur.execute(f'UPDATE users SET sum_checks = sum_checks + {sum} WHERE number = {number}')
        conn.commit()

        try:
            info = cur.execute('SELECT * FROM bonuses WHERE number=?', (number,)).fetchone()
            if info[3] > 0:
                cur.execute(f"UPDATE bonuses SET count_day = {0} WHERE number = {info[2]}")
                cur.execute(f"UPDATE bonuses SET bonuses = {0} WHERE number = {info[2]}")
            else:
                cur.execute(f"UPDATE bonuses SET count_day = {0} WHERE number = {info[2]}")
        except Exception as e:
            print(f'Ошибка Релиз чек "нет", не сбросил таймер дня и не списал бонусы: {e}')
        
        if my_ref != 'None':
            bonus_ref = int(sum * percent_ref)  # проценты от покупки
            if bonus_ref != 0:
                bot.send_message(chat_id_ref,
                             f'Ваш друг, совершил покупку. Вам были начислены бонусы {bonus_ref}')
            cur.execute(f'UPDATE users SET bonus = bonus + {bonus_ref} WHERE user_id = {my_ref}')

        for el in users:
            if number == el[2]:
                client_id = el[7]

        conn2 = sqlite3.connect('OrdersBase.kts')
        cur2 = conn2.cursor()
        Date_Now = datetime.now() + timedelta(hours=5)
        date_order = Date_Now.date()
        time_order = Date_Now.time()
        cur2.execute(
            "INSERT INTO orders (user_id, sum_check, sum_in_tenge, sum_bonus, date_check, time_check) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % (
                client_id, sum, sum, 0, date_order, time_order))
        conn.commit()
        conn2.commit()
        cur2.close()
        conn2.close()

        cur.execute('SELECT * FROM users')
        users = cur.fetchall()
        info = ''
        for el in users:
            if number == el[2] or my_ref == el[7]:
                info += f'ID: {el[0]} Имя: {el[1]} Номер: {el[2]} Бонусы: {el[8]}\n'

        msg = bot.send_message(message.chat.id, info)
        msgs_id.append(msg)

        conn.commit()
        cur.close()
        conn.close()

        msg = bot.send_message(message.chat.id, 'Сумма введена!')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        user_states[usid] = False

def del_check(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    b_sum_check = types.KeyboardButton('Сумма чека')
    b_date_check = types.KeyboardButton('По дате')
    b_cancel = types.KeyboardButton('Отмена')
    markup.add(b_sum_check)
    markup.add(b_date_check)
    markup.add(b_cancel)

    msg = bot.send_message(call.message.chat.id, 'Выберите нужный вариант поиска чека', reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, find_check)

def find_check(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    markup = types.ReplyKeyboardRemove()

    if message.text == 'Сумма чека':
        msg = bot.send_message(message.chat.id, 'Введите сумму чека:', reply_markup=markup)
        msgs_id.append(msg)
        bot.register_next_step_handler(message, find_sum_check)
    elif message.text == 'По дате':
        msg = bot.send_message(message.chat.id, 'Введите датe чека:\nДата должна быть [ДД.ММ.ГГГГ]', reply_markup=markup)
        msgs_id.append(msg)
        bot.register_next_step_handler(message, find_data_check)
    elif message.text == 'Отмена':
        msg = bot.send_message(message.chat.id, f'Вы отменили команду - Удалить чек')
        msgs_id.append(msg)

        # Исправления дублирования
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    else:
        msg = bot.send_message(message.chat.id, 'Неверные данные')
        msgs_id.append(msg)
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        b_sum_check = types.KeyboardButton('Сумма чека')
        b_date_check = types.KeyboardButton('По дате')
        b_cancel = types.KeyboardButton('Отмена')
        markup.add(b_sum_check)
        markup.add(b_date_check)
        markup.add(b_cancel)

        msg = bot.send_message(message.chat.id, 'Выберите нужный вариант поиска чека', reply_markup=markup)
        msgs_id.append(msg)
        bot.register_next_step_handler(message, find_check)
    user_msg.append(msgs_id)

def find_sum_check(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    isCheckTrue = any(character.isdigit() for character in message.text)
    if isCheckTrue:
        sum_check = int(message.text)

        conn = sqlite3.connect('OrdersBase.kts')
        cur = conn.cursor()
        cur.execute('SELECT * FROM orders')
        orders = cur.fetchall()
        info = ''
        for el in orders:
            if sum_check == el[2]:
                info += str(f'id заказа: {el[0]}, Сумма чека: {el[2]}, Дата заказа: {el[5]}, Время заказа: {el[6]}\n')
        if len(info) >= 4096:
            for i in range(0, len(info), 4096):
                msg = bot.send_message(message.chat.id, str(info[i:i + 4096]))
                msgs_id.append(msg)
        else:
            if info == '':
                msg = bot.send_message(message.chat.id, 'Ничего не найдено')
                msgs_id.append(msg)
            else:
                msg = bot.send_message(message.chat.id, info)
                msgs_id.append(msg)
        msg = bot.send_message(message.chat.id, 'Напишите id для удаления чека.\nНапишите Отмена для отмены')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, delete_check_id)
        cur.close()
        conn.close()
    elif message.text == 'Отмена':
        msg = bot.send_message(message.chat.id, f'Вы отменили команду - Удалить чек')
        msgs_id.append(msg)

        # Исправления дублирования
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    else:
        msg = bot.send_message(message.chat.id, 'Вы ввели некоректные данные, попробуйте ещё раз')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, find_sum_check)
    user_msg.append(msgs_id)

def find_data_check(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if len(message.text) == 10 or message.text == 'Сегодня':
        i = 0
        user_dated = message.text.split('.')
        if len(user_dated) == 3:
            for date in user_dated:
                for c in date:
                    if c == '0' or c == '1' or c == '2' or c == '3' or c == '4' or c == '5' or c == '6' or c == '7' or c == '8' or c == '9':
                        i += 1
        error = False
        if i == 8:
            # Проверка исключений
            if (int(user_dated[1]) == 2 and int(user_dated[0]) > 28) or (int(user_dated[0]) == 0) or (
                    int(user_dated[1]) == 0) or (int(user_dated[2]) == 0):
                error = True
            if (int(user_dated[1]) == 1 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 3 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 5 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 7 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 8 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 10 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 12 and int(user_dated[0]) > 31):
                error = True
            if (int(user_dated[1]) == 4 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 6 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 9 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 11 and int(user_dated[0]) > 30):
                error = True
            if int(user_dated[1]) > 12:
                error = True

        if (i == 8 and error == False) or message.text == 'Сегодня':
            if message.text == 'Сегодня':
                date = datetime.now().date()
            else:
                date = datetime.strptime(message.text, '%d.%m.%Y').date()

            conn = sqlite3.connect('OrdersBase.kts')
            cur = conn.cursor()
            cur.execute('SELECT * FROM orders')
            orders = cur.fetchall()

            info = ''
            for el in orders:
                if str(date) == str(el[5]):
                    info += str(
                        f'id заказа: {el[0]}, Сумма чека: {el[2]}, Дата заказа: {el[5]}, Время заказа: {el[6]}\n')
            if len(info) >= 4096:
                for i in range(0, len(info), 4096):
                    msg = bot.send_message(message.chat.id, str(info[i:i + 4096]))
                    msgs_id.append(msg)
            else:
                if info == '':
                    msg = bot.send_message(message.chat.id, 'Ничего не найдено')
                    msgs_id.append(msg)
                else:
                    msg = bot.send_message(message.chat.id, info)
                    msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Напишите id для удаления чека.\nНапишите Отмена для отмены')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, delete_check_id)
            cur.close()
            conn.close()
        else:
            msg = bot.send_message(message.chat.id,
                                   'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
            msgs_id.append(msg)

            bot.register_next_step_handler(message, find_data_check)
    elif message.text == 'Отмена':
        msg = bot.send_message(message.chat.id, 'Вы отменили команду - Удалить чек')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    else:
        msg = bot.send_message(message.chat.id,
                               'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, find_data_check)
    user_msg.append(msgs_id)

def delete_check_id(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    isCheckTrue = any(character.isdigit() for character in message.text)
    if isCheckTrue:
        id_check = int(message.text)

        conn2 = sqlite3.connect('ClientsBase.kts')
        cur2 = conn2.cursor()
        cur2.execute('SELECT * FROM users')
        users = cur2.fetchall()

        conn = sqlite3.connect('OrdersBase.kts')
        cur = conn.cursor()
        cur.execute('SELECT * FROM orders')
        orders = cur.fetchall()
        isFind = False
        sum_tenge = 0
        sum_bonus = 0
        _procent_my = 0.02
        _procent_ref = 0.01
        us_client = None
        my_ref = None
        for el in orders:
            if id_check == el[0]:
                sum_tenge = el[3]
                sum_bonus = el[4]
                us_client = el[1]
                isFind = True
        for el in users:
            if us_client == el[7]:
                my_ref = el[10]
                _procent_my = el[13]
        for el in users:
            if str(my_ref) == str(el[7]):
                _procent_ref = el[14]

        if isFind:
            boneses = int(sum_tenge * _procent_my)  # проценты от покупки
            bonus_ref = int(sum_tenge * _procent_ref)  # процент рефереру
            sum_ = sum_bonus + sum_tenge
            cur2.execute(f'UPDATE users SET bonus = bonus - {boneses} WHERE user_id = {us_client}')
            cur2.execute(f'UPDATE users SET bonus = bonus + {sum_bonus} WHERE user_id = {us_client}')
            cur2.execute(f'UPDATE users SET count_checks = count_checks - {1} WHERE user_id = {us_client}')
            cur2.execute(f'UPDATE users SET sum_checks = sum_checks - {sum_} WHERE user_id = {us_client}')
            if my_ref != 'None':
                cur2.execute(f'UPDATE users SET bonus = bonus - {bonus_ref} WHERE user_id = {my_ref}')
            conn2.commit()
            cur2.close()
            conn2.close()

            cur.execute(f'DELETE FROM orders WHERE id = {id_check}')
            msg = bot.send_message(message.chat.id, 'Чек был удален!')
            msgs_id.append(msg)
            conn.commit()
            cur.close()
            conn.close()

            user_states[usid] = False
            user_msg.append(msgs_id)
            return
        else:
            msg = bot.send_message(message.chat.id, 'Чек не был найден и удален!\nПопробуйте ещё раз\nДля отмены введите Отмена')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, delete_check_id)
    elif message.text == 'Отмена':
        msg = bot.send_message(message.chat.id, f'Вы отменили команду - Удалить чек')
        msgs_id.append(msg)

        # Исправления дублирования
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    else:
        msg = bot.send_message(message.chat.id, 'Вы ввели некоректные данные, попробуйте ещё раз')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, delete_check_id)
    user_msg.append(msgs_id)

def info_client(call):
    user_id = call.from_user.id
    if user_id in user_states and user_states[user_id]:
        return
    user_states[user_id] = True

    for i in user_msg:
        if i and i[0] == user_id:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(user_id)

    msg = bot.send_message(call.message.chat.id,
                           'Введите номер пользователя.\nДля отмены действия введите Отмена')
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, info_client1)

def info_client1(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.content_type == 'text':
        number = message.text
        # База данных
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        # Создание таблицы
        cur.execute('SELECT * FROM users')
        users = cur.fetchall()

        isnumber = False
        my_ref = None
        client_bonus = 0
        user = []
        for el in users:
            if number == el[2]:
                my_ref = el[10]
                client_bonus = el[8]
                isnumber = True
                user = el
        if isnumber == True:
            msg = bot.send_message(message.chat.id,
                                   f'Номер найден!')
            msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, f'ID: {user[0]}, <b>Имя:</b> {user[1]}, <b>Номер:</b> {user[2]}, <b>Пол:</b> {user[3]}, <b>Дата рождения:</b> {user[4]}, <b>Реферальный номер:</b> {user[5]}, CHAT ID: {user[6]}, USER ID: {user[7]}, <b>Количество бонусов:</b> {user[8]}, Дата регистрации: {user[9]}, ID реферера: {user[10]}, <b>Количество чеков:</b> {user[11]}, <b>Сумма выкупа:</b> {user[12]}, Процент от его заказов: {user[13]}, Процент от заказов рефералов: {user[14]}\nЕго заказы:', parse_mode='HTML')
            msgs_id.append(msg)

            conn2 = sqlite3.connect('OrdersBase.kts')
            cur2 = conn2.cursor()
            orders = cur2.execute(f"SELECT * FROM orders WHERE user_id = {user[7]}").fetchall()
            info = ''
            for el in orders:
                info += str(f'ID заказа: {el[0]}, *Сумма чека:* {el[2]}, *Сумма в тенге:* {el[3]}, *Сумма бонусами:* {el[4]}, Дата заказа: {el[5]}, Время заказа: {el[6]}\n')

            if info != '':
                send_long_message(message.chat.id, info, parse_mode="Markdown")
            else:
                msg = bot.send_message(message.chat.id, 'Ничего не найдено')
                msgs_id.append(msg)

            user_states[usid] = False

        elif message.text == 'Отмена':
            msg = bot.send_message(message.chat.id, f'Вы отменили команду - Информацию о клиенте')
            msgs_id.append(msg)

            # Исправления дублирования
            user_states[usid] = False
            user_msg.append(msgs_id)
            return
        else:
            msg = bot.send_message(message.chat.id, 'Номер не найден( и камень я не дам\nПопробуйте ещё раз')
            msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   'Введите номер пользователя.\nДля отмены действия введите Отмена')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, info_client1)
    else:
        msg = bot.send_message(message.chat.id, 'Вы ввели не номер, попробуйте ещё раз')
        msgs_id.append(msg)
        msg = bot.send_message(message.chat.id,
                               'Введите номер пользователя.\nДля отмены действия введите Отмена')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, info_client1)
    user_msg.append(msgs_id)

def end_day(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    date = datetime.now().date()

    conn = sqlite3.connect('OrdersBase.kts')
    cur = conn.cursor()
    cur.execute('SELECT * FROM orders')
    orders = cur.fetchall()

    sum_all_check = 0
    sum_bonus = 0
    sum_tenge = 0
    info = ''
    for el in orders:
        if str(date) == str(el[5]):
            info += str(
                f'id заказа: {el[0]}, Сумма чека: {el[2]}, Дата заказа: {el[5]}, Время заказа: {el[6]}\n')
            sum_all_check += el[2]
            sum_bonus += el[4]
            sum_tenge += el[3]
    if len(info) >= 4096:
        for i in range(0, len(info), 4096):
            msg = bot.send_message(call.message.chat.id, str(info[i:i + 4096]))
            msgs_id.append(msg)
    else:
        if info == '':
            msg = bot.send_message(call.message.chat.id, 'Ничего не найдено')
            msgs_id.append(msg)
        else:
            msg = bot.send_message(call.message.chat.id, info)
            msgs_id.append(msg)
    msg = bot.send_message(call.message.chat.id, f'Итог: Общая сумма: {sum_all_check}, Оплата бонусами: {sum_bonus}, Оплата в тенге: {sum_tenge}')
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    cur.close()
    conn.close()

def photo_shaurma(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    shaurma1 = open('./Menu/shaurma1.jpeg', 'rb')
    shaurma2 = open('./Menu/shaurma2.jpeg', 'rb')
    shaurma_ad = open('./Menu/shaurma_ad.jpeg', 'rb')
    msg = bot.send_photo(call.message.chat.id, shaurma1)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, shaurma2)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, shaurma_ad)
    msgs_id.append(msg)

    user_msg.append(msgs_id)

def photo_rols(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    rols_cold1 = open('./Menu/rols_cold1.jpeg', 'rb')
    rols_cold2 = open('./Menu/rols_cold2.jpeg', 'rb')
    rols_hotbox = open('./Menu/rols_hotbox.jpeg', 'rb')
    rols_infriture = open('./Menu/rols_infriture.jpeg', 'rb')
    msg = bot.send_photo(call.message.chat.id, rols_cold1)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, rols_cold2)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, rols_hotbox)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, rols_infriture)
    msgs_id.append(msg)

    user_msg.append(msgs_id)

def photo_oni(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    rols_oni = open('./Menu/rols_oni.jpeg', 'rb')
    rols_ad = open('./Menu/rols_ad.jpeg', 'rb')
    msg = bot.send_photo(call.message.chat.id, rols_oni)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, rols_ad)
    msgs_id.append(msg)

    user_msg.append(msgs_id)

def photo_pizza(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    pizza1 = open('./Menu/pizza1.jpeg', 'rb')
    pizza2 = open('./Menu/pizza2.jpeg', 'rb')
    pizza_ad = open('./Menu/pizza_ad.jpeg', 'rb')
    msg = bot.send_photo(call.message.chat.id, pizza1)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, pizza2)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, pizza_ad)
    msgs_id.append(msg)

    user_msg.append(msgs_id)

def photo_coffee(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    coffee = open('./Menu/coffee.jpeg', 'rb')
    coffe_ad = open('./Menu/coffe_ad.jpeg', 'rb')
    msg = bot.send_photo(call.message.chat.id, coffee)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, coffe_ad)
    msgs_id.append(msg)

    user_msg.append(msgs_id)

def photo_tea(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    tea = open('./Menu/tea.jpeg', 'rb')
    msg = bot.send_photo(call.message.chat.id, tea)
    msgs_id.append(msg)

    user_msg.append(msgs_id)

def photo_sets(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    sets_1 = open('./Menu/Sets/sets_1.jpg', 'rb')
    sets_2 = open('./Menu/Sets/sets_2.jpg', 'rb')
    msg = bot.send_photo(call.message.chat.id, sets_1)
    msgs_id.append(msg)
    msg = bot.send_photo(call.message.chat.id, sets_2)
    msgs_id.append(msg)

    user_msg.append(msgs_id)

# Кнопки под текстом
@bot.callback_query_handler(func=lambda callback: True)
def callback_message(call):
    user_id = call.from_user.id
    # Для пользователя
    if call.data == 'pid':
        bot.send_message(call.message.chat.id, 'Что?')
    elif call.data == 'b_reg_yes':
        ref_link_accept(call.message, call)
    elif call.data == 'b_reg_no':
        ref_link_accept(call.message, call)
    elif call.data == 'menu':
        menu(call.message)
    elif call.data == 'reg':
        register_user(call.message)
    # Действия при нажатии кнопки Бонусы в Меню
    elif call.data == 'user_bonus':
        send_bonuses_user(call)
    elif call.data == 'contacts':  # Кнопка Контакты в Меню
        send_contact_user(call)
    elif call.data == 'kitchen_menu':  # Кнопка меню в Меню
        markup = types.InlineKeyboardMarkup()
        b_sh = types.InlineKeyboardButton('🌯Шаурма🌯', callback_data='shaurma')
        b_rols = types.InlineKeyboardButton('🍣Роллы🍣', callback_data='rols')
        b_oni = types.InlineKeyboardButton('🍙Онигири, Суши и Гунканы🍙', callback_data='oni')
        b_pizza = types.InlineKeyboardButton('🍕Пицца🍕', callback_data='pizza')
        b_coffee = types.InlineKeyboardButton('☕️Кофе☕️', callback_data='coffee')
        b_tea = types.InlineKeyboardButton('🍵Чай🍵', callback_data='tea')
        b_sets = types.InlineKeyboardButton('🍱Сеты🍱', callback_data='sets')
        # b_sm = types.InlineKeyboardButton('🍕🌯🍣🍙☕️🍵🍱', callback_data='sm')

        b_back = types.InlineKeyboardButton('🔙Назад🔙', callback_data='back_menu')

        markup.add(b_sh)
        markup.add(b_rols)
        markup.add(b_oni)
        markup.add(b_pizza)
        markup.add(b_coffee, b_tea)
        markup.add(b_sets)

        markup.add(b_back)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'shaurma':
        photo_shaurma(call)
    elif call.data == 'rols':
        photo_rols(call)
    elif call.data == 'oni':
        photo_oni(call)
    elif call.data == 'pizza':
        photo_pizza(call)
    elif call.data == 'coffee':
        photo_coffee(call)
    elif call.data == 'tea':
        photo_tea(call)
    elif call.data == 'sets':
        photo_sets(call)
    elif call.data == 'my_ref':  # Мои рефералы в Меню
        my_ref(call)
    elif call.data == 'send_request':  # Отправить отзыв в Меню
        send_request_user(call)
    elif call.data == 'edit_profile':  # Редактировать профиль в Меню
        markup = types.InlineKeyboardMarkup()
        b_name = types.InlineKeyboardButton('✏️Изменить имя✏️', callback_data='rename')
        b_date_born = types.InlineKeyboardButton('✏️Изменить дату рождения✏️', callback_data='redate_born')
        b_sex = types.InlineKeyboardButton('✏️Изменить пол✏️', callback_data='resex')
        b_info = types.InlineKeyboardButton('Обо мне', callback_data='info')
        b_back = types.InlineKeyboardButton('🔙Назад🔙', callback_data='back_menu')
        markup.add(b_name)
        markup.add(b_date_born, b_sex)
        markup.add(b_info)
        markup.add(b_back)
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'back_menu':
        markup = types.InlineKeyboardMarkup()
        btn_bon = types.InlineKeyboardButton('🎁Бонусы🎁', callback_data='user_bonus')
        btn_cont = types.InlineKeyboardButton('📞Контакты📞', callback_data='contacts')
        btn_inst = types.InlineKeyboardButton('📸Инстаграм📸',
                                              url='https://www.instagram.com/kitsune.shemonaiha?igsh=NmpyZW9rcnVhMTUx')
        btn_menu = types.InlineKeyboardButton('📱Меню📱', callback_data='kitchen_menu')
        btn_ref = types.InlineKeyboardButton('💰Мои рефералы💰', callback_data='my_ref')
        btn_ans = types.InlineKeyboardButton('✏️Оставить отзыв✏️', callback_data='send_request')
        btn_edit_user = types.InlineKeyboardButton('📋Изменить профиль📋', callback_data='edit_profile')
        btn_queations = types.InlineKeyboardButton('❓Часто задаваемые вопросы❓', callback_data='queations')
        markup.add(btn_bon)
        markup.add(btn_cont, btn_inst)
        markup.add(btn_menu)
        markup.add(btn_ref)
        markup.add(btn_ans)
        markup.add(btn_edit_user)
        markup.add(btn_queations)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'queations':
        markup = types.InlineKeyboardMarkup()
        b_work_ref = types.InlineKeyboardButton('🎁Как работает система приглашений🎁', callback_data='ref_w')
        b_bonus = types.InlineKeyboardButton('💰Как работают бонусы💰', callback_data='bonus_w')
        b_req = types.InlineKeyboardButton('✏️Отзывы✏️', callback_data='req_w')
        b_profile = types.InlineKeyboardButton('📋Мой профиль📋', callback_data='profile_w')
        b_back = types.InlineKeyboardButton('🔙Назад🔙', callback_data='back_menu')
        markup.add(b_work_ref)
        markup.add(b_bonus)
        markup.add(b_req)
        markup.add(b_profile)
        markup.add(b_back)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'ref_w':
        usid = call.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    try:
                        bot.delete_message(call.message.chat.id, m.message_id)
                    except Exception as e:
                        print(f'Ошибка удаления сообщения: {e}')
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msg = bot.send_message(call.message.chat.id, 'Как работает система приглашений?')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        msg = bot.send_message(call.message.chat.id,
                               'Вы приглашаете друга и при регистрации он должен ввести ваш реферальный номер. Узнать какой ваш реферальный номер, вы можете в меню во вкладке "Бонусы" или "Мои рефералы".\nТакже вы получаете бонусы за заказы вашего друга.')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
    elif call.data == 'bonus_w':
        usid = call.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    try:
                        bot.delete_message(call.message.chat.id, m.message_id)
                    except Exception as e:
                        print(f'Ошибка удаления сообщения: {e}')
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msg = bot.send_message(call.message.chat.id, 'Как работают бонусы?')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        msg = bot.send_message(call.message.chat.id,
                               'После каждого заказа вы получаете бонусы. Также эти бонусы можно и потратить.\n1 Бонус = 1 Тенге\nКак их тратить? Когда вы делаете заказ, вам сообщат сколько у вас бонусов и сколько вы хотите потратить. Также бонусы могут начисляться при приглашении друга, как вам, так и другу.\nПосмотреть сколько у вас сейчас бонусов можно в меню во вкладке "Бонусы".\nПри достижении определенных сумм выкупа, ваш процент зачисления бонусов будет увеличен.')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
    elif call.data == 'req_w':
        usid = call.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    try:
                        bot.delete_message(call.message.chat.id, m.message_id)
                    except Exception as e:
                        print(f'Ошибка удаления сообщения: {e}')
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msg = bot.send_message(call.message.chat.id, 'Как работают отзывы?')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        msg = bot.send_message(call.message.chat.id,
                               'Вы можете оставить свой отзыв или оставить сообщение. В меню есть кнопка "✏️Оставить отзыв✏️". Жмете на неё и пишите сообщение. Можете оставлять свой отзыв и задать вопросы. Задав вопрос, в близжайшем времени вам ответят. Можно отправлять и фото, и видео, и даже кругляшки! Ваш отзыв будет обязательно прочитан)')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
    elif call.data == 'profile_w':
        usid = call.from_user.id
        for i in user_msg:
            if i[0] == usid:
                i.pop(0)
                for m in i:
                    try:
                        bot.delete_message(call.message.chat.id, m.message_id)
                    except Exception as e:
                        print(f'Ошибка удаления сообщения: {e}')
                user_msg.remove(i)
        msgs_id = []
        msgs_id.append(usid)
        msg = bot.send_message(call.message.chat.id,
                               'Во вкладке "📋Изменить профиль📋", вы можете посмотреть информацию о своем профиле. Так же вы можете редактировать данные.\nВо вкладке "📞Контакты📞" находятся контакты нашего заведения. Вы можете позвонить и сделать заказ, а после заказать доставку или забрать самовывозом.')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
    elif call.data == 'redate_born':
        red(call)
    elif call.data == 'rename':
        ren(call)
    elif call.data == 'resex':
        res(call)
    elif call.data == 'info':
        info_user(call)
    elif call.data == 'new_check':
        new_check(call)
    elif call.data == 'del_check':
        del_check(call)
    elif call.data == 'info_client':
        info_client(call)
    elif call.data == 'end_day':
        end_day(call)
    elif call.data == 'anal_back':
        markup = types.InlineKeyboardMarkup()
        b_anal = types.InlineKeyboardButton('Аналитика', callback_data='anal')
        b_sm = types.InlineKeyboardButton('Рассылка', callback_data='sm')
        b_prv = types.InlineKeyboardButton('Правки', callback_data='prv')
        b_rquests = types.InlineKeyboardButton('Результатыты опросов', callback_data='rquests')

        markup.add(b_anal)
        markup.add(b_sm)
        markup.add(b_prv)
        markup.add(b_rquests)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'anal':
        markup = types.InlineKeyboardMarkup()
        b_sum_checks = types.InlineKeyboardButton('Топ по сумме выкупа', callback_data='top_sum_check')
        b_count_ref = types.InlineKeyboardButton('Топ по кол-ву рефералов', callback_data='top_count_ref')
        b_count_bonus = types.InlineKeyboardButton('Топ по кол-ву бонусов', callback_data='top_count_bonus')
        b_sum_average = types.InlineKeyboardButton('Топ по сумме среднего чека', callback_data='top_sum_average')
        b_count_checks = types.InlineKeyboardButton('Топ по кол-ву заказов', callback_data='top_count_checks')
        b_sum_from_male = types.InlineKeyboardButton('Топ по сумме заказов среди Муж',
                                                     callback_data='top_sum_check_male')
        b_sum_from_female = types.InlineKeyboardButton('Топ по сумме заказов среди Жен',
                                                       callback_data='top_sum_check_female')
        b_count_old = types.InlineKeyboardButton('Кол-во клиентов по возрасту', callback_data='top_count_old')
        b_sum_in_old = types.InlineKeyboardButton('Топ сум по возрасту', callback_data='top_sum_in_old')

        b_back = types.InlineKeyboardButton('Назад', callback_data='anal_back')

        markup.add(b_sum_checks)
        markup.add(b_count_ref)
        markup.add(b_count_bonus)
        markup.add(b_sum_average)
        markup.add(b_count_checks)
        markup.add(b_sum_from_male)
        markup.add(b_sum_from_female)
        markup.add(b_count_old)
        markup.add(b_sum_in_old)
        markup.add(b_back)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'top_sum_check':
        date_settings(call, 1)
    elif call.data == 'top_count_ref':
        date_settings(call, 2)
    elif call.data == 'top_count_bonus':
        date_settings(call, 3)
    elif call.data == 'top_sum_average':
        date_settings(call, 4)
    elif call.data == 'top_count_checks':
        date_settings(call, 5)
    elif call.data == 'top_sum_check_male':
        date_settings(call, 6)
    elif call.data == 'top_sum_check_female':
        date_settings(call, 7)
    elif call.data == 'top_count_old':
        date_settings(call, 8)
    elif call.data == 'top_sum_in_old':
        markup = types.InlineKeyboardMarkup()
        b_date_10 = types.InlineKeyboardButton('От 6 до 10', callback_data='sum_date_10')
        b_date_15 = types.InlineKeyboardButton('От 11 до 15', callback_data='sum_date_15')
        b_date_20 = types.InlineKeyboardButton('От 16 до 20', callback_data='sum_date_20')
        b_date_25 = types.InlineKeyboardButton('От 21 до 25', callback_data='sum_date_25')
        b_date_30 = types.InlineKeyboardButton('От 26 до 30', callback_data='sum_date_30')
        b_date_40 = types.InlineKeyboardButton('От 31 до 40', callback_data='sum_date_40')
        b_date_45 = types.InlineKeyboardButton('От 41 и больше', callback_data='sum_date_45')

        b_back = types.InlineKeyboardButton('Назад', callback_data='anal')

        markup.add(b_date_10)
        markup.add(b_date_15)
        markup.add(b_date_20)
        markup.add(b_date_25)
        markup.add(b_date_30)
        markup.add(b_date_40)
        markup.add(b_date_45)
        markup.add(b_back)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'sum_date_10':
        sum_in_old(call, 10)
    elif call.data == 'sum_date_15':
        sum_in_old(call, 15)
    elif call.data == 'sum_date_20':
        sum_in_old(call, 20)
    elif call.data == 'sum_date_25':
        sum_in_old(call, 25)
    elif call.data == 'sum_date_30':
        sum_in_old(call, 30)
    elif call.data == 'sum_date_40':
        sum_in_old(call, 40)
    elif call.data == 'sum_date_45':
        sum_in_old(call, 45)
    elif call.data == 'sm':
        markup = types.InlineKeyboardMarkup()
        b_sm_all = types.InlineKeyboardButton('Разослать ВСЕМ', callback_data='sm_all')
        b_sm_person = types.InlineKeyboardButton('Персональная', callback_data='sm_person')
        b_sm_groups = types.InlineKeyboardButton('По группам', callback_data='sm_groups')

        b_back = types.InlineKeyboardButton('Назад', callback_data='anal_back')

        markup.add(b_sm_all)
        markup.add(b_sm_person)
        markup.add(b_sm_groups)
        markup.add(b_back)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'sm_all':
        sm_all(call)
    elif call.data == 'sm_person':
        sm_person(call)
    elif call.data == 'sm_groups':
        sm_groups(call)
    elif call.data == 'prv':
        markup = types.InlineKeyboardMarkup()
        b_sm_all = types.InlineKeyboardButton('Изменить бонусы', callback_data='prv1')
        b_sm_person = types.InlineKeyboardButton('Изменить процент для рефералов', callback_data='prv2')

        b_back = types.InlineKeyboardButton('Назад', callback_data='anal_back')

        markup.add(b_sm_all)
        markup.add(b_sm_person)
        markup.add(b_back)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'prv1':
        prv(call)
    elif call.data == 'prv2':
        procent_f(call)
    elif call.data == 'rquests':
        markup = types.InlineKeyboardMarkup()

        b_all = types.InlineKeyboardButton('Все опросы', callback_data='poll_all')
        b_current = types.InlineKeyboardButton('Поиск по названию', callback_data='poll_current')
        b_back = types.InlineKeyboardButton('Назад', callback_data='anal_back')

        markup.add(b_all)
        markup.add(b_current)
        markup.add(b_back)

        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
    elif call.data == 'poll_all':
        show_results(call)
    elif call.data == 'poll_current':
        search_poll(call)
    else:  # Обработка голосования
        if '_' in call.data:
            poll_id, vote = call.data.split('_')

            # Проверка, голосовал ли пользователь
            _cur.execute("SELECT * FROM user_votes WHERE user_id = ? AND poll_id = ?", (user_id, poll_id))
            if _cur.fetchone():
                bot.answer_callback_query(call.id, "Вы уже проголосовали в этом опросе.")
            else:
                _cur.execute("INSERT INTO user_votes (user_id, poll_id, vote) VALUES (?, ?, ?)",
                             (user_id, poll_id, vote))
                _cur.execute("UPDATE options SET votes = votes + 1 WHERE poll_id = ? AND option = ?",
                             (poll_id, vote))
                _conn.commit()
                bot.answer_callback_query(call.id, f"Вы проголосовали: {vote}")

                # Обновить сообщение, чтобы пользователь не мог проголосовать снова
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)

# Результаты опросов --------------------------------------------------------------------------------------------
def show_results(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    chat_id = call.message.chat.id
    results_message = "Результаты опросов:\n"
    _cur.execute("SELECT poll_id, question FROM polls")
    polls = _cur.fetchall()
    for poll_id, question in polls:
        results_message += f"\nОпрос: {question}\n"
        _cur.execute("SELECT option, votes FROM options WHERE poll_id = ?", (poll_id,))
        options = _cur.fetchall()
        for option, count in options:
            results_message += f"{option}: {count} голосов\n"

    if len(results_message) >= 4096:
        for i in range(0, len(results_message), 4096):
            msg = bot.send_message(chat_id, str(results_message[i:i + 4096]))
            msgs_id.append(msg)
        user_msg.append(msgs_id)
    else:
        msg = bot.send_message(chat_id, results_message)
        msgs_id.append(msg)
        user_msg.append(msgs_id)

def search_poll(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    msg = bot.send_message(call.message.chat.id, "Введите название опроса для поиска:")
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(msg, process_search_query)

def process_search_query(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    chat_id = message.chat.id
    query = message.text
    _cur.execute("SELECT poll_id, question FROM polls WHERE question LIKE ?", ('%' + query + '%',))
    polls = _cur.fetchall()

    if polls:
        results_message = "Найденные опросы:\n"
        for poll_id, question in polls:
            results_message += f"\nОпрос: {question}\n"
            _cur.execute("SELECT option, votes FROM options WHERE poll_id = ?", (poll_id,))
            options = _cur.fetchall()
            for option, count in options:
                results_message += f"{option}: {count} голосов\n"
        if len(results_message) >= 4096:
            for i in range(0, len(results_message), 4096):
                msg = bot.send_message(chat_id, str(results_message[i:i + 4096]))
                msgs_id.append(msg)
            user_msg.append(msgs_id)
        else:
            msg = bot.send_message(chat_id, results_message)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
    else:
        bot.send_message(chat_id, "Опросов с таким названием не найдено.")
    user_states[usid] = False

# ----------------------------------------------------------------------------------------------------------------

@bot.message_handler(commands=['admin'])
def admin_panel(message):
    chat_id = None
    for ad in admins:
        if message.chat.id == ad:
            chat_id = ad
    if chat_id != None:
        markup = types.InlineKeyboardMarkup()
        b_anal = types.InlineKeyboardButton('АНАЛитика', callback_data='anal')
        b_sm = types.InlineKeyboardButton('Рас-Три-юлять(Рассылка)', callback_data='sm')
        b_prv = types.InlineKeyboardButton('сПравки', callback_data='prv')
        b_rquests = types.InlineKeyboardButton('РезУЛЬТАты опросов', callback_data='rquests')

        markup.add(b_anal)
        markup.add(b_sm)
        markup.add(b_prv)
        markup.add(b_rquests)

        bot.send_message(chat_id, 'Вы вошли в админскую консоль.\nЧто вас интерисует?', reply_markup=markup)
    else:
        bot.send_message(message.chat.id, f'Ваш код {message.chat.id}')

def procent_f(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    msg = bot.send_message(call.message.chat.id, 'Введите номер клиента\n(без + и первая цифра должна быть 7)')
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, procent_f1)

def procent_f1(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if message.text == 'Отмена':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    isCheckTrue = any(character.isdigit() for character in message.text)
    if isCheckTrue:
        user_number = message.text
        conn2 = sqlite3.connect('ClientsBase.kts')
        cur2 = conn2.cursor()
        cur2.execute('SELECT * FROM users')
        users = cur2.fetchall()
        isFind = False
        chat_client = None
        procent_ref = None
        name = None
        for el in users:
            if user_number == el[2]:
                chat_client = el[6]
                name = el[1]
                procent_ref = el[14]
                isFind = True
        cur2.close()
        conn2.close()
        if isFind:
            msg = bot.send_message(message.chat.id,
                                   f'Пользователь [{name} - {user_number}] был найден. Его процент от рефералов - {procent_ref}. Введите значение (Например: 0.01) на которое желаете заменить.')
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, procent_f2, chat_client)
        else:
            msg = bot.send_message(message.chat.id, 'Пользователь не был найден. Команда была отменена!')
            msgs_id.append(msg)
            user_states[usid] = False
            user_msg.append(msgs_id)
    else:
        msg = bot.send_message(message.chat.id, 'Команда была отменена!')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

def procent_f2(message, chat_us):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if message.text == 'Отмена':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

    isCheckTrue = any(character.isdigit() for character in message.text)
    if isinstance(message.text, float):
        isCheckTrue = True
    if isCheckTrue:
        _procent_ref = float(message.text)
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        cur.execute(f'UPDATE users SET procent_ref = {_procent_ref} WHERE chat_id = {chat_us}')
        conn.commit()
        user = cur.execute(f"SELECT * FROM users WHERE chat_id = {chat_us}").fetchone()
        msg = bot.send_message(message.chat.id, f'Реферальный процент был изменен на {_procent_ref} у [{user[1]} - {user[2]}]')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        user_states[usid] = False
        cur.close()
        conn.close()
    else:
        msg = bot.send_message(message.chat.id, 'Команда была отменена!')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

def prv(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    msg = bot.send_message(call.message.chat.id, 'Введите номер клиента\n(без + и первая цифра должна быть 7)')
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, prv1)

def prv1(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if message.text == 'Отмена':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    isCheckTrue = any(character.isdigit() for character in message.text)
    if isCheckTrue:
        user_number = message.text
        conn2 = sqlite3.connect('ClientsBase.kts')
        cur2 = conn2.cursor()
        cur2.execute('SELECT * FROM users')
        users = cur2.fetchall()
        isFind = False
        chat_client = None
        bonus = None
        name = None
        for el in users:
            if user_number == el[2]:
                chat_client = el[6]
                name = el[1]
                bonus = el[8]
                isFind = True
        cur2.close()
        conn2.close()
        if isFind:
            msg = bot.send_message(message.chat.id, f'Пользователь [{name} - {user_number}] был найден. У него {bonus} бонусов. Введите количество бонусов на которое желаете заменить.')
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, prv2, chat_client)
        else:
            msg = bot.send_message(message.chat.id, 'Пользователь не был найден. Команда была отменена!')
            msgs_id.append(msg)
            user_states[usid] = False
            user_msg.append(msgs_id)
    else:
        msg = bot.send_message(message.chat.id, 'Команда была отменена!')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

def prv2(message, chat_us):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if message.text == 'Отмена':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

    isCheckTrue = any(character.isdigit() for character in message.text)
    if isCheckTrue:
        bonus = int(message.text)
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        cur.execute(f'UPDATE users SET bonus = {bonus} WHERE chat_id = {chat_us}')
        conn.commit()
        user = cur.execute(f"SELECT * FROM users WHERE chat_id = {chat_us}").fetchone()
        msg = bot.send_message(message.chat.id, f'Бонусы были изменены на {bonus} у [{user[1]} - {user[2]}]')
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        user_states[usid] = False
        cur.close()
        conn.close()
    else:
        msg = bot.send_message(message.chat.id, 'Команда была отменена!')
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

# Рассылка всем
def sm_all(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

    b_cancel = types.KeyboardButton(text='Отмена')
    markup.row(b_cancel)

    msg = bot.send_message(call.message.chat.id, 'Введите сообщение которое хотите разослать всем', reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, sm_all_1)

def sm_all_1(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()

    cur.execute('SELECT * FROM users')
    users = cur.fetchall()

    markup = types.ReplyKeyboardRemove()

    if message.text == 'Отмена' or message.text == '/menu':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    if message.text == 'Опрос':

        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        msg = bot.send_message(message.chat.id, 'Введите вопрос для опроса:', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(msg, poll, users)
        cur.close()
        conn.close()
        return

    for el in users:
        if int(el[6]) != message.chat.id:
            if message.content_type == 'text':
                bot.send_message(int(el[6]), message.text)
            elif message.content_type == 'photo':
                raw = message.photo[2].file_id
                name = raw + '.jpg'
                file_info = bot.get_file(raw)
                downloaded_file = bot.download_file(file_info.file_path)
                with open(f'./Temp/{name}', "wb") as new_file:
                    new_file.write(downloaded_file)
                img = open(f'./Temp/{name}', 'rb')
                if message.caption != 'None':
                    bot.send_photo(int(el[6]), img, caption=message.caption)
                else:
                    bot.send_photo(int(el[6]), img)
            elif message.content_type == 'video':
                raw = message.video.file_id
                name = raw + '.mp4'
                file_info = bot.get_file(raw)
                downloaded_file = bot.download_file(file_info.file_path)
                with open(f'./Temp/{name}', "wb") as new_file:
                    new_file.write(downloaded_file)
                img = open(f'./Temp/{name}', 'rb')
                if message.caption != 'None':
                    bot.send_video(int(el[6]), img, caption=message.caption)
                else:
                    bot.send_video(int(el[6]), img)
    cur.close()
    conn.close()
    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

    b_cancel = types.KeyboardButton(text='Отмена')
    markup.row(b_cancel)
    msg = bot.send_message(message.chat.id, 'Сообщение было успешно отправлено!\nВведите новое сообщение сообщение:',
                           reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(message, sm_all_1)

# СИСТЕМА ОПРОСОВ ------------------------------------------------------------------------------------------------
def poll(message, users):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    markup = types.ReplyKeyboardRemove()

    if message.text != 'None' and message.text != 'Отмена':
        chat_id = message.chat.id
        question = message.caption if message.content_type == 'photo' else message.text
        photo = None
        if message.content_type == 'photo':
            raw = message.photo[-1].file_id
            photo = raw + '.jpg'
            file_info = bot.get_file(raw)
            downloaded_file = bot.download_file(file_info.file_path)
            os.makedirs('./Temp', exist_ok=True)
            with open(f'./Temp/{photo}', "wb") as new_file:
                new_file.write(downloaded_file)
        current_polls[chat_id] = {'question': question, 'photo': photo}

        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        msg = bot.reply_to(message, "Введите количество кнопок:", reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)

        bot.register_next_step_handler(msg, process_button_count_step, users)
    elif message.text == 'Отмена' or message.text == '/menu':
        msg = bot.send_message(message.chat.id, f'Команда была отменена', reply_markup=markup)
        msgs_id.append(msg)

        # Исправления дублирования
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    else:
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        msg = bot.send_message(message.chat.id, f'Вы ввели некоректные данные, попробуйте ещё раз', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)

        bot.register_next_step_handler(message, poll, users)

def process_button_count_step(message, users):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    markup = types.ReplyKeyboardRemove()

    if message.text != 'None' and message.text != 'Отмена':
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        isCheckTrue = any(character.isdigit() for character in message.text)
        if isCheckTrue:
            chat_id = message.chat.id
            button_count = int(message.text)
            current_polls[chat_id]['button_count'] = button_count
            msg = bot.reply_to(message, 'Введите названия кнопок через "Новую строку":', reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(msg, process_button_names_step, users)
        else:
            msg = bot.send_message(message.chat.id, 'Вы ввели некоректные данные, попробуйте ещё раз', reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, process_button_count_step, users)
    elif message.text == 'Отмена' or message.text == '/menu':
        msg = bot.send_message(message.chat.id, f'Команда была отменена', reply_markup=markup)
        msgs_id.append(msg)

        # Исправления дублирования
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    else:
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        msg = bot.send_message(message.chat.id, 'Вы ввели некоректные данные, попробуйте ещё раз', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, process_button_count_step, users)

def process_button_names_step(message, users):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    markup = types.ReplyKeyboardRemove()

    chat_id = message.chat.id
    if message.text == 'Отмена' or message.text == '/menu':
        del current_polls[chat_id]

        msg = bot.send_message(message.chat.id, f'Команда была отменена', reply_markup=markup)
        msgs_id.append(msg)

        # Исправления дублирования
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    elif message.content_type == 'text':
        button_names = [name.strip() for name in message.text.split('\n')]
        if len(button_names) != current_polls[chat_id]['button_count']:
            markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

            b_cancel = types.KeyboardButton(text='Отмена')
            markup.row(b_cancel)

            bot.reply_to(message,
                         f"Вы ввели {len(button_names)} названий кнопок вместо {current_polls[chat_id]['button_count']}. Попробуйте снова.")
            msg = bot.reply_to(message,
                               f'Введите названия {current_polls[chat_id]["button_count"]} кнопок через "Новую строку":', reply_markup=markup)
            bot.register_next_step_handler(msg, process_button_names_step, users)
            return

        current_polls[chat_id]['button_names'] = button_names
        question = current_polls[chat_id]['question']
        photo = current_polls[chat_id]['photo']
        poll_id = str(message.message_id)  # Уникальный ID опроса, преобразуем в строку
        create_poll_message(question, button_names, poll_id, users, photo)

        msg = bot.send_message(message.chat.id, 'Опрос был создан и отправлен!', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)

        user_states[usid] = False
        del current_polls[chat_id]
    else:
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        msg = bot.send_message(message.chat.id, 'Вы ввели некоректные данные, попробуйте ещё раз', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, process_button_names_step, users)

# Создает опрос и автоматически его рассылает!
def create_poll_message(question, options, poll_id, users, photo):
    markup = types.InlineKeyboardMarkup()
    for option in options:
        markup.add(types.InlineKeyboardButton(text=option, callback_data=f'{poll_id}_{option}'))
    # Отправка опросника
    for el in users:
        if photo:
            bot.send_photo(el[6], photo=open(f'./Temp/{photo}', 'rb'), caption=question, reply_markup=markup)
        else:
            bot.send_message(el[6], question, reply_markup=markup)

    # Сохранение опроса и опций в базу данных
    _cur.execute("INSERT INTO polls (poll_id, question) VALUES (?, ?)", (poll_id, question))
    for option in options:
        _cur.execute("INSERT INTO options (poll_id, option, votes) VALUES (?, ?, 0)", (poll_id, option))
    _conn.commit()
# ----------------------------------------------------------------------------------------------------------------

def sm_person(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

    b_cancel = types.KeyboardButton(text='Отмена')
    markup.row(b_cancel)

    msg = bot.send_message(call.message.chat.id, 'Введите его номер телефона (без +)', reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, sm_person1)

def sm_person1(message):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    markup = types.ReplyKeyboardRemove()

    if message.text == 'Отмена' or message.text == '/menu':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    isCheckTrue = any(character.isdigit() for character in message.text)
    if isCheckTrue:
        user_number = message.text
        conn2 = sqlite3.connect('ClientsBase.kts')
        cur2 = conn2.cursor()
        cur2.execute('SELECT * FROM users')
        users = cur2.fetchall()
        isFind = False
        chat_client = None
        for el in users:
            if user_number == el[2]:
                chat_client = el[6]
                isFind = True
        cur2.close()
        conn2.close()
        if isFind:
            markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

            b_cancel = types.KeyboardButton(text='Отмена')
            markup.row(b_cancel)

            msg = bot.send_message(message.chat.id, 'Пользователь был найден введите сообщение.', reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, sm_person2, chat_client)
        else:
            msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
            msgs_id.append(msg)
            user_states[usid] = False
            user_msg.append(msgs_id)

def sm_person2(message, chat_id):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if message.content_type == 'text':
        if message.text == 'Отмена' or message.text == '/menu':
            markup = types.ReplyKeyboardRemove()
            msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
            msgs_id.append(msg)
            user_states[usid] = False
            user_msg.append(msgs_id)
            return
        if message.text == 'Опрос':
            users = [[0,0,0,0,0,0,chat_id]]

            markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

            b_cancel = types.KeyboardButton(text='Отмена')
            markup.row(b_cancel)

            msg = bot.send_message(message.chat.id, 'Введите вопрос для опроса:', reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(msg, poll, users)
            return
        bot.send_message(chat_id, message.text)
    elif message.content_type == 'photo':
        raw = message.photo[2].file_id
        name = raw + '.jpg'
        file_info = bot.get_file(raw)
        downloaded_file = bot.download_file(file_info.file_path)
        with open(f'./Temp/{name}', "wb") as new_file:
            new_file.write(downloaded_file)
        img = open(f'./Temp/{name}', 'rb')
        if message.caption != 'None':
            bot.send_photo(chat_id, img, caption=message.caption)
        else:
            bot.send_photo(chat_id, img)
    elif message.content_type == 'video':
        raw = message.video.file_id
        name = raw + '.mp4'
        file_info = bot.get_file(raw)
        downloaded_file = bot.download_file(file_info.file_path)
        with open(f'./Temp/{name}', "wb") as new_file:
            new_file.write(downloaded_file)
        img = open(f'./Temp/{name}', 'rb')
        if message.caption != 'None':
            bot.send_video(chat_id, img, caption=message.caption)
        else:
            bot.send_video(chat_id, img)
    elif message.content_type == 'poll':
        mess = bot.copy_message(message.chat.id, message.chat.id, message.message_id)
        bot.forward_message(chat_id, message.chat.id, mess.message_id)
    else:
        bot.send_message(message.chat.id, 'Сообщение не было отправлено!')
    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

    b_cancel = types.KeyboardButton(text='Отмена')
    markup.row(b_cancel)
    msg = bot.send_message(message.chat.id, 'Сообщение было успешно отправлено!\nВведите новое сообщение сообщение:', reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(message, sm_person2, chat_id)

def sm_groups(call):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

    b_cancel = types.KeyboardButton(text='Отмена')
    markup.row(b_cancel)

    msg = bot.send_message(call.message.chat.id, 'Выбор возраста.\nВведите от какого возраста начать рассылку.\nЧто выбрать всех введите: Все', reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, sm_groups1)

def sm_groups1(message):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)

    markup = types.ReplyKeyboardRemove()

    isCheckTrue = any(character.isdigit() for character in message.text)
    if message.text == 'Все':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        b_a = types.KeyboardButton('Все')
        b_m = types.KeyboardButton('Муж')
        b_j = types.KeyboardButton('Жен')
        markup.add(b_a)
        markup.add(b_m)
        markup.add(b_j)
        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        msg = bot.send_message(message.chat.id,
                              'Выберите пол.', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, sm_groups3)
    elif message.text == 'Отмена' or message.text == '/menu':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    elif isCheckTrue:
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        yaer_from = int(message.text)
        msg = bot.send_message(message.chat.id,
                              'Введите до какого возраста начать рассылку.', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, sm_groups2,yaer_from)
    else:
        msg = bot.send_message(message.chat.id, 'Произошла ошибка!\nКоманда была отменена(', reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

def sm_groups2(message, yaer_from):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)

    markup = types.ReplyKeyboardRemove()

    isCheckTrue = any(character.isdigit() for character in message.text)
    if message.text == 'Отмена' or message.text == '/menu':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    elif isCheckTrue:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        b_a = types.KeyboardButton('Все')
        b_m = types.KeyboardButton('Муж')
        b_j = types.KeyboardButton('Жен')
        markup.add(b_a)
        markup.add(b_m)
        markup.add(b_j)
        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        yaer_to = int(message.text)
        msg = bot.send_message(message.chat.id,
                              'Выберите пол.', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, sm_groups3, yaer_from, yaer_to)
    else:
        msg = bot.send_message(message.chat.id, 'Произошла ошибка!\nКоманда была отменена(', reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

def sm_groups3(message, year_from = 0, year_to = 0):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)
    markup = types.ReplyKeyboardRemove()
    if message.text == 'Отмена' or message.text == '/menu':
        msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return
    elif message.text == 'Все':
        mmm = message.text

        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        msg = bot.send_message(message.chat.id, 'Введите сообщение которое хотите разослать.', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, sm_groups4, year_from, year_to, mmm)
    elif message.text == 'Муж':
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        mmm = message.text
        msg = bot.send_message(message.chat.id, 'Введите сообщение которое хотите разослать.', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, sm_groups4, year_from, year_to, mmm)
    elif message.text == 'Жен':
        markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

        b_cancel = types.KeyboardButton(text='Отмена')
        markup.row(b_cancel)

        mmm = message.text
        msg = bot.send_message(message.chat.id, 'Введите сообщение которое хотите разослать.', reply_markup=markup)
        msgs_id.append(msg)
        user_msg.append(msgs_id)
        bot.register_next_step_handler(message, sm_groups4, year_from, year_to, mmm)
    else:
        msg = bot.send_message(message.chat.id, 'Произошла ошибка!\nКоманда была отменена(', reply_markup=markup)
        msgs_id.append(msg)
        user_states[usid] = False
        user_msg.append(msgs_id)
        return

def sm_groups4(message,year_from, year_to, mmm):
    usid = message.from_user.id
    msgs_id = []
    msgs_id.append(usid)

    markup = types.ReplyKeyboardRemove()

    if mmm == 'Все':
        if year_from == 0 and year_to == 0:
            if message.text == 'Отмена' or message.text == '/menu':
                msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
                msgs_id.append(msg)
                user_states[usid] = False
                user_msg.append(msgs_id)
                return
            if message.text == 'Опрос':
                conn = sqlite3.connect('ClientsBase.kts')
                cur = conn.cursor()

                cur.execute('SELECT * FROM users')
                users = cur.fetchall()

                markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

                b_cancel = types.KeyboardButton(text='Отмена')
                markup.row(b_cancel)

                msg = bot.send_message(message.chat.id, 'Введите вопрос для опроса:', reply_markup=markup)
                msgs_id.append(msg)
                user_msg.append(msgs_id)
                bot.register_next_step_handler(msg, poll, users)
                cur.close()
                conn.close()
                return

            conn = sqlite3.connect('ClientsBase.kts')
            cur = conn.cursor()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            mess = None
            if message.content_type == 'poll':
                mess = bot.copy_message(message.chat.id, message.chat.id, message.message_id)
            for el in users:
                if int(el[6]) != message.chat.id:
                    if message.content_type == 'text':

                        bot.send_message(el[6], message.text)

                    elif message.content_type == 'photo':
                        raw = message.photo[2].file_id
                        name = raw + '.jpg'
                        file_info = bot.get_file(raw)
                        downloaded_file = bot.download_file(file_info.file_path)
                        with open(f'./Temp/{name}', "wb") as new_file:
                            new_file.write(downloaded_file)
                        img = open(f'./Temp/{name}', 'rb')
                        bot.send_photo(el[6], img)
                        if message.caption != 'None':
                            bot.send_photo(el[6], img, caption=message.caption)
                        else:
                            bot.send_photo(el[6], img)
                    elif message.content_type == 'video':
                        raw = message.video.file_id
                        name = raw + '.mp4'
                        file_info = bot.get_file(raw)
                        downloaded_file = bot.download_file(file_info.file_path)
                        with open(f'./Temp/{name}', "wb") as new_file:
                            new_file.write(downloaded_file)
                        img = open(f'./Temp/{name}', 'rb')
                        if message.caption != 'None':
                            bot.send_video(el[6], img, caption=message.caption)
                        else:
                            bot.send_video(el[6], img)
                    elif message.content_type == 'poll':
                        bot.forward_message(el[6], message.chat.id, mess.message_id)
            cur.close()
            conn.close()
            markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

            b_cancel = types.KeyboardButton(text='Отмена')
            markup.row(b_cancel)
            msg = bot.send_message(message.chat.id,
                                   'Сообщение было успешно отправлено!\nВведите новое сообщение сообщение:',
                                   reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, sm_groups4, year_from, year_to, mmm)
        else:
            date_now = datetime.now().date().year
            date_from = date_now - year_from
            date_to = date_now - year_to

            if message.text == 'Отмена' or message.text == '/menu':
                msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
                msgs_id.append(msg)
                user_states[usid] = False
                user_msg.append(msgs_id)
                return
            if message.text == 'Опрос':
                conn = sqlite3.connect('ClientsBase.kts')
                cur = conn.cursor()

                cur.execute('SELECT * FROM users')
                users = cur.fetchall()
                pro_users = []

                markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

                b_cancel = types.KeyboardButton(text='Отмена')
                markup.row(b_cancel)

                while date_from <= date_to:
                    for el in users:
                        if datetime.strptime(el[4], '%Y-%m-%d').year == date_from:
                            pro_users.append(el)
                    date_from += 1
                msg = bot.send_message(message.chat.id, 'Введите вопрос для опроса:', reply_markup=markup)
                msgs_id.append(msg)
                user_msg.append(msgs_id)
                bot.register_next_step_handler(msg, poll, pro_users)
                cur.close()
                conn.close()
                return

            conn = sqlite3.connect('ClientsBase.kts')
            cur = conn.cursor()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            mess = None
            if message.content_type == 'poll':
                mess = bot.copy_message(message.chat.id, message.chat.id, message.message_id)
            while date_from <= date_to:
                for el in users:
                    if int(el[6]) != message.chat.id and datetime.strptime(el[4], '%Y-%m-%d').year == date_from:
                        if message.content_type == 'text':
                            bot.send_message(el[6], message.text)

                        elif message.content_type == 'photo':
                            raw = message.photo[2].file_id
                            name = raw + '.jpg'
                            file_info = bot.get_file(raw)
                            downloaded_file = bot.download_file(file_info.file_path)
                            with open(f'./Temp/{name}', "wb") as new_file:
                                new_file.write(downloaded_file)
                            img = open(f'./Temp/{name}', 'rb')
                            if message.caption != 'None':
                                bot.send_photo(el[6], img, caption=message.caption)
                            else:
                                bot.send_photo(el[6], img)
                        elif message.content_type == 'video':
                            raw = message.video.file_id
                            name = raw + '.mp4'
                            file_info = bot.get_file(raw)
                            downloaded_file = bot.download_file(file_info.file_path)
                            with open(f'./Temp/{name}', "wb") as new_file:
                                new_file.write(downloaded_file)
                            img = open(f'./Temp/{name}', 'rb')
                            if message.caption != 'None':
                                bot.send_video(el[6], img, caption=message.caption)
                            else:
                                bot.send_video(el[6], img)
                        elif message.content_type == 'poll':
                            bot.forward_message(el[6], message.chat.id, mess.message_id)
                date_from += 1
            cur.close()
            conn.close()
            markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

            b_cancel = types.KeyboardButton(text='Отмена')
            markup.row(b_cancel)
            msg = bot.send_message(message.chat.id,
                                   'Сообщение было успешно отправлено!\nВведите новое сообщение сообщение:',
                                   reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, sm_groups4, year_from, year_to, mmm)
    else:
        if year_from == 0 and year_to == 0:
            if message.text == 'Отмена' or message.text == '/menu':
                msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
                msgs_id.append(msg)
                user_states[usid] = False
                user_msg.append(msgs_id)
                return
            if message.text == 'Опрос':
                conn = sqlite3.connect('ClientsBase.kts')
                cur = conn.cursor()

                cur.execute('SELECT * FROM users')
                users = cur.fetchall()

                markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

                b_cancel = types.KeyboardButton(text='Отмена')
                markup.row(b_cancel)

                pro_users = []
                for el in users:
                    if str(el[3]) == mmm:
                        pro_users.append(el)
                msg = bot.send_message(message.chat.id, 'Введите вопрос для опроса:', reply_markup=markup)
                msgs_id.append(msg)
                user_msg.append(msgs_id)
                bot.register_next_step_handler(msg, poll, pro_users)
                cur.close()
                conn.close()
                return

            conn = sqlite3.connect('ClientsBase.kts')
            cur = conn.cursor()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            mess = None
            if message.content_type == 'poll':
                mess = bot.copy_message(message.chat.id, message.chat.id, message.message_id)
            for el in users:
                if int(el[6]) != message.chat.id and str(el[3]) == mmm:
                    if message.content_type == 'text':
                        bot.send_message(el[6], message.text)
                    elif message.content_type == 'photo':
                        raw = message.photo[2].file_id
                        name = raw + '.jpg'
                        file_info = bot.get_file(raw)
                        downloaded_file = bot.download_file(file_info.file_path)
                        with open(f'./Temp/{name}', "wb") as new_file:
                            new_file.write(downloaded_file)
                        img = open(f'./Temp/{name}', 'rb')
                        if message.caption != 'None':
                            bot.send_photo(el[6], img, caption=message.caption)
                        else:
                            bot.send_photo(el[6], img)
                    elif message.content_type == 'video':
                        raw = message.video.file_id
                        name = raw + '.mp4'
                        file_info = bot.get_file(raw)
                        downloaded_file = bot.download_file(file_info.file_path)
                        with open(f'./Temp/{name}', "wb") as new_file:
                            new_file.write(downloaded_file)
                        img = open(f'./Temp/{name}', 'rb')
                        if message.caption != 'None':
                            bot.send_video(el[6], img, caption=message.caption)
                        else:
                            bot.send_video(el[6], img)
                    elif message.content_type == 'poll':
                        bot.forward_message(el[6], message.chat.id, mess.message_id)
            cur.close()
            conn.close()
            markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

            b_cancel = types.KeyboardButton(text='Отмена')
            markup.row(b_cancel)
            msg = bot.send_message(message.chat.id,
                                   'Сообщение было успешно отправлено!\nВведите новое сообщение сообщение:',
                                   reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, sm_groups4, year_from, year_to, mmm)
        else:
            date_now = datetime.now().date().year
            date_from = date_now - year_from
            date_to = date_now - year_to

            if message.text == 'Отмена' or message.text == '/menu':
                msg = bot.send_message(message.chat.id, 'Команда была отменена!', reply_markup=markup)
                msgs_id.append(msg)
                user_states[usid] = False
                user_msg.append(msgs_id)
                return
            if message.text == 'Опрос':
                conn = sqlite3.connect('ClientsBase.kts')
                cur = conn.cursor()

                cur.execute('SELECT * FROM users')
                users = cur.fetchall()
                pro_users = []

                markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

                b_cancel = types.KeyboardButton(text='Отмена')
                markup.row(b_cancel)

                while date_from <= date_to:
                    for el in users:
                        if datetime.strptime(el[4], '%Y-%m-%d').year == date_from and str(el[3] == mmm):
                            pro_users.append(el)
                    date_from += 1
                msg = bot.send_message(message.chat.id, 'Введите вопрос для опроса:', reply_markup=markup)
                msgs_id.append(msg)
                user_msg.append(msgs_id)
                bot.register_next_step_handler(msg, poll, pro_users)
                cur.close()
                conn.close()
                return

            conn = sqlite3.connect('ClientsBase.kts')
            cur = conn.cursor()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            mess = None
            if message.content_type == 'poll':
                mess = bot.copy_message(message.chat.id, message.chat.id, message.message_id)
            while date_from <= date_to:
                for el in users:
                    if int(el[6]) != message.chat.id and datetime.strptime(el[4], '%Y-%m-%d').year == date_from and str(el[3] == mmm):
                        if message.content_type == 'text':
                            bot.send_message(el[6], message.text)
                        elif message.content_type == 'photo':
                            raw = message.photo[2].file_id
                            name = raw + '.jpg'
                            file_info = bot.get_file(raw)
                            downloaded_file = bot.download_file(file_info.file_path)
                            with open(f'./Temp/{name}', "wb") as new_file:
                                new_file.write(downloaded_file)
                            img = open(f'./Temp/{name}', 'rb')
                            if message.caption != 'None':
                                bot.send_photo(el[6], img, caption=message.caption)
                            else:
                                bot.send_photo(el[6], img)
                        elif message.content_type == 'video':
                            raw = message.video.file_id
                            name = raw + '.mp4'
                            file_info = bot.get_file(raw)
                            downloaded_file = bot.download_file(file_info.file_path)
                            with open(f'./Temp/{name}', "wb") as new_file:
                                new_file.write(downloaded_file)
                            img = open(f'./Temp/{name}', 'rb')
                            if message.caption != 'None':
                                bot.send_video(el[6], img, caption=message.caption)
                            else:
                                bot.send_video(el[6], img)
                        elif message.content_type == 'poll':
                            bot.forward_message(el[6], message.chat.id, mess.message_id)
                date_from += 1
            cur.close()
            conn.close()
            markup = types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)

            b_cancel = types.KeyboardButton(text='Отмена')
            markup.row(b_cancel)
            msg = bot.send_message(message.chat.id,
                                   'Сообщение было успешно отправлено!\nВведите новое сообщение сообщение:',
                                   reply_markup=markup)
            msgs_id.append(msg)
            user_msg.append(msgs_id)
            bot.register_next_step_handler(message, sm_groups4, year_from, year_to, mmm)

# Аналитика
def date_settings(call, id = 0):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    b_all_time = types.KeyboardButton('За все время')
    b_today = types.KeyboardButton('За сегодня')
    b_yesterday = types.KeyboardButton('За вчера')
    b_week = types.KeyboardButton('За 7 дней')
    b_mounth = types.KeyboardButton('За 30 дней')
    markup.add(b_all_time)
    markup.add(b_today)
    markup.add(b_yesterday)
    markup.add(b_week)
    markup.add(b_mounth)

    msg = bot.send_message(call.message.chat.id, 'Введите дату начало периода. Дата должна быть в таков формате; [ДД.ММ.ГГГГ]\nИли же воспользуйтесь кнопками', reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, date_settings_one, id)

def date_settings_one(message, id):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)
    if message.text == 'За все время':
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if id == 1: # По сумме выкупа первые сто
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            # cur.execute('SELECT * FROM users WHERE sum_checks=(SELECT MAX(sum_checks) FROM users)')
            cur.execute('SELECT * FROM users GROUP BY sum_checks HAVING MAX(sum_checks) ORDER BY sum_checks DESC')
            sum_max = cur.fetchmany(100)
            sum_max_list = []
            for el in sum_max:
                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10], el[11], el[12]
                ]
                sum_max_list.append(sum_dict)
            for el in list(reversed(sum_max_list)):
                ref_count = 0
                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                msg = bot.send_message(message.chat.id, f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)

            sum_checks = 0
            count_checks = 0
            for el in users:
                sum_checks += el[12]
                count_checks += el[11]

            msg = bot.send_message(message.chat.id, f'Топ по сумме выкупа:\nСумма чеков: {sum_checks}\nКоличество чеков: {count_checks}')
            msgs_id.append(msg)
        elif id == 2: # По кол-ву рефералов первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {el[13]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству рефералов за все время:')
            msgs_id.append(msg)
        elif id == 3: # Топ по количеству бонусов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[8])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Кол-во бонусов: {el[8]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству бонусов за все время:')
            msgs_id.append(msg)
        elif id == 4: # Топ по среднему чеку
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count, 0
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            for el in new_l:
                if el[11] != 0:
                    el[14] = int(el[12]/el[11])

            sor_sum_max_list = sorted(new_l, key=lambda item: item[14])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Средний чек: {el[14]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по среднему чеку:')
            msgs_id.append(msg)
        elif id == 5: # По количеству чеков первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[11])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству чеков:')
            msgs_id.append(msg)
        elif id == 6: # По сумме выкупа у МУЖЧИН первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1
                if el[3] != 'Жен':
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, f'Топ по сумме выкупа у Мужчин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 7: # По сумме выкупа у Женщин первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1
                if el[3] != 'Муж':
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Женщин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 8: # Количество клиентов
            count_clients_mau(message)
        cur.close()
        conn.close()
        cur_or.close()
        conn_or.close()
    elif message.text == 'За сегодня':
        date = datetime.now().date()
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if id == 1:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            sum_checks = 0
            count_checks = 0
            for el in sor_sum_max_list:
                sum_checks += el[12]
                count_checks += el[11]

            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nСумма чеков: {sum_checks}\nКоличество чеков: {count_checks}')

            msgs_id.append(msg)
        elif id == 2:  # По количеству рефералов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {el[13]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству рефералов за все время:')
            msgs_id.append(msg)
        elif id == 3: # Топ по количеству бонусов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[8])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Кол-во бонусов: {el[8]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству бонусов за все время:')
            msgs_id.append(msg)
        elif id == 4: # Топ по среднему чеку
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count, 0
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            for el in new_l:
                if el[11] != 0:
                    el[14] = int(el[12]/el[11])

            sor_sum_max_list = sorted(new_l, key=lambda item: item[14])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Средний чек: {el[14]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по среднему чеку:')
            msgs_id.append(msg)
        elif id == 5: # По количеству чеков первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[11])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству чеков:')
            msgs_id.append(msg)
        elif id == 6: # По сумме выкупа у МУЖЧИН первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1
                if el[3] != 'Жен':
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Мужчин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 7: # По сумме выкупа у Женщин первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1
                if el[3] != 'Муж':
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Женщин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 8: # Количество клиентов
            count_clients_mau(message)
        cur.close()
        conn.close()
        cur_or.close()
        conn_or.close()
    elif message.text == 'За вчера':
        date = datetime.now().date() - timedelta(days=1)
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if id == 1:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            sum_checks = 0
            count_checks = 0
            for el in sor_sum_max_list:
                sum_checks += el[12]
                count_checks += el[11]

            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nСумма чеков: {sum_checks}\nКоличество чеков: {count_checks}')

            msgs_id.append(msg)
        elif id == 2:  # По количеству рефералов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {el[13]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству рефералов за все время:')
            msgs_id.append(msg)
        elif id == 3: # Топ по количеству бонусов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[8])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Кол-во бонусов: {el[8]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству бонусов за все время:')
            msgs_id.append(msg)
        elif id == 4: # Топ по среднему чеку
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count, 0
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            for el in new_l:
                if el[11] != 0:
                    el[14] = int(el[12]/el[11])

            sor_sum_max_list = sorted(new_l, key=lambda item: item[14])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Средний чек: {el[14]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по среднему чеку:')
            msgs_id.append(msg)
        elif id == 5: # По количеству чеков первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[11])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству чеков:')
            msgs_id.append(msg)
        elif id == 6: # По сумме выкупа у МУЖЧИН первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1
                if el[3] != 'Жен':
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Мужчин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 7: # По сумме выкупа у Женщин первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1
                if el[3] != 'Муж':
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Женщин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 8: # Количество клиентов
            count_clients_mau(message)
        cur_or.close()
        conn_or.close()
        cur.close()
        conn.close()
    elif message.text == 'За 7 дней':
        date_now = datetime.now().date()
        date_end = date_now - timedelta(days=7)
        delta = timedelta(days=1)
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if id == 1:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            sum_checks = 0
            count_checks = 0
            for el in sor_sum_max_list:
                sum_checks += el[12]
                count_checks += el[11]

            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nСумма чеков: {sum_checks}\nКоличество чеков: {count_checks}')

            msgs_id.append(msg)
        elif id == 2:  # По количеству рефералов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {el[13]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству рефералов за все время:')
            msgs_id.append(msg)
        elif id == 3: # Топ по количеству бонусов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[8])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Кол-во бонусов: {el[8]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству бонусов за все время:')
            msgs_id.append(msg)
        elif id == 4: # Топ по среднему чеку
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_, 0
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            for el in new_l:
                if el[11] != 0:
                    el[13] = int(el[12]/el[11])

            sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Средний чек: {el[13]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по среднему чеку:')
            msgs_id.append(msg)
        elif id == 5: # По количеству чеков первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[11])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству чеков:')
            msgs_id.append(msg)
        elif id == 6: # По сумме выкупа у МУЖЧИН первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0 and el[3] != 'Жен':
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Мужчин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 7: # По сумме выкупа у Женщин первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0 and el[3] != 'Муж':
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Женщин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 8: # Количество клиентов
            count_clients_mau(message)
        cur_or.close()
        conn_or.close()
        cur.close()
        conn.close()
    elif message.text == 'За 30 дней':
        date_now = datetime.now().date()
        date_end = date_now - timedelta(days=30)
        delta = timedelta(days=1)
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if id == 1:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1

                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            sum_checks = 0
            count_checks = 0
            for el in sor_sum_max_list:
                sum_checks += el[12]
                count_checks += el[11]

            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nСумма чеков: {sum_checks}\nКоличество чеков: {count_checks}')

            msgs_id.append(msg)
        elif id == 2:  # По количеству рефералов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {el[13]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству рефералов за все время:')
            msgs_id.append(msg)
        elif id == 3: # Топ по количеству бонусов
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                sum_ = 0
                count_ = 0
                for i in orders:
                    if el[7] == i[1]:
                        sum_ += int(i[2])
                        count_ += 1

                sum_dict = [
                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                    count_,
                    sum_, ref_count
                ]
                sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[8])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Кол-во бонусов: {el[8]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству бонусов за все время:')
            msgs_id.append(msg)
        elif id == 4: # Топ по среднему чеку
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_, 0
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            for el in new_l:
                if el[11] != 0:
                    el[13] = int(el[12] / el[11])

            sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Средний чек: {el[13]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по среднему чеку:')
            msgs_id.append(msg)
        elif id == 5: # По количеству чеков первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sor_sum_max_list = sorted(new_l, key=lambda item: item[11])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, 'Топ по количеству чеков:')
            msgs_id.append(msg)
        elif id == 6: # По сумме выкупа у МУЖЧИН первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0 and el[3] != 'Жен':
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Мужчин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 7: # По сумме выкупа у Женщин первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1]:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0 and el[3] != 'Муж':
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
            del sor_sum_max_list[0:-100]
            for el in sor_sum_max_list:
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа у Женщин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif id == 8: # Количество клиентов
            count_clients_mau(message)
        cur_or.close()
        conn_or.close()
        cur.close()
        conn.close()
    elif len(message.text) == 10:
        i = 0
        user_dated = message.text.split('.')
        if len(user_dated) == 3:
            for date in user_dated:
                for c in date:
                    if c == '0' or c == '1' or c == '2' or c == '3' or c == '4' or c == '5' or c == '6' or c == '7' or c == '8' or c == '9':
                        i += 1

        if i == 8:
            # Проверка исключений
            error = False
            if (int(user_dated[1]) == 2 and int(user_dated[0]) > 28) or (int(user_dated[0]) == 0) or (
                    int(user_dated[1]) == 0) or (int(user_dated[2]) == 0):
                error = True
            if (int(user_dated[1]) == 1 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 3 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 5 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 7 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 8 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 10 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 12 and int(user_dated[0]) > 31):
                error = True
            if (int(user_dated[1]) == 4 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 6 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 9 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 11 and int(user_dated[0]) > 30):
                error = True
            if int(user_dated[1]) > 12:
                error = True

            if error == False:
                date_end = datetime.strptime(message.text, '%d.%m.%Y').date()
                msg = bot.send_message(message.chat.id, 'Введите конечную дату')
                msgs_id.append(msg)
                bot.register_next_step_handler(message, date_settings_two, id, date_end)

            else:
                msg = bot.send_message(message.chat.id,
                                       'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
                msgs_id.append(msg)

                bot.register_next_step_handler(message, date_settings_one, id)
        else:
            msg = bot.send_message(message.chat.id,
                                   'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, date_settings_one, id)
    else:
        msg = bot.send_message(message.chat.id,
                               'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, date_settings_one, id)
    user_states[usid] = False
    user_msg.append(msgs_id)

def date_settings_two(message, id, date_end):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    if len(message.text) == 10:
        i = 0
        user_dated = message.text.split('.')
        if len(user_dated) == 3:
            for date in user_dated:
                for c in date:
                    if c == '0' or c == '1' or c == '2' or c == '3' or c == '4' or c == '5' or c == '6' or c == '7' or c == '8' or c == '9':
                        i += 1

        if i == 8:
            # Проверка исключений
            error = False
            if (int(user_dated[1]) == 2 and int(user_dated[0]) > 28) or (int(user_dated[0]) == 0) or (
                int(user_dated[1]) == 0) or (int(user_dated[2]) == 0):
                error = True
            if (int(user_dated[1]) == 1 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 3 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 5 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 7 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 8 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 10 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 12 and int(user_dated[0]) > 31):
                error = True
            if (int(user_dated[1]) == 4 and int(user_dated[0]) > 30) or (
                int(user_dated[1]) == 6 and int(user_dated[0]) > 30) or (
                int(user_dated[1]) == 9 and int(user_dated[0]) > 30) or (
                int(user_dated[1]) == 11 and int(user_dated[0]) > 30):
                error = True
            if int(user_dated[1]) > 12:
                error = True

            if error == False:
                date_now = datetime.strptime(message.text, '%d.%m.%Y').date()
                delta = timedelta(days=1)
                conn = sqlite3.connect('ClientsBase.kts')
                cur = conn.cursor()
                conn_or = sqlite3.connect('OrdersBase.kts')
                cur_or = conn_or.cursor()

                if id == 1:  # По сумме выкупа первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1]:
                                    sum_ += int(i[2])
                                    count_ += 1

                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

                    for el in sor_sum_max_list:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1

                        msg = bot.send_message(message.chat.id,
                                           f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                        msgs_id.append(msg)
                    sum_checks = 0
                    count_checks = 0
                    for el in sor_sum_max_list:
                        sum_checks += el[12]
                        count_checks += el[11]

                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа:\nСумма чеков: {sum_checks}\nКоличество чеков: {count_checks}')

                    msgs_id.append(msg)
                elif id == 2:  # По количеству рефералов
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    for el in users:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1
                        sum_ = 0
                        count_ = 0
                        for i in orders:
                            if el[7] == i[1]:
                                sum_ += int(i[2])
                                count_ += 1

                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_, ref_count
                        ]
                        sum_max_list.append(sum_dict)

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
                    del sor_sum_max_list[0:-100]
                    for el in sor_sum_max_list:
                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {el[13]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id, 'Топ по количеству рефералов за все время:')
                    msgs_id.append(msg)
                elif id == 3:  # Топ по количеству бонусов
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    for el in users:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1
                        sum_ = 0
                        count_ = 0
                        for i in orders:
                            if el[7] == i[1]:
                                sum_ += int(i[2])
                                count_ += 1

                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_, ref_count
                        ]
                        sum_max_list.append(sum_dict)

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[8])
                    del sor_sum_max_list[0:-100]
                    for el in sor_sum_max_list:
                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Кол-во бонусов: {el[8]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id, 'Топ по количеству бонусов за все время:')
                    msgs_id.append(msg)
                elif id == 4:  # Топ по среднему чеку
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1]:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_, 0
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    for el in new_l:
                        if el[11] != 0:
                            el[13] = int(el[12] / el[11])

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[13])
                    del sor_sum_max_list[0:-100]
                    for el in sor_sum_max_list:
                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Средний чек: {el[13]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id, 'Топ по среднему чеку:')
                    msgs_id.append(msg)
                elif id == 5:  # По количеству чеков первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1]:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[11])
                    del sor_sum_max_list[0:-100]
                    for el in sor_sum_max_list:
                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id, 'Топ по количеству чеков:')
                    msgs_id.append(msg)
                elif id == 6:  # По сумме выкупа у МУЖЧИН первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1]:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0 and el[3] != 'Жен':
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
                    del sor_sum_max_list[0:-100]
                    for el in sor_sum_max_list:
                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа у Мужчин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                elif id == 7:  # По сумме выкупа у Женщин первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1]:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0 and el[3] != 'Муж':
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])
                    del sor_sum_max_list[0:-100]
                    for el in sor_sum_max_list:
                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, Сумма выкупа: {el[12]}, Количество чеков: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа у Женщин:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                elif id == 8:  # Количество клиентов
                    count_clients_mau(message)
                cur_or.close()
                conn_or.close()
                cur.close()
                conn.close()
            else:
                msg = bot.send_message(message.chat.id,
                                   'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
                msgs_id.append(msg)

                bot.register_next_step_handler(message, date_settings_two, id, date_end)
        else:
            msg = bot.send_message(message.chat.id,
                               'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, date_settings_two, id, date_end)
    else:
        msg = bot.send_message(message.chat.id,
                               'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, date_settings_two, id, date_end)
    user_states[usid] = False
    user_msg.append(msgs_id)

@bot.message_handler(commands=['del'])
def del_acc(message):
    chat_id = None
    for ad in admins:
        if message.chat.id == ad:
            chat_id = ad
    if chat_id != None:
        bot.send_message(message.chat.id, 'Введите айди чтобы удалить аккаунт')
        bot.register_next_step_handler(message, delete_acc)
    else:
        bot.send_message(message.chat.id, f'Ваш код {message.chat.id}')

@bot.message_handler(commands=['deltimer'])
def del_t(message):
    chat_id = None
    for ad in admins:
        if message.chat.id == ad:
            chat_id = ad
    if chat_id != None:
        bot.send_message(message.chat.id, 'Введите айди чтобы удалить аккаунт')
        bot.register_next_step_handler(message, delete_t)
    else:
        bot.send_message(message.chat.id, f'Ваш код {message.chat.id}')

def delete_acc(message):
    isCheckTrue = any(character.isdigit() for character in message.text)
    if isCheckTrue:
        id_check = message.text
        conn2 = sqlite3.connect('ClientsBase.kts')
        cur2 = conn2.cursor()
        cur2.execute('SELECT * FROM users')
        users = cur2.fetchall()
        isFind = False
        us_id = None
        us_referer = None
        for el in users:
            if int(id_check) == el[0]:
                us_id = el[0]
                us_referer = el[10]
                isFind = True

        if isFind:
            if str(us_referer) != 'None':
                cur2.execute(f'UPDATE users SET bonus = bonus - {boneses_ref} WHERE user_id = {us_referer}')
            cur2.execute(f'DELETE FROM users WHERE id = {us_id}')
            bot.send_message(message.chat.id, 'Пользователь был стерт!')
            conn2.commit()
            cur2.close()
            conn2.close()
            return

def delete_t(message):
    isCheckTrue = any(character.isdigit() for character in message.text)
    if isCheckTrue:
        id_check = message.text
        conn2 = sqlite3.connect('ClientsBase.kts')
        cur2 = conn2.cursor()
        cur2.execute('SELECT * FROM bonuses')
        users = cur2.fetchall()
        isFind = False
        us_id = None
        us_referer = None
        for el in users:
            if int(id_check) == el[0]:
                us_id = el[0]
                isFind = True

        if isFind:
            cur2.execute(f'DELETE FROM bonuses WHERE id = {us_id}')
            bot.send_message(message.chat.id, 'Бонусный таймер был стерт!')
            conn2.commit()
            cur2.close()
            conn2.close()
            return

def sum_in_old(call, date):
    usid = call.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(call.message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    b_all_time = types.KeyboardButton('За все время')
    b_today = types.KeyboardButton('За сегодня')
    b_yesterday = types.KeyboardButton('За вчера')
    b_week = types.KeyboardButton('За 7 дней')
    b_mounth = types.KeyboardButton('За 30 дней')
    markup.add(b_all_time)
    markup.add(b_today)
    markup.add(b_yesterday)
    markup.add(b_week)
    markup.add(b_mounth)

    msg = bot.send_message(call.message.chat.id,
                           'Введите дату начало периода. Дата должна быть в таков формате; [ДД.ММ.ГГГГ]\nИли же воспользуйтесь кнопками',
                           reply_markup=markup)
    msgs_id.append(msg)
    user_msg.append(msgs_id)
    bot.register_next_step_handler(call.message, sum_in_old_one, date)

def sum_in_old_one(message, data):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    date_now = datetime.now().date().year
    date_10 = date_now - 10
    date_15 = date_now - 15
    date_20 = date_now - 20
    date_25 = date_now - 25
    date_30 = date_now - 30
    date_40 = date_now - 40

    if message.text == 'За все время':
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if data == 10:  # По сумме выкупа первые сто
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            # cur.execute('SELECT * FROM users WHERE sum_checks=(SELECT MAX(sum_checks) FROM users)')
            cur.execute('SELECT * FROM users GROUP BY sum_checks HAVING MAX(sum_checks) ORDER BY sum_checks DESC')
            sum_max = cur.fetchall()
            sum_max_list = []
            sum_checks = 0
            count_checks = 0
            for el in sum_max:
                if date_now >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_10:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10], el[11], el[12]
                    ]
                    sum_checks += el[12]
                    count_checks += el[11]
                    sum_max_list.append(sum_dict)

            for el in list(reversed(sum_max_list)):
                ref_count = 0
                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id, f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 15:  # По сумме выкупа первые сто
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            # cur.execute('SELECT * FROM users WHERE sum_checks=(SELECT MAX(sum_checks) FROM users)')
            cur.execute('SELECT * FROM users GROUP BY sum_checks HAVING MAX(sum_checks) ORDER BY sum_checks DESC')
            sum_max = cur.fetchall()
            sum_max_list = []
            sum_checks = 0
            count_checks = 0
            for el in sum_max:
                if date_10 - 1 > datetime.strptime(el[4], '%Y-%m-%d').year >= date_15:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10], el[11], el[12]
                    ]
                    sum_checks += el[12]
                    count_checks += el[11]
                    sum_max_list.append(sum_dict)
            for el in list(reversed(sum_max_list)):
                ref_count = 0
                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 20:  # По сумме выкупа первые сто
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            # cur.execute('SELECT * FROM users WHERE sum_checks=(SELECT MAX(sum_checks) FROM users)')
            cur.execute('SELECT * FROM users GROUP BY sum_checks HAVING MAX(sum_checks) ORDER BY sum_checks DESC')
            sum_max = cur.fetchall()
            sum_max_list = []
            sum_checks = 0
            count_checks = 0
            for el in sum_max:
                if date_15 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_20:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10], el[11], el[12]
                    ]
                    sum_checks += el[12]
                    count_checks += el[11]
                    sum_max_list.append(sum_dict)
            for el in list(reversed(sum_max_list)):
                ref_count = 0
                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 25:  # По сумме выкупа первые сто
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            # cur.execute('SELECT * FROM users WHERE sum_checks=(SELECT MAX(sum_checks) FROM users)')
            cur.execute('SELECT * FROM users GROUP BY sum_checks HAVING MAX(sum_checks) ORDER BY sum_checks DESC')
            sum_max = cur.fetchall()
            sum_max_list = []
            sum_checks = 0
            count_checks = 0
            for el in sum_max:
                if date_20 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_25:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10], el[11], el[12]
                    ]
                    sum_checks += el[12]
                    count_checks += el[11]
                    sum_max_list.append(sum_dict)
            for el in list(reversed(sum_max_list)):
                ref_count = 0
                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 30:  # По сумме выкупа первые сто
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            # cur.execute('SELECT * FROM users WHERE sum_checks=(SELECT MAX(sum_checks) FROM users)')
            cur.execute('SELECT * FROM users GROUP BY sum_checks HAVING MAX(sum_checks) ORDER BY sum_checks DESC')
            sum_max = cur.fetchall()
            sum_max_list = []
            sum_checks = 0
            count_checks = 0
            for el in sum_max:
                if date_25 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_30:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10], el[11], el[12]
                    ]
                    sum_checks += el[12]
                    count_checks += el[11]
                    sum_max_list.append(sum_dict)
            for el in list(reversed(sum_max_list)):
                ref_count = 0
                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 40:  # По сумме выкупа первые сто
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            # cur.execute('SELECT * FROM users WHERE sum_checks=(SELECT MAX(sum_checks) FROM users)')
            cur.execute('SELECT * FROM users GROUP BY sum_checks HAVING MAX(sum_checks) ORDER BY sum_checks DESC')
            sum_max = cur.fetchall()
            sum_max_list = []
            sum_checks = 0
            count_checks = 0
            for el in sum_max:
                if date_30 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_40:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10], el[11], el[12]
                    ]
                    sum_checks += el[12]
                    count_checks += el[11]
                    sum_max_list.append(sum_dict)
            for el in list(reversed(sum_max_list)):
                ref_count = 0
                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 45:  # По сумме выкупа первые сто
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()
            # cur.execute('SELECT * FROM users WHERE sum_checks=(SELECT MAX(sum_checks) FROM users)')
            cur.execute('SELECT * FROM users GROUP BY sum_checks HAVING MAX(sum_checks) ORDER BY sum_checks DESC')
            sum_max = cur.fetchall()
            sum_max_list = []
            sum_checks = 0
            count_checks = 0
            for el in sum_max:
                if date_40 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10], el[11], el[12]
                    ]
                    sum_checks += el[12]
                    count_checks += el[11]
                    sum_max_list.append(sum_dict)
            for el in list(reversed(sum_max_list)):
                ref_count = 0
                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1
                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        cur.close()
        conn.close()
        cur_or.close()
        conn_or.close()
    elif message.text == 'За сегодня':
        date = datetime.now().date()
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if data == 10:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_now >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_10:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 15:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_10 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_15:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 20:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_15 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_20:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 25:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_20 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_25:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 30:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_25 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_30:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 40:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_30 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year >= date_40:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 45:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_40 - 1 >= datetime.strptime(el[4], '%Y-%m-%d').year:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        cur.close()
        conn.close()
        cur_or.close()
        conn_or.close()
    elif message.text == 'За вчера':
        date = datetime.now().date() - timedelta(days=1)
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if data == 10:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_now >= datetime.strptime(el[4],
                                                                                                  '%Y-%m-%d').year >= date_10:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 15:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_10 - 1 >= datetime.strptime(el[4],
                                                                                                 '%Y-%m-%d').year >= date_15:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 20:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_15 - 1 >= datetime.strptime(el[4],
                                                                                                 '%Y-%m-%d').year >= date_20:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 25:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_20 - 1 >= datetime.strptime(el[4],
                                                                                                 '%Y-%m-%d').year >= date_25:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 30:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_25 - 1 >= datetime.strptime(el[4],
                                                                                                 '%Y-%m-%d').year >= date_30:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 40:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_30 - 1 >= datetime.strptime(el[4],
                                                                                                 '%Y-%m-%d').year >= date_40:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 45:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            for el in users:
                sum_ = 0
                count_ = 0
                for i in orders:
                    if str(date) == str(i[5]) and el[7] == i[1] and date_40 - 1 >= datetime.strptime(el[4],
                                                                                                 '%Y-%m-%d').year:
                        sum_ += int(i[2])
                        count_ += 1
                if sum_ != 0:
                    sum_dict = [
                        el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                        count_,
                        sum_
                    ]
                    sum_max_list.append(sum_dict)

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        cur_or.close()
        conn_or.close()
        cur.close()
        conn.close()
    elif message.text == 'За 7 дней':
        date_now = datetime.now().date()
        date_end = date_now - timedelta(days=7)
        delta = timedelta(days=1)
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if data == 10:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_now >= el[4].year >= date_10:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 15:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_10 - 1 >= datetime.strptime(el[4],
                                                                                                          '%Y-%m-%d').year >= date_15:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 20:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_15 - 1 >= datetime.strptime(el[4],
                                                                                                          '%Y-%m-%d').year >= date_20:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 25:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_20 - 1 >= datetime.strptime(el[4],
                                                                                                          '%Y-%m-%d').year >= date_25:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 30:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_25 - 1 >= datetime.strptime(el[4],
                                                                                                          '%Y-%m-%d').year >= date_30:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 40:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_30 - 1 >= datetime.strptime(el[4],
                                                                                                          '%Y-%m-%d').year >= date_40:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 45:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_40 - 1 >= datetime.strptime(el[4],
                                                                                                          '%Y-%m-%d').year :
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        cur_or.close()
        conn_or.close()
        cur.close()
        conn.close()
    elif message.text == 'За 30 дней':
        date_now = datetime.now().date()
        date_end = date_now - timedelta(days=30)
        delta = timedelta(days=1)
        conn = sqlite3.connect('ClientsBase.kts')
        cur = conn.cursor()
        conn_or = sqlite3.connect('OrdersBase.kts')
        cur_or = conn_or.cursor()

        if data == 10:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_now >= el[4].year >= date_10:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 15:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_10 - 1 >= datetime.strptime(el[4],
                                                                                                         '%Y-%m-%d').year >= date_15:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 20:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_15 - 1 >= datetime.strptime(el[4],
                                                                                                         '%Y-%m-%d').year >= date_20:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 25:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_20 - 1 >= datetime.strptime(el[4],
                                                                                                         '%Y-%m-%d').year >= date_25:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 30:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_25 - 1 >= datetime.strptime(el[4],
                                                                                                         '%Y-%m-%d').year >= date_30:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 40:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_30 - 1 >= datetime.strptime(el[4],
                                                                                                         '%Y-%m-%d').year >= date_40:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        elif data == 45:  # По сумме выкупа первые сто
            cur_or.execute(f'SELECT * FROM orders')
            orders = cur_or.fetchall()
            cur.execute('SELECT * FROM users')
            users = cur.fetchall()

            sum_max_list = []
            while date_end <= date_now:
                for el in users:
                    sum_ = 0
                    count_ = 0
                    for i in orders:
                        if str(date_end) == str(i[5]) and el[7] == i[1] and date_40 - 1 >= datetime.strptime(el[4],
                                                                                                         '%Y-%m-%d').year:
                            sum_ += int(i[2])
                            count_ += 1
                    if sum_ != 0:
                        sum_dict = [
                            el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                            count_,
                            sum_
                        ]
                        sum_max_list.append(sum_dict)
                date_end += delta

            new_l = []
            indexes = []
            for i, el in enumerate(sum_max_list):
                for il in sum_max_list[i + 1:]:
                    if el[7] == il[7]:
                        il[12] += el[12]
                        il[11] += el[11]
                        indexes.append(el[7])
                        new_l.append(il)
            for i in indexes:
                for el in sum_max_list:
                    if el[7] == i:
                        sum_max_list.remove(el)

            new_l += sum_max_list

            sum_checks = 0
            count_checks = 0
            for el in new_l:
                sum_checks += el[12]
                count_checks += el[11]

            sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

            for el in sor_sum_max_list:
                ref_count = 0

                for i in users:
                    if el[7] == i[10]:
                        ref_count += 1

                msg = bot.send_message(message.chat.id,
                                       f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                msgs_id.append(msg)
            msg = bot.send_message(message.chat.id,
                                   f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
            msgs_id.append(msg)
        cur_or.close()
        conn_or.close()
        cur.close()
        conn.close()
    elif len(message.text) == 10:
        i = 0
        user_dated = message.text.split('.')
        if len(user_dated) == 3:
            for date in user_dated:
                for c in date:
                    if c == '0' or c == '1' or c == '2' or c == '3' or c == '4' or c == '5' or c == '6' or c == '7' or c == '8' or c == '9':
                        i += 1

        if i == 8:
            # Проверка исключений
            error = False
            if (int(user_dated[1]) == 2 and int(user_dated[0]) > 28) or (int(user_dated[0]) == 0) or (
                    int(user_dated[1]) == 0) or (int(user_dated[2]) == 0):
                error = True
            if (int(user_dated[1]) == 1 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 3 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 5 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 7 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 8 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 10 and int(user_dated[0]) > 31) or (
                    int(user_dated[1]) == 12 and int(user_dated[0]) > 31):
                error = True
            if (int(user_dated[1]) == 4 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 6 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 9 and int(user_dated[0]) > 30) or (
                    int(user_dated[1]) == 11 and int(user_dated[0]) > 30):
                error = True
            if int(user_dated[1]) > 12:
                error = True

            if error == False:
                date_end = datetime.strptime(message.text, '%d.%m.%Y').date()
                msg = bot.send_message(message.chat.id, 'Введите конечную дату')
                msgs_id.append(msg)
                bot.register_next_step_handler(message, sum_in_old_two, data, date_end)

            else:
                msg = bot.send_message(message.chat.id,
                                       'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
                msgs_id.append(msg)

                bot.register_next_step_handler(message, sum_in_old_one, data)
        else:
            msg = bot.send_message(message.chat.id,
                                   'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, sum_in_old_one, data)
    else:
        msg = bot.send_message(message.chat.id,
                               'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, sum_in_old_one, data)
    user_states[usid] = False
    user_msg.append(msgs_id)

def sum_in_old_two(message, data, date_end):
    usid = message.from_user.id
    for i in user_msg:
        if i[0] == usid:
            i.pop(0)
            for m in i:
                try:
                    bot.delete_message(message.chat.id, m.message_id)
                except Exception as e:
                    print(f'Ошибка удаления сообщения: {e}')
            user_msg.remove(i)
    msgs_id = []
    msgs_id.append(usid)
    msgs_id.append(message)

    if usid in user_states and user_states[usid]:
        return
    user_states[usid] = True

    date_now = datetime.now().date().year
    date_10 = date_now - 10
    date_15 = date_now - 15
    date_20 = date_now - 20
    date_25 = date_now - 25
    date_30 = date_now - 30
    date_40 = date_now - 40

    if len(message.text) == 10:
        i = 0
        user_dated = message.text.split('.')
        if len(user_dated) == 3:
            for date in user_dated:
                for c in date:
                    if c == '0' or c == '1' or c == '2' or c == '3' or c == '4' or c == '5' or c == '6' or c == '7' or c == '8' or c == '9':
                        i += 1

        if i == 8:
            # Проверка исключений
            error = False
            if (int(user_dated[1]) == 2 and int(user_dated[0]) > 28) or (int(user_dated[0]) == 0) or (
                int(user_dated[1]) == 0) or (int(user_dated[2]) == 0):
                error = True
            if (int(user_dated[1]) == 1 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 3 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 5 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 7 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 8 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 10 and int(user_dated[0]) > 31) or (
                int(user_dated[1]) == 12 and int(user_dated[0]) > 31):
                error = True
            if (int(user_dated[1]) == 4 and int(user_dated[0]) > 30) or (
                int(user_dated[1]) == 6 and int(user_dated[0]) > 30) or (
                int(user_dated[1]) == 9 and int(user_dated[0]) > 30) or (
                int(user_dated[1]) == 11 and int(user_dated[0]) > 30):
                error = True
            if int(user_dated[1]) > 12:
                error = True

            if error == False:
                date_now = datetime.strptime(message.text, '%d.%m.%Y').date()
                delta = timedelta(days=1)
                conn = sqlite3.connect('ClientsBase.kts')
                cur = conn.cursor()
                conn_or = sqlite3.connect('OrdersBase.kts')
                cur_or = conn_or.cursor()

                if data == 10:  # По сумме выкупа первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1] and date_now >= el[4].year >= date_10:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

                    for el in sor_sum_max_list:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1

                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                elif data == 15:  # По сумме выкупа первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1] and date_10 - 1 >= datetime.strptime(el[4],
                                                                                                                 '%Y-%m-%d').year >= date_15:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

                    for el in sor_sum_max_list:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1

                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                elif data == 20:  # По сумме выкупа первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1] and date_15 - 1 >= datetime.strptime(el[4],
                                                                                                                 '%Y-%m-%d').year >= date_20:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

                    for el in sor_sum_max_list:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1

                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                elif data == 25:  # По сумме выкупа первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1] and date_20 - 1 >= datetime.strptime(el[4],
                                                                                                                 '%Y-%m-%d').year >= date_25:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

                    for el in sor_sum_max_list:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1

                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                elif data == 30:  # По сумме выкупа первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1] and date_25 - 1 >= datetime.strptime(el[4],
                                                                                                                 '%Y-%m-%d').year >= date_30:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

                    for el in sor_sum_max_list:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1

                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                elif data == 40:  # По сумме выкупа первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1] and date_30 - 1 >= datetime.strptime(el[4],
                                                                                                                 '%Y-%m-%d').year >= date_40:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

                    for el in sor_sum_max_list:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1

                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                elif data == 45:  # По сумме выкупа первые сто
                    cur_or.execute(f'SELECT * FROM orders')
                    orders = cur_or.fetchall()
                    cur.execute('SELECT * FROM users')
                    users = cur.fetchall()

                    sum_max_list = []
                    while date_end <= date_now:
                        for el in users:
                            sum_ = 0
                            count_ = 0
                            for i in orders:
                                if str(date_end) == str(i[5]) and el[7] == i[1] and date_40 - 1 >= datetime.strptime(el[4],
                                                                                                                 '%Y-%m-%d').year:
                                    sum_ += int(i[2])
                                    count_ += 1
                            if sum_ != 0:
                                sum_dict = [
                                    el[0], el[1], el[2], el[3], el[4], el[5], el[6], el[7], el[8], el[9], el[10],
                                    count_,
                                    sum_
                                ]
                                sum_max_list.append(sum_dict)
                        date_end += delta

                    new_l = []
                    indexes = []
                    for i, el in enumerate(sum_max_list):
                        for il in sum_max_list[i + 1:]:
                            if el[7] == il[7]:
                                il[12] += el[12]
                                il[11] += el[11]
                                indexes.append(el[7])
                                new_l.append(il)
                    for i in indexes:
                        for el in sum_max_list:
                            if el[7] == i:
                                sum_max_list.remove(el)

                    new_l += sum_max_list

                    sum_checks = 0
                    count_checks = 0
                    for el in new_l:
                        sum_checks += el[12]
                        count_checks += el[11]

                    sor_sum_max_list = sorted(new_l, key=lambda item: item[12])

                    for el in sor_sum_max_list:
                        ref_count = 0

                        for i in users:
                            if el[7] == i[10]:
                                ref_count += 1

                        msg = bot.send_message(message.chat.id,
                                               f'Имя: {el[1]}, Номер: {el[2]}, Пол: {el[3]}, Дата рождения: {el[4]}, Дата регистрации: {el[9]}, У него рефералов: {ref_count}, Сумма выкупа: {el[12]}, Количество заказов: {el[11]}')
                        msgs_id.append(msg)
                    msg = bot.send_message(message.chat.id,
                                           f'Топ по сумме выкупа:\nОбщая сумма выкупа {sum_checks}\nКоличество чеков {count_checks}')
                    msgs_id.append(msg)
                cur_or.close()
                conn_or.close()
                cur.close()
                conn.close()
            else:
                msg = bot.send_message(message.chat.id,
                                   'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
                msgs_id.append(msg)

                bot.register_next_step_handler(message, sum_in_old_two, data, date_end)
        else:
            msg = bot.send_message(message.chat.id,
                               'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
            msgs_id.append(msg)
            bot.register_next_step_handler(message, sum_in_old_two, data, date_end)
    else:
        msg = bot.send_message(message.chat.id,
                               'Произошла ошибка\nПопробуйте ввести дату более коректно\nДата должна выглядеть в таком формате: [ДД.ММ.ГГГГ]\n(Для отмены действие напишите Отмена)')
        msgs_id.append(msg)
        bot.register_next_step_handler(message, sum_in_old_two, data, date_end)
    user_states[usid] = False
    user_msg.append(msgs_id)

def count_clients_mau(message):
    conn = sqlite3.connect('ClientsBase.kts')
    cur = conn.cursor()

    today = datetime.today().date()

    cur.execute('SELECT * FROM users')
    users = cur.fetchall()
    sum_dict = []
    for el in users:
        count_10 = 0
        count_15 = 0
        count_20 = 0
        count_25 = 0
        count_30 = 0
        count_40 = 0
        count_45 = 0
        age = today.year - datetime.strptime(el[4], '%Y-%m-%d').year - ((today.month, today.day) < (datetime.strptime(el[4], '%Y-%m-%d').month, datetime.strptime(el[4], '%Y-%m-%d').day))
        if 0 <= age <= 10:
            count_10 += 1
        elif 11 <= age <= 15:
            count_15 += 1
        elif 16 <= age <= 20:
            count_20 += 1
        elif 21 <= age <= 25:
            count_25 += 1
        elif 26 <= age <= 30:
            count_30 += 1
        elif 31 <= age <= 40:
            count_40 += 1
        elif 41 <= age:
            count_45 += 1
        if sum_dict:
            sum_dict[0] += 1
            sum_dict[1] += count_10
            sum_dict[2] += count_15
            sum_dict[3] += count_20
            sum_dict[4] += count_25
            sum_dict[5] += count_30
            sum_dict[6] += count_40
            sum_dict[7] += count_45
        else:
            sum_dict = [
            1, count_10, count_15, count_20, count_25, count_30, count_40, count_45
        ]

    bot.send_message(message.chat.id,
                     f'Количество клиентов: {sum_dict[0]}\nОт 6 до 10: {sum_dict[1]}\nОт 11 до 15: {sum_dict[2]}\nОт 16 до 20: {sum_dict[3]}\nОт 21 до 25: {sum_dict[4]}\nОт 26 до 30: {sum_dict[5]}\nОт 31 до 40: {sum_dict[6]}\nОт 41 и больше: {sum_dict[7]}')

    cur.close()
    conn.close()

if __name__ == '__main__':
    thread = threading.Thread(target=foo)
    thread.start() # запускает цикличную функцию проверки времени на данный момент
    while True:
        try:
            bot.polling(none_stop=True)
        except Exception as e:
            print(e)
            sleep(15)