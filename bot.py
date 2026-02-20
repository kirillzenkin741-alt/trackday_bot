import asyncio
import logging
import sqlite3
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = "8104769190:AAFxWOFeC43FVBVo86qL0u7XIUlGHMEj_Iw"
GROUP_ID = -1001720791478  # например: -1001234567890
ADMIN_ID = 920157708    # твой Telegram ID

# Темы недели (бот будет случайно выбирать)
THEMES = [
    "🎵 Трек под дождь",
    "🚗 Музыка для поездки",
    "😤 Трек когда бесит всё вокруг",
    "🌅 Утреннее настроение",
    "🔥 То что слушал в 14 лет",
    "😅 Стыдное но любимое",
    "💤 Музыка перед сном",
    "🏆 Трек недели по настроению",
    "🌙 Ночная атмосфера",
    "⚡ Заряжает энергией",
    "🥲 Ностальгия",
    "🎉 Настроение праздника",
]
# ===================================================

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone="Asia/Tomsk")

# ==================== БАЗА ДАННЫХ ====================
def init_db():
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week TEXT UNIQUE,
            theme TEXT,
            state TEXT DEFAULT 'collecting',
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            user_id INTEGER,
            username TEXT,
            full_name TEXT,
            track_url TEXT,
            track_description TEXT,
            submitted_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            voter_id INTEGER,
            track_id INTEGER,
            voted_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS points (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            total_points INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            participations INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()

def get_current_week():
    return datetime.now().strftime("%Y-W%W")

def get_current_session():
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    week = get_current_week()
    c.execute("SELECT * FROM sessions WHERE week = ?", (week,))
    session = c.fetchone()
    conn.close()
    return session

def create_session(theme):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    week = get_current_week()
    now = datetime.now().isoformat()
    try:
        c.execute("INSERT INTO sessions (week, theme, state, created_at) VALUES (?, ?, 'collecting', ?)",
                  (week, theme, now))
        conn.commit()
        session_id = c.lastrowid
    except sqlite3.IntegrityError:
        c.execute("SELECT id FROM sessions WHERE week = ?", (week,))
        session_id = c.fetchone()[0]
    conn.close()
    return session_id

def get_user_track_in_session(user_id, session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT * FROM tracks WHERE user_id = ? AND session_id = ?", (user_id, session_id))
    track = c.fetchone()
    conn.close()
    return track

def add_track(session_id, user_id, username, full_name, track_url, description):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute("""
        INSERT INTO tracks (session_id, user_id, username, full_name, track_url, track_description, submitted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (session_id, user_id, username, full_name, track_url, description, now))
    conn.commit()
    conn.close()

def get_session_tracks(session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT * FROM tracks WHERE session_id = ?", (session_id,))
    tracks = c.fetchall()
    conn.close()
    return tracks

def has_voted(voter_id, session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT * FROM votes WHERE voter_id = ? AND session_id = ?", (voter_id, session_id))
    vote = c.fetchone()
    conn.close()
    return vote is not None

def add_vote(session_id, voter_id, track_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute("INSERT INTO votes (session_id, voter_id, track_id, voted_at) VALUES (?, ?, ?, ?)",
              (session_id, voter_id, track_id, now))
    conn.commit()
    conn.close()

def get_vote_results(session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("""
        SELECT t.id, t.full_name, t.username, t.track_url, t.track_description, COUNT(v.id) as vote_count
        FROM tracks t
        LEFT JOIN votes v ON t.id = v.track_id
        WHERE t.session_id = ?
        GROUP BY t.id
        ORDER BY vote_count DESC
    """, (session_id,))
    results = c.fetchall()
    conn.close()
    return results

def update_session_state(session_id, state):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("UPDATE sessions SET state = ? WHERE id = ?", (state, session_id))
    conn.commit()
    conn.close()

def update_points(user_id, username, full_name, points_delta, is_win=False, is_participation=False):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT * FROM points WHERE user_id = ?", (user_id,))
    existing = c.fetchone()
    if existing:
        wins_delta = 1 if is_win else 0
        part_delta = 1 if is_participation else 0
        c.execute("""
            UPDATE points SET total_points = total_points + ?, wins = wins + ?, 
            participations = participations + ?, username = ?, full_name = ?
            WHERE user_id = ?
        """, (points_delta, wins_delta, part_delta, username, full_name, user_id))
    else:
        c.execute("""
            INSERT INTO points (user_id, username, full_name, total_points, wins, participations)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, username, full_name, points_delta, 1 if is_win else 0, 1 if is_participation else 0))
    conn.commit()
    conn.close()

def get_leaderboard():
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT full_name, total_points, wins, participations FROM points ORDER BY total_points DESC LIMIT 10")
    board = c.fetchall()
    conn.close()
    return board

# ==================== РАСПИСАНИЕ ====================
import random

async def start_collection():
    """Среда 10:00 — открываем приём треков"""
    theme = random.choice(THEMES)
    session_id = create_session(theme)
    
    text = (
        f"🎵 <b>TRACK DAY!</b>\n\n"
        f"Тема этой недели: <b>{theme}</b>\n\n"
        f"Скидывайте свои треки мне в <b>личку</b> (@track0_day_bot) до 20:00 🎧\n\n"
        f"📌 Правила:\n"
        f"• Один трек от каждого\n"
        f"• Ссылка + можно описание почему именно этот\n"
        f"• Треки будут анонимными до результатов\n\n"
        f"За участие: +1 очко 🏆"
    )
    await bot.send_message(GROUP_ID, text, parse_mode="HTML")

async def start_voting():
    """Среда 20:00 — открываем голосование"""
    session = get_current_session()
    if not session:
        return
    
    session_id = session[0]
    tracks = get_session_tracks(session_id)
    
    if not tracks:
        await bot.send_message(GROUP_ID, "😔 Никто не скинул трек на этой неделе...")
        return
    
    update_session_state(session_id, 'voting')
    
    # Начисляем очки за участие
    for track in tracks:
        update_points(track[2], track[3], track[4], 1, is_participation=True)
    
    text = f"🗳 <b>ГОЛОСОВАНИЕ НАЧАЛОСЬ!</b>\n\nТема: <b>{session[2]}</b>\n\nТреки этой недели (авторы скрыты):\n\n"
    
    for i, track in enumerate(tracks, 1):
        desc = f" — {track[6]}" if track[6] else ""
        text += f"{i}. <a href='{track[5]}'>Трек #{i}</a>{desc}\n"
    
    text += "\n👇 Голосуй за лучший трек:"
    
    # Создаём кнопки голосования
    buttons = []
    for i, track in enumerate(tracks, 1):
        buttons.append([InlineKeyboardButton(
            text=f"🎵 Трек #{i}",
            callback_data=f"vote_{session_id}_{track[0]}"
        )])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await bot.send_message(GROUP_ID, text, reply_markup=keyboard, parse_mode="HTML")

async def finish_voting():
    """Четверг 20:00 — результаты"""
    session = get_current_session()
    if not session:
        return
    
    session_id = session[0]
    if session[3] != 'voting':
        return
    
    update_session_state(session_id, 'finished')
    results = get_vote_results(session_id)
    
    if not results:
        return
    
    text = f"🏆 <b>РЕЗУЛЬТАТЫ TRACK DAY!</b>\n\nТема: <b>{session[2]}</b>\n\n"
    
    medals = ["🥇", "🥈", "🥉"]
    winner = results[0]
    
    for i, result in enumerate(results):
        track_id, full_name, username, url, desc, votes = result
        medal = medals[i] if i < 3 else f"{i+1}."
        username_str = f"@{username}" if username else full_name
        text += f"{medal} <a href='{url}'>{full_name}</a> — {votes} голос(ов)\n"
    
    # Начисляем очки победителю
    if winner[5] > 0:
        update_points(
            get_track_user_id(winner[0]),
            winner[2], winner[1],
            3, is_win=True
        )
        text += f"\n🎉 Победитель: <b>{winner[1]}</b> (+3 очка!)"
    
    # Очки всем кто голосовал — добавим позже
    
    await bot.send_message(GROUP_ID, text, parse_mode="HTML")

def get_track_user_id(track_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT user_id FROM tracks WHERE id = ?", (track_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

# ==================== ХЭНДЛЕРЫ ====================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "👋 Привет! Я бот для <b>Track Day</b>!\n\n"
        "Каждую среду мы выбираем лучший трек недели 🎵\n\n"
        "📌 Команды:\n"
        "/submit — скинуть трек (работает в личке)\n"
        "/leaderboard — таблица лидеров\n"
        "/mystats — твоя статистика\n"
        "/help — помощь",
        parse_mode="HTML"
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "🎵 <b>Как участвовать:</b>\n\n"
        "1. Каждую среду в 10:00 бот объявляет тему\n"
        "2. Пиши мне в личку /submit и скидывай ссылку на трек\n"
        "3. В 20:00 открывается анонимное голосование в группе\n"
        "4. В четверг в 20:00 — результаты\n\n"
        "🏆 <b>Очки:</b>\n"
        "• Скинул трек: +1 очко\n"
        "• Твой трек победил: +3 очка\n"
        "• Проголосовал: +1 очко\n\n"
        "Чем больше очков — тем выше в лидерборде!",
        parse_mode="HTML"
    )

@dp.message(Command("submit"))
async def cmd_submit(message: Message):
    # Работает только в личке
    if message.chat.type != "private":
        await message.answer("📩 Треки принимаю только в личке! Напиши мне сюда: @track0_day_bot")
        return
    
    session = get_current_session()
    if not session:
        await message.answer("😔 Сейчас нет активного сбора треков. Приходи в среду!")
        return
    
    if session[3] != 'collecting':
        await message.answer("⏰ Приём треков уже закрыт, идёт голосование!")
        return
    
    existing = get_user_track_in_session(message.from_user.id, session[0])
    if existing:
        await message.answer(
            f"Ты уже скинул трек на этой неделе:\n{existing[5]}\n\nХочешь заменить? Просто пришли новую ссылку."
        )
    
    await message.answer(
        f"🎵 Тема недели: <b>{session[2]}</b>\n\n"
        "Пришли ссылку на трек (Spotify, YouTube, VK Music — любая)\n"
        "Можно добавить описание почему именно этот трек 👇",
        parse_mode="HTML"
    )
    # Сохраняем что ждём трек от этого юзера
    pending_submissions[message.from_user.id] = session[0]

# Словарь ожидающих submissions
pending_submissions = {}

@dp.message(F.chat.type == "private")
async def handle_private_message(message: Message):
    user_id = message.from_user.id
    
    if user_id not in pending_submissions:
        await message.answer("Напиши /submit чтобы скинуть трек, или /help для помощи")
        return
    
    session_id = pending_submissions[user_id]
    text = message.text or ""
    
    # Проверяем есть ли ссылка
    if not any(domain in text for domain in ["http", "youtu", "spotify", "vk.com", "music.yandex"]):
        await message.answer("❌ Не вижу ссылки на трек. Пришли ссылку (YouTube, Spotify, VK Music, Яндекс Музыка)")
        return
    
    # Разделяем ссылку и описание
    parts = text.split(maxsplit=1)
    track_url = parts[0]
    description = parts[1] if len(parts) > 1 else ""
    
    # Проверяем не отправлял ли уже
    existing = get_user_track_in_session(user_id, session_id)
    
    full_name = message.from_user.full_name
    username = message.from_user.username or ""
    
    if existing:
        # Обновляем трек
        conn = sqlite3.connect("trackday.db")
        c = conn.cursor()
        c.execute("UPDATE tracks SET track_url = ?, track_description = ? WHERE id = ?",
                  (track_url, description, existing[0]))
        conn.commit()
        conn.close()
        await message.answer("✅ Трек обновлён! Ждём голосования в 20:00")
    else:
        add_track(session_id, user_id, username, full_name, track_url, description)
        await message.answer(
            f"✅ Трек принят! Спасибо 🎵\n\n"
            f"Голосование начнётся сегодня в 20:00\n"
            f"Твой трек будет анонимным до объявления результатов 🕵️"
        )
    
    del pending_submissions[user_id]

@dp.callback_query(F.data.startswith("vote_"))
async def handle_vote(callback: CallbackQuery):
    parts = callback.data.split("_")
    session_id = int(parts[1])
    track_id = int(parts[2])
    voter_id = callback.from_user.id
    
    # Проверяем что сессия в состоянии голосования
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT state FROM sessions WHERE id = ?", (session_id,))
    session = c.fetchone()
    conn.close()
    
    if not session or session[0] != 'voting':
        await callback.answer("❌ Голосование уже закончилось!", show_alert=True)
        return
    
    # Проверяем не голосовал ли уже
    if has_voted(voter_id, session_id):
        await callback.answer("Ты уже проголосовал! 😊", show_alert=True)
        return
    
    # Проверяем не голосует ли за свой трек
    own_track = get_user_track_in_session(voter_id, session_id)
    if own_track and own_track[0] == track_id:
        await callback.answer("Нельзя голосовать за свой трек 😄", show_alert=True)
        return
    
    add_vote(session_id, voter_id, track_id)
    
    # Начисляем очко за голосование
    update_points(voter_id, callback.from_user.username or "", callback.from_user.full_name, 1)
    
    await callback.answer("✅ Голос принят! +1 очко тебе 🏆", show_alert=True)

@dp.message(Command("leaderboard"))
async def cmd_leaderboard(message: Message):
    board = get_leaderboard()
    
    if not board:
        await message.answer("Пока никто не набрал очков 😔")
        return
    
    text = "🏆 <b>ТАБЛИЦА ЛИДЕРОВ</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    
    for i, (full_name, points, wins, participations) in enumerate(board):
        medal = medals[i] if i < 3 else f"{i+1}."
        text += f"{medal} <b>{full_name}</b>\n"
        text += f"    💎 {points} очков | 🏆 {wins} побед | 🎵 {participations} треков\n\n"
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("mystats"))
async def cmd_mystats(message: Message):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT total_points, wins, participations FROM points WHERE user_id = ?",
              (message.from_user.id,))
    stats = c.fetchone()
    
    # Позиция в рейтинге
    c.execute("SELECT COUNT(*) FROM points WHERE total_points > (SELECT total_points FROM points WHERE user_id = ?)",
              (message.from_user.id,))
    rank = c.fetchone()[0] + 1
    conn.close()
    
    if not stats:
        await message.answer("У тебя пока нет статистики. Участвуй в Track Day! 🎵")
        return
    
    points, wins, participations = stats
    await message.answer(
        f"📊 <b>Твоя статистика</b>\n\n"
        f"🏅 Место в рейтинге: #{rank}\n"
        f"💎 Очков: {points}\n"
        f"🏆 Побед: {wins}\n"
        f"🎵 Участий: {participations}",
        parse_mode="HTML"
    )

# Команды для админа
@dp.message(Command("startcollection"))
async def cmd_force_start(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await start_collection()
    await message.answer("✅ Сбор треков запущен!")

@dp.message(Command("startvoting"))
async def cmd_force_voting(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await start_voting()
    await message.answer("✅ Голосование запущено!")

@dp.message(Command("finishvoting"))
async def cmd_force_finish(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await finish_voting()
    await message.answer("✅ Голосование завершено!")

# ==================== ЗАПУСК ====================
async def main():
    init_db()
    
    # Расписание: каждую среду
    scheduler.add_job(start_collection, CronTrigger(day_of_week="wed", hour=10, minute=0))
    scheduler.add_job(start_voting, CronTrigger(day_of_week="wed", hour=20, minute=0))
    scheduler.add_job(finish_voting, CronTrigger(day_of_week="thu", hour=20, minute=0))
    scheduler.start()
    
    print("🎵 Track Day Bot запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())