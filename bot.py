import os
from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import sqlite3
import random
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
SPREADSHEET_ID = "1izQKvuzt9iXuTjCpHoXeWwi8GgI5yanDW-fRBtHGzzg"
CREDENTIALS_FILE = "credentials.json"

# Встроенные темы (запасные, если в базе нет)
# ===================================================


# ==================== АВТОПОДБОР ЭМОДЗИ ====================
import random as _random

EMOJI_MAP = [
    (["гта", "gta", "погон", "полиц", "мент", "побег"], "🚨"),
    (["похорон", "похорон", "труп", "умер", "смерть"], "⚰️"),
    (["секс", "еба", "трах", "постел"], "🔥"),
    (["класс", "школ", "урок", "учитель", "первый класс"], "🎒"),
    (["клубн", "клуб", "пиздец", "угар", "дискач"], "🎪"),
    (["чугун", "железо", "сталь", "молот", "кузнец"], "🔨"),
    (["ночь", "ночная", "3 ночи", "ночью", "полночь"], "🌙"),
    (["утро", "утром", "просну", "рассвет", "подъём"], "🌅"),
    (["дождь", "дождл", "ливень", "гроза"], "🌧️"),
    (["злой", "злая", "бесит", "бесят", "агресс", "ненавиж"], "😤"),
    (["грустн", "слезы", "слёзы", "плакать", "печаль", "тоск"], "😭"),
    (["весел", "смех", "смешн", "прикол", "ржать"], "😂"),
    (["стыдн", "стыд", "позор", "неловк"], "😳"),
    (["энерги", "заряжа", "бодр", "мотивац"], "⚡"),
    (["машин", "поездк", "дорог", "авто", "руль"], "🚗"),
    (["лето", "летн", "пляж", "жара", "загор"], "🌴"),
    (["зима", "зимн", "снег", "холод", "мороз"], "❄️"),
    (["осень", "осенн"], "🍂"),
    (["весна", "весенн"], "🌱"),
    (["победа", "чемпион", "герой", "главный", "боец"], "🏆"),
    (["спорт", "трениров", "бег", "качал", "зал"], "🏋️"),
    (["любов", "влюб", "сердц", "романт", "нежност"], "❤️"),
    (["пьянк", "вписк", "пиво", "алкоголь", "бухать"], "🍺"),
    (["детств", "школ", "14 лет", "молодост", "детск"], "📻"),
    (["страшн", "ужас", "жутк", "тьма", "мрак"], "💀"),
    (["космос", "галактик", "вселенн", "планет"], "🚀"),
    (["город", "улиц", "метро", "спальник"], "🏙️"),
    (["природ", "лес", "горы", "поле", "река"], "🌾"),
    (["работ", "учеб", "концентр", "думать", "фокус"], "🧠"),
    (["тайн", "секрет", "никто не знает", "скрыт"], "🤫"),
    (["танц", "вечеринк", "пати"], "🕺"),
    (["тяжел", "метал", "рок", "хард", "агр"], "🤘"),
    (["нежн", "тих", "спокойн", "расслаб", "медитац"], "🌸"),
    (["сон", "засыпа", "перед сном", "спать"], "💤"),
    (["репит", "по кругу", "снова и снова", "залип"], "🔁"),
    (["орать", "кричать", "громк", "вопить"], "🔊"),
    (["игра", "игры", "геймер", "видеоигр"], "🎮"),
    (["кино", "фильм", "саундтрек", "кинотеатр"], "🎬"),
    (["ностальг", "воспомина", "прошлое", "раньше"], "🥲"),
    (["деньги", "богатств", "понты", "роскош"], "💰"),
    (["странн", "безумн", "сумасш", "психоз"], "🛸"),
    (["наушник", "детали", "внимательн", "слушать тихо"], "🔬"),
    (["открыти", "изменил", "открыл", "новый жанр"], "🦋"),
    (["недооцен", "мало знают", "скрытый"], "💎"),
    (["завируси", "хайп", "популярн", "тренд"], "📱"),
    (["поход", "турист", "костёр", "природа"], "🏕️"),
    (["бег", "марафон", "спринт"], "🏃"),
    (["драк", "бит", "удар", "жёстк"], "👊"),
    (["плав", "море", "океан", "вода"], "🌊"),
    (["горы", "скал", "вершин"], "🏔️"),
    (["самолёт", "путешеств", "перелёт"], "✈️"),
]

def pick_emoji(theme_text: str) -> str:
    text_lower = theme_text.lower()
    for keywords, emoji in EMOJI_MAP:
        if any(kw in text_lower for kw in keywords):
            return emoji
    fallback = ["🎵", "🎶", "🎸", "🎤", "🥁", "🎹", "🔥", "✨", "💫", "🎭", "🎪"]
    return _random.choice(fallback)

def add_emoji_to_theme(theme_text: str) -> str:
    stripped = theme_text.strip()
    first_char = stripped[0] if stripped else ""
    emoji = pick_emoji(stripped)
    if not first_char.isalpha() and not first_char.isdigit():
        return f"{stripped} {emoji}"
    else:
        return f"{emoji} {stripped}"

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone="Asia/Tomsk")

# Состояния для управления темами
admin_states = {}

# ==================== GOOGLE SHEETS ====================
def get_sheets_client():
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logging.error(f"Google Sheets error: {e}")
        return None

def update_leaderboard_sheet():
    try:
        client = get_sheets_client()
        if not client:
            return
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        try:
            sheet = spreadsheet.worksheet("🏆 Лидерборд")
        except:
            sheet = spreadsheet.add_worksheet("🏆 Лидерборд", 100, 10)
        board = get_leaderboard()
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        sheet.clear()
        sheet.update("A1:F1", [["🏆 TRACK DAY — ТАБЛИЦА ЛИДЕРОВ", "", "", "", "", f"Обновлено: {now}"]])
        sheet.update("A2:E2", [["#", "Участник", "💎 Очки", "🏆 Победы", "🎵 Участий"]])
        rows = []
        medals = ["🥇", "🥈", "🥉"]
        for i, (full_name, points, wins, participations) in enumerate(board):
            medal = medals[i] if i < 3 else str(i + 1)
            rows.append([medal, full_name, points, wins, participations])
        if rows:
            sheet.update(f"A3:E{2 + len(rows)}", rows)
        try:
            spreadsheet.worksheet("📅 История")
        except:
            history_sheet = spreadsheet.add_worksheet("📅 История", 1000, 10)
            history_sheet.update("A1:F1", [["Неделя", "Тема", "Победитель", "Трек", "Голосов", "Участников"]])
        logging.info("Google Sheets updated!")
    except Exception as e:
        logging.error(f"Error updating sheets: {e}")

def add_week_to_history(week, theme, winner_name, track_url, votes, participants):
    try:
        client = get_sheets_client()
        if not client:
            return
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        try:
            history_sheet = spreadsheet.worksheet("📅 История")
        except:
            history_sheet = spreadsheet.add_worksheet("📅 История", 1000, 10)
            history_sheet.update("A1:F1", [["Неделя", "Тема", "Победитель", "Трек", "Голосов", "Участников"]])
        history_sheet.append_row([week, theme, winner_name, track_url, votes, participants])
    except Exception as e:
        logging.error(f"Error adding history: {e}")

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
    c.execute("""
        CREATE TABLE IF NOT EXISTS themes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme TEXT UNIQUE,
            used INTEGER DEFAULT 0,
            added_at TEXT,
            submitted_by INTEGER DEFAULT 0,
            submitted_name TEXT DEFAULT ""
        )
    """)
    # Добавляем колонки если их нет (для старых баз)
    try:
        c.execute("ALTER TABLE themes ADD COLUMN submitted_by INTEGER DEFAULT 0")
    except:
        pass
    try:
        c.execute("ALTER TABLE themes ADD COLUMN submitted_name TEXT DEFAULT ''")
    except:
        pass
    conn.commit()
    conn.close()

def get_random_theme():
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    # Сначала берём неиспользованные
    c.execute("SELECT id, theme FROM themes WHERE used = 0 ORDER BY RANDOM() LIMIT 1")
    theme = c.fetchone()
    if not theme:
        # Если все использованы — сбрасываем флаги
        c.execute("UPDATE themes SET used = 0")
        conn.commit()
        c.execute("SELECT id, theme FROM themes ORDER BY RANDOM() LIMIT 1")
        theme = c.fetchone()
    if theme:
        c.execute("UPDATE themes SET used = 1 WHERE id = ?", (theme[0],))
        conn.commit()
        conn.close()
        return theme[1]
    conn.close()
    return "🎵 Трек по настроению"

def add_theme_to_db(theme_text, user_id=0, user_name=""):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    try:
        c.execute("INSERT INTO themes (theme, used, added_at, submitted_by, submitted_name) VALUES (?, 0, ?, ?, ?)",
                  (theme_text, now, user_id, user_name))
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    conn.close()
    return result

def delete_theme_from_db(theme_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("DELETE FROM themes WHERE id = ?", (theme_id,))
    conn.commit()
    conn.close()

def get_theme_db_id_by_seq(seq_num, user_id=None):
    """Получить реальный id в базе по порядковому номеру в списке"""
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    if user_id:
        c.execute("SELECT id, theme FROM themes WHERE submitted_by = ? ORDER BY id", (user_id,))
    else:
        c.execute("SELECT id, theme FROM themes ORDER BY id")
    themes = c.fetchall()
    conn.close()
    if 1 <= seq_num <= len(themes):
        return themes[seq_num - 1]  # (id, theme_text)
    return None

def get_all_themes(user_id=None):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    if user_id:
        c.execute("SELECT id, theme, used FROM themes WHERE submitted_by = ? ORDER BY id", (user_id,))
    else:
        c.execute("SELECT id, theme, used FROM themes ORDER BY id")
    themes = c.fetchall()
    conn.close()
    return themes

def get_themes_count():
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM themes")
    count = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM themes WHERE used = 0")
    unused = c.fetchone()[0]
    conn.close()
    return count, unused

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
    c.execute("SELECT full_name, total_points, wins, participations FROM points ORDER BY total_points DESC LIMIT 20")
    board = c.fetchall()
    conn.close()
    return board

def get_track_user_id(track_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT user_id FROM tracks WHERE id = ?", (track_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

# ==================== РАСПИСАНИЕ ====================
# Среда 10:00 — открытие сбора треков
# Среда 22:00 — закрытие сбора, начало голосования
# Четверг 12:00 — конец голосования, результаты

async def start_collection():
    theme = get_random_theme()
    session_id = create_session(theme)
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
    text = (
        f"🎵 <b>TRACK DAY!</b>\n\n"
        f"Тема этой недели: <b>{theme}</b>\n\n"
        f"Скидывайте свои треки мне в <b>личку</b> (@track0_day_bot) до 22:00 🎧\n\n"
        f"📌 Правила:\n"
        f"• Один трек от каждого\n"
        f"• Ссылка + можно описание почему именно этот\n"
        f"• Треки будут анонимными до результатов\n\n"
        f"🏆 <a href='{sheet_url}'>Таблица лидеров</a> | За участие: +1 очко"
    )
    await bot.send_message(GROUP_ID, text, parse_mode="HTML")

async def start_voting():
    session = get_current_session()
    if not session:
        return
    session_id = session[0]
    tracks = get_session_tracks(session_id)
    if not tracks:
        await bot.send_message(GROUP_ID, "😔 Никто не скинул трек на этой неделе...")
        return
    update_session_state(session_id, 'voting')
    for track in tracks:
        update_points(track[2], track[3], track[4], 1, is_participation=True)
    text = f"🗳 <b>ГОЛОСОВАНИЕ НАЧАЛОСЬ!</b>\n\nТема: <b>{session[2]}</b>\n\nТреки этой недели (авторы скрыты):\n\n"
    for i, track in enumerate(tracks, 1):
        desc = f" — {track[6]}" if track[6] else ""
        text += f"{i}. <a href='{track[5]}'>Трек #{i}</a>{desc}\n"
    text += "\n⏰ Голосование закроется завтра в 12:00\n👇 Голосуй за лучший трек:"
    buttons = []
    for i, track in enumerate(tracks, 1):
        buttons.append([InlineKeyboardButton(
            text=f"🎵 Трек #{i}",
            callback_data=f"vote_{session_id}_{track[0]}"
        )])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await bot.send_message(GROUP_ID, text, reply_markup=keyboard, parse_mode="HTML")

async def finish_voting():
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
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"

    # Результаты голосования
    text = f"🏆 <b>РЕЗУЛЬТАТЫ TRACK DAY!</b>\n\nТема: <b>{session[2]}</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    winner = results[0]
    for i, result in enumerate(results):
        track_id, full_name, username, url, desc, votes = result
        medal = medals[i] if i < 3 else f"{i+1}."
        text += f"{medal} <a href='{url}'>{full_name}</a> — {votes} голос(ов)\n"

    if winner[5] > 0:
        winner_user_id = get_track_user_id(winner[0])
        if winner_user_id:
            update_points(winner_user_id, winner[2], winner[1], 3, is_win=True)
        text += f"\n🎉 Победитель: <b>{winner[1]}</b> (+3 очка!)\n"

    # Итоговая таблица очков
    board = get_leaderboard()
    text += f"\n\n📊 <b>ТЕКУЩИЙ РЕЙТИНГ:</b>\n"
    for i, (full_name, points, wins, participations) in enumerate(board):
        medal = medals[i] if i < 3 else f"{i+1}."
        text += f"{medal} <b>{full_name}</b> — {points} очков\n"

    tracks = get_session_tracks(session_id)
    add_week_to_history(session[1], session[2], winner[1], winner[3], winner[5], len(tracks))
    update_leaderboard_sheet()

    text += f"\n📋 <a href='{sheet_url}'>Полная таблица лидеров</a>"
    await bot.send_message(GROUP_ID, text, parse_mode="HTML")

# ==================== ХЭНДЛЕРЫ ====================
pending_submissions = {}

@dp.message(Command("start"))
async def cmd_start(message: Message):
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
    if message.from_user.id == ADMIN_ID:
        await message.answer(
            "👋 Привет, админ! Я бот для <b>Track Day</b>!\n\n"
            "📌 Команды:\n"
            "/submit — скинуть трек\n"
            "/themes — управление темами 🔒\n"
            "/leaderboard — таблица лидеров\n"
            "/mystats — твоя статистика\n"
            "/startcollection — запустить сбор вручную\n"
            "/startvoting — запустить голосование вручную\n"
            "/finishvoting — завершить голосование вручную\n"
            "/updatesheets — обновить Google таблицу\n\n"
            f"📊 <a href='{sheet_url}'>Таблица лидеров онлайн</a>",
            parse_mode="HTML"
        )
    else:
        await message.answer(
            "👋 Привет! Я бот для <b>Track Day</b>!\n\n"
            "Каждую среду мы выбираем лучший трек недели 🎵\n\n"
            "📌 Команды:\n"
            "/submit — скинуть трек (работает в личке)\n"
            "/addtheme — предложить тему недели 💡\n"
            "/leaderboard — таблица лидеров\n"
            "/mystats — твоя статистика\n"
            "/help — помощь\n\n"
            f"📊 <a href='{sheet_url}'>Таблица лидеров онлайн</a>",
            parse_mode="HTML"
        )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "🎵 <b>Как участвовать:</b>\n\n"
        "1. Каждую среду в 10:00 бот объявляет тему\n"
        "2. Пиши мне в личку /submit и скидывай ссылку на трек\n"
        "3. В 22:00 открывается анонимное голосование в группе\n"
        "4. В четверг в 12:00 — результаты\n\n"
        "🏆 <b>Очки:</b>\n"
        "• Скинул трек: +1 очко\n"
        "• Твой трек победил: +3 очка\n"
        "• Проголосовал: +1 очко",
        parse_mode="HTML"
    )

# ==================== УПРАВЛЕНИЕ ТЕМАМИ (только для админа) ====================
@dp.message(Command("themes"))
async def cmd_themes(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.chat.type != "private":
        await message.answer("🔒 Управление темами только в личке!")
        return

    total, unused = get_themes_count()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить тему", callback_data="themes_add")],
        [InlineKeyboardButton(text="📋 Просмотреть все темы", callback_data="themes_list_0")],
        [InlineKeyboardButton(text="🗑 Удалить тему", callback_data="themes_delete_menu")],
        [InlineKeyboardButton(text="🔄 Сбросить флаги использования", callback_data="themes_reset")],
    ])
    await message.answer(
        f"🎛 <b>Управление темами</b>\n\n"
        f"📊 Всего тем: <b>{total}</b>\n"
        f"✅ Неиспользованных: <b>{unused}</b>\n\n"
        f"Что хочешь сделать?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "themes_add")
async def themes_add_start(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    admin_states[callback.from_user.id] = "waiting_theme"
    await callback.message.answer(
        "✏️ Напиши новую тему и отправь мне.\n\n"
        "Можно добавить эмодзи в начале, например:\n"
        "<i>🔥 Трек который слушаешь перед важным делом</i>\n\n"
        "Для отмены напиши /cancel",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("themes_list_"))
async def themes_list(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    page = int(callback.data.split("_")[2])
    is_admin = callback.from_user.id == ADMIN_ID
    themes = get_all_themes(user_id=None if is_admin else callback.from_user.id)
    per_page = 20
    total_pages = (len(themes) + per_page - 1) // per_page
    start = page * per_page
    end = start + per_page
    page_themes = themes[start:end]

    if not themes:
        await callback.message.edit_text(
            "📋 Тем пока нет. Добавь первую!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="themes_back")]
            ])
        )
        await callback.answer()
        return

    text = f"📋 <b>Темы (страница {page+1}/{max(1,total_pages)}):</b>\n\n"
    for i, (theme_id, theme_text, used) in enumerate(page_themes, start + 1):
        status = "✅" if not used else "☑️"
        text += f"{status} <b>{i}.</b> {theme_text}\n\n"

    buttons = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"themes_list_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"themes_list_{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="themes_back")])

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "themes_delete_menu")
async def themes_delete_menu(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    admin_states[callback.from_user.id] = "waiting_delete_id"
    await callback.message.answer(
        "🗑 Напиши <b>номер темы</b> которую хочешь удалить.\n"
        "Номер можно найти в списке тем (команда /themes → Просмотреть все).\n\n"
        "Для отмены напиши /cancel",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "themes_reset")
async def themes_reset(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("UPDATE themes SET used = 0")
    conn.commit()
    conn.close()
    await callback.message.answer("✅ Все темы помечены как неиспользованные!")
    await callback.answer()

@dp.callback_query(F.data == "themes_back")
async def themes_back(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    total, unused = get_themes_count()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить тему", callback_data="themes_add")],
        [InlineKeyboardButton(text="📋 Просмотреть все темы", callback_data="themes_list_0")],
        [InlineKeyboardButton(text="🗑 Удалить тему", callback_data="themes_delete_menu")],
        [InlineKeyboardButton(text="🔄 Сбросить флаги использования", callback_data="themes_reset")],
    ])
    await callback.message.edit_text(
        f"🎛 <b>Управление темами</b>\n\n"
        f"📊 Всего тем: <b>{total}</b>\n"
        f"✅ Неиспользованных: <b>{unused}</b>\n\n"
        f"Что хочешь сделать?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message):
    if message.from_user.id in admin_states:
        del admin_states[message.from_user.id]
        await message.answer("❌ Отменено.")

# ==================== SUBMIT ====================

@dp.message(Command("addtheme"))
async def cmd_addtheme(message: Message):
    if message.chat.type != "private":
        await message.answer("📩 Темы добавляю только в личке! Напиши мне: @track0_day_bot")
        return
    admin_states[message.from_user.id] = "waiting_theme"
    await message.answer(
        "✏️ Напиши тему и отправь мне!\n\n"
        "Просто текст без эмодзи, например:\n"
        "<i>Песня которую слушал в детстве</i>\n\n"
        "Для отмены: /cancel",
        parse_mode="HTML"
    )

@dp.message(Command("submit"))
async def cmd_submit(message: Message):
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
        "Пришли ссылку на трек (Spotify, YouTube, VK Music, Tidal — любая)\n"
        "Можно добавить описание почему именно этот трек 👇",
        parse_mode="HTML"
    )
    pending_submissions[message.from_user.id] = session[0]

@dp.message(F.chat.type == "private")
async def handle_private_message(message: Message):
    user_id = message.from_user.id

    # Обработка добавления темы
    if admin_states.get(user_id) == "waiting_theme" and user_id == ADMIN_ID:
        raw_text = message.text.strip()
        theme_with_emoji = add_emoji_to_theme(raw_text)
        if add_theme_to_db(theme_with_emoji, user_id=user_id, user_name=message.from_user.full_name):
            total, unused = get_themes_count()
            await message.answer(
                f"✅ Тема добавлена!\n\n<b>{theme_with_emoji}</b>\n\n"
                f"Всего тем: {total} | Неиспользованных: {unused}\n\n"
                f"Пиши следующую тему или /cancel для выхода",
                parse_mode="HTML"
            )
        else:
            await message.answer("❌ Такая тема уже есть! Пиши следующую или /cancel")
        return

    # Обработка удаления темы
    if admin_states.get(user_id) == "waiting_delete_id" and user_id == ADMIN_ID:
        try:
            seq_num = int(message.text.strip())
            result = get_theme_db_id_by_seq(seq_num)
            if result:
                db_id, theme_text = result
                delete_theme_from_db(db_id)
                total, unused = get_themes_count()
                await message.answer(
                    f"✅ Тема #{seq_num} удалена:\n<i>{theme_text}</i>\n\n"
                    f"Осталось тем: {total}\n"
                    f"Ещё удалить? Пиши номер или /cancel",
                    parse_mode="HTML"
                )
            else:
                await message.answer("❌ Темы с таким номером нет. Проверь список /themes → Просмотреть все")
        except ValueError:
            await message.answer("❌ Введи число — порядковый номер из списка.")
        return

    # Обработка трека
    if user_id not in pending_submissions:
        await message.answer("Напиши /submit чтобы скинуть трек, или /help для помощи")
        return

    session_id = pending_submissions[user_id]
    text = message.text or ""
    if not any(domain in text for domain in ["http", "youtu", "spotify", "vk.com", "music.yandex", "tidal"]):
        await message.answer("❌ Не вижу ссылки на трек. Пришли ссылку (YouTube, Spotify, VK Music, Яндекс Музыка, Tidal)")
        return
    parts = text.split(maxsplit=1)
    track_url = parts[0]
    description = parts[1] if len(parts) > 1 else ""
    existing = get_user_track_in_session(user_id, session_id)
    full_name = message.from_user.full_name
    username = message.from_user.username or ""
    if existing:
        conn = sqlite3.connect("trackday.db")
        c = conn.cursor()
        c.execute("UPDATE tracks SET track_url = ?, track_description = ? WHERE id = ?",
                  (track_url, description, existing[0]))
        conn.commit()
        conn.close()
        await message.answer("✅ Трек обновлён! Ждём голосования в 22:00")
    else:
        add_track(session_id, user_id, username, full_name, track_url, description)
        await message.answer(
            "✅ Трек принят! Спасибо 🎵\n\n"
            "Голосование начнётся сегодня в 22:00\n"
            "Твой трек будет анонимным до объявления результатов 🕵️"
        )
    del pending_submissions[user_id]

@dp.callback_query(F.data.startswith("vote_"))
async def handle_vote(callback: CallbackQuery):
    parts = callback.data.split("_")
    session_id = int(parts[1])
    track_id = int(parts[2])
    voter_id = callback.from_user.id
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT state FROM sessions WHERE id = ?", (session_id,))
    session = c.fetchone()
    conn.close()
    if not session or session[0] != 'voting':
        await callback.answer("❌ Голосование уже закончилось!", show_alert=True)
        return
    if has_voted(voter_id, session_id):
        await callback.answer("Ты уже проголосовал! 😊", show_alert=True)
        return
    own_track = get_user_track_in_session(voter_id, session_id)
    if own_track and own_track[0] == track_id:
        await callback.answer("Нельзя голосовать за свой трек 😄", show_alert=True)
        return
    add_vote(session_id, voter_id, track_id)
    update_points(voter_id, callback.from_user.username or "", callback.from_user.full_name, 1)
    await callback.answer("✅ Голос принят! +1 очко тебе 🏆", show_alert=True)

@dp.message(Command("leaderboard"))
async def cmd_leaderboard(message: Message):
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
    board = get_leaderboard()
    if not board:
        await message.answer("Пока никто не набрал очков 😔")
        return
    text = "🏆 <b>ТАБЛИЦА ЛИДЕРОВ</b>\n\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, (full_name, points, wins, participations) in enumerate(board):
        medal = medals[i] if i < 3 else f"{i+1}."
        text += f"{medal} <b>{full_name}</b> — {points} очков (побед: {wins}, треков: {participations})\n"
    text += f"\n📊 <a href='{sheet_url}'>Полная таблица</a>"
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("mystats"))
async def cmd_mystats(message: Message):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT total_points, wins, participations FROM points WHERE user_id = ?",
              (message.from_user.id,))
    stats = c.fetchone()
    c.execute("SELECT COUNT(*) FROM points WHERE total_points > COALESCE((SELECT total_points FROM points WHERE user_id = ?), 0)",
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

@dp.message(Command("updatesheets"))
async def cmd_update_sheets(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    update_leaderboard_sheet()
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
    await message.answer(f"✅ Таблица обновлена! <a href='{sheet_url}'>Открыть</a>", parse_mode="HTML")

# ==================== ЗАПУСК ====================
async def main():
    init_db()
    # Среда 10:00 — открытие сбора
    scheduler.add_job(start_collection, CronTrigger(day_of_week="wed", hour=10, minute=0))
    # Среда 22:00 — начало голосования
    scheduler.add_job(start_voting, CronTrigger(day_of_week="wed", hour=22, minute=0))
    # Четверг 12:00 — результаты
    scheduler.add_job(finish_voting, CronTrigger(day_of_week="thu", hour=12, minute=0))
    scheduler.start()
    print("🎵 Track Day Bot запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())