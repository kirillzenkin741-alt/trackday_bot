import os
from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
import logging
import sqlite3
import random
import re
import hashlib
import aiohttp
import gspread
from datetime import datetime, timedelta
from urllib.parse import urlsplit, urlunsplit
from google.oauth2.service_account import Credentials
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ChatMemberUpdated
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

try:
    from transliterate import translit
except ImportError:
    translit = None
from config import load_settings, ConfigError
from states import ThemeStates, NominationStates, SubmitStates
from handlers import register_all_handlers

# ==================== НАСТРОЙКИ ====================
try:
    settings = load_settings()
except ConfigError as e:
    raise SystemExit(f"Configuration error: {e}")
BOT_TOKEN = settings.bot_token
GROUP_ID = settings.group_id
ADMIN_ID = settings.admin_id
SPREADSHEET_ID = settings.spreadsheet_id
CREDENTIALS_FILE = settings.credentials_file

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone=settings.timezone)

API_CACHE_TTL_HOURS = 24
API_CACHE_STALE_DAYS = 7
SUBMIT_CANDIDATE_TTL_MINUTES = 15
RETRY_DELAYS = (0.6, 1.2, 2.4)

RU_TO_LAT_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo", "ж": "zh", "з": "z",
    "и": "i", "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}
LAT_TO_RU_SEQ = {
    "shch": "щ", "sch": "щ", "yo": "ё", "yu": "ю", "ya": "я", "zh": "ж", "kh": "х", "ts": "ц",
    "ch": "ч", "sh": "ш", "iy": "ий",
}
LAT_TO_RU_CHAR = {
    "a": "а", "b": "б", "c": "к", "d": "д", "e": "е", "f": "ф", "g": "г", "h": "х", "i": "и",
    "j": "й", "k": "к", "l": "л", "m": "м", "n": "н", "o": "о", "p": "п", "q": "к", "r": "р",
    "s": "с", "t": "т", "u": "у", "v": "в", "w": "в", "x": "кс", "y": "й", "z": "з",
}

MENU_SUBMIT_TEXT = "🎵 Отправить трек"
MENU_MY_TRACK_TEXT = "🎵 Мой трек"
MENU_STATS_TEXT = "📊 Статистика"
MENU_LEADERBOARD_TEXT = "🏆 Лидеры"
MENU_HISTORY_TEXT = "📖 История"
MENU_ADDTHEME_TEXT = "💡 Предложить тему"
MENU_ADMIN_TEXT = "⚙️ Управление"
MENU_CANCEL_TEXT = "❌ Отмена"


def now_iso() -> str:
    return datetime.now().isoformat()


def normalize_query_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def canonicalize_track_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
    except Exception:
        return raw
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def make_api_cache_key(provider: str, raw_key: str) -> str:
    digest = hashlib.sha256(f"{provider}:{raw_key}".encode("utf-8")).hexdigest()
    return digest


def get_api_cache_payload(provider: str, cache_key: str, fresh_only: bool):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = now_iso()
    if fresh_only:
        c.execute(
            """
            SELECT payload FROM api_cache
            WHERE provider = ? AND cache_key = ? AND expires_at > ?
            LIMIT 1
            """,
            (provider, cache_key, now),
        )
    else:
        stale_from = (datetime.now() - timedelta(days=API_CACHE_STALE_DAYS)).isoformat()
        c.execute(
            """
            SELECT payload FROM api_cache
            WHERE provider = ? AND cache_key = ? AND created_at >= ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (provider, cache_key, stale_from),
        )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return None


def upsert_api_cache_payload(provider: str, cache_key: str, payload, ttl_hours: int = API_CACHE_TTL_HOURS):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    created_at = now_iso()
    expires_at = (datetime.now() + timedelta(hours=ttl_hours)).isoformat()
    c.execute(
        """
        INSERT OR REPLACE INTO api_cache (cache_key, provider, payload, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (cache_key, provider, json.dumps(payload, ensure_ascii=False), created_at, expires_at),
    )
    conn.commit()
    conn.close()


async def fetch_with_cache(provider: str, cache_key: str, fetcher_callable, default_payload):
    cached = get_api_cache_payload(provider, cache_key, fresh_only=True)
    if cached is not None:
        return cached

    last_error = None
    for attempt, delay in enumerate(RETRY_DELAYS, start=1):
        try:
            payload = await fetcher_callable()
            upsert_api_cache_payload(provider, cache_key, payload, ttl_hours=API_CACHE_TTL_HOURS)
            return payload
        except Exception as err:
            last_error = err
            logging.warning(
                "%s request failed: attempt=%s err_type=%s err=%r",
                provider,
                attempt,
                type(err).__name__,
                err,
            )
            if attempt < len(RETRY_DELAYS):
                await asyncio.sleep(delay)

    stale = get_api_cache_payload(provider, cache_key, fresh_only=False)
    if stale is not None:
        logging.warning("%s using stale cache for key=%s", provider, cache_key[:12])
        return stale
    if last_error:
        logging.error("%s failed without cache: err_type=%s err=%r", provider, type(last_error).__name__, last_error)
    return default_payload


def cleanup_expired_runtime_data():
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = now_iso()
    c.execute("DELETE FROM submit_candidates WHERE expires_at < ?", (now,))
    c.execute("DELETE FROM api_cache WHERE expires_at < ?", (now,))
    conn.commit()
    conn.close()


async def cleanup_runtime_data_job():
    cleanup_expired_runtime_data()


# ==================== ODESLI (SONG.LINK) ====================
async def _fetch_song_links_from_odesli(url: str) -> dict:
    timeout = aiohttp.ClientTimeout(total=25)
    attempts = [{"url": url, "userCountry": "US"}, {"url": url}]
    odesli_data = None
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for params in attempts:
            async with session.get("https://api.song.link/v1-alpha.1/links", params=params) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.warning(
                        "Odesli non-200: status=%s params=%s body=%s",
                        resp.status,
                        params,
                        body[:300],
                    )
                    continue
                odesli_data = await resp.json(content_type=None)
                if odesli_data:
                    break
    if not odesli_data:
        raise ValueError("empty_odesli_response")

    platforms = odesli_data.get("linksByPlatform", {})
    result_links = {}
    platform_map = {
        "spotify": ("Spotify", "🟢"),
        "youtube": ("YouTube", "🔴"),
        "youtubeMusic": ("YT Music", "🎵"),
        "tidal": ("Tidal", "💿"),
        "vkMusic": ("VK Music", "🔵"),
        "vk": ("VK Music", "🔵"),
        "yandex": ("Яндекс", "🟡"),
        "yandexMusic": ("Яндекс", "🟡"),
        "appleMusic": ("Apple Music", "🍎"),
        "itunes": ("iTunes", "🎧"),
        "soundcloud": ("SoundCloud", "🟠"),
        "deezer": ("Deezer", "🟣"),
        "amazonMusic": ("Amazon Music", "🛒"),
    }
    for key, (name, emoji) in platform_map.items():
        platform = platforms.get(key)
        if platform and platform.get("url"):
            result_links[key] = {"name": f"{emoji} {name}", "url": platform["url"]}
    for key, platform_data in platforms.items():
        if key in result_links:
            continue
        platform_url = (platform_data or {}).get("url")
        if not platform_url:
            continue
        pretty = re.sub(r"([a-z])([A-Z])", r"\1 \2", key).strip().title()
        result_links[key] = {"name": f"🎼 {pretty}", "url": platform_url}

    meta_title = ""
    meta_artist = ""
    meta_thumbnail = ""
    canonical_url = odesli_data.get("pageUrl", url)
    entity_uid = odesli_data.get("entityUniqueId")
    entities = odesli_data.get("entitiesByUniqueId", {})
    if entity_uid and entity_uid in entities:
        entity = entities.get(entity_uid, {})
        meta_title = entity.get("title", "") or ""
        meta_artist = entity.get("artistName", "") or ""
        meta_thumbnail = entity.get("thumbnailUrl", "") or ""

    return {
        "metadata": {
            "title": meta_title,
            "artist": meta_artist,
            "thumbnail_url": meta_thumbnail,
            "canonical_url": canonical_url,
        },
        "links": result_links,
    }


async def get_song_links(url: str) -> dict:
    """Получить ссылки на трек со всех площадок через Odesli API."""
    cache_key = make_api_cache_key("odesli", url.strip().lower())
    return await fetch_with_cache(
        provider="odesli",
        cache_key=cache_key,
        fetcher_callable=lambda: _fetch_song_links_from_odesli(url),
        default_payload={"metadata": {}, "links": {}},
    )




def _split_artist_title(query: str):
    if not query:
        return "", ""
    parts = re.split(r"\s[-—]\s|:\s*", query, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return "", query.strip()


def _looks_like_artist_title(query: str) -> bool:
    artist, title = _split_artist_title(query)
    return bool(artist and title)




def _contains_cyrillic(text: str) -> bool:
    return bool(re.search(r"[А-Яа-яЁё]", text or ""))


def _contains_latin(text: str) -> bool:
    return bool(re.search(r"[A-Za-z]", text or ""))


def _transliterate_ru_to_lat_text(text: str):
    out = []
    for ch in text:
        low = ch.lower()
        mapped = RU_TO_LAT_MAP.get(low)
        if mapped is None:
            out.append(ch)
            continue
        if ch.isupper():
            out.append(mapped.upper())
        else:
            out.append(mapped)
    return "".join(out)


def _transliterate_lat_to_ru_text(text: str):
    src = text.lower()
    i = 0
    out = []
    while i < len(src):
        matched = False
        for seq in sorted(LAT_TO_RU_SEQ.keys(), key=len, reverse=True):
            if src.startswith(seq, i):
                out.append(LAT_TO_RU_SEQ[seq])
                i += len(seq)
                matched = True
                break
        if matched:
            continue
        ch = src[i]
        if ch in LAT_TO_RU_CHAR:
            out.append(LAT_TO_RU_CHAR[ch])
        else:
            out.append(text[i])
        i += 1
    return "".join(out)


def _transliterate_mixed(query: str, mode: str):
    if mode == "ru_to_lat":
        return re.sub(r"[А-Яа-яЁё]+", lambda m: _transliterate_ru_to_lat_text(m.group(0)), query)
    return re.sub(r"[A-Za-z]+", lambda m: _transliterate_lat_to_ru_text(m.group(0)), query)


def _normalized_variant_key(text: str):
    return re.sub(r"[\W_]+", " ", normalize_query_text(text)).strip()


def generate_translit_variants(query: str):
    variants = []
    seen = set()

    def add_variant(v: str):
        nv = _normalized_variant_key(v)
        if not nv or nv in seen:
            return
        seen.add(nv)
        variants.append(v.strip())

    add_variant(query)
    try:
        if _contains_cyrillic(query):
            add_variant(_transliterate_mixed(query, "ru_to_lat"))
    except Exception:
        pass
    try:
        if _contains_latin(query):
            add_variant(_transliterate_mixed(query, "lat_to_ru"))
    except Exception:
        pass
    if translit:
        try:
            if _contains_cyrillic(query):
                add_variant(translit(query, "ru", reversed=True))
        except Exception:
            pass
        try:
            if _contains_latin(query):
                add_variant(translit(query, "ru"))
        except Exception:
            pass
    return variants


def normalize_track_tokens(text: str):
    txt = normalize_query_text(text or "")
    txt = re.sub(r"[\(\)\[\]\{\}\"'`]", " ", txt)
    txt = re.sub(r"\b(feat|ft)\.?\b", " ", txt)
    txt = re.sub(r"\band\b|&|,|/|\\|\|", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return {t for t in re.findall(r"[A-Za-zА-Яа-я0-9]{2,}", txt)}


def _candidate_match_score(candidate: dict, query: str):
    query_artist, query_title = _split_artist_title(query)
    cand_artist = candidate.get("artist", "") or ""
    cand_title = candidate.get("title", "") or ""

    q_artist_tokens = normalize_track_tokens(query_artist)
    q_title_tokens = normalize_track_tokens(query_title if query_title else query)
    c_artist_tokens = normalize_track_tokens(cand_artist)
    c_title_tokens = normalize_track_tokens(cand_title)

    artist_overlap = len(q_artist_tokens.intersection(c_artist_tokens)) if q_artist_tokens else 0
    title_overlap = len(q_title_tokens.intersection(c_title_tokens)) if q_title_tokens else 0
    all_overlap = len(normalize_track_tokens(query).intersection(normalize_track_tokens(f"{cand_artist} {cand_title}")))

    score = artist_overlap * 6 + title_overlap * 10 + all_overlap * 2
    if q_title_tokens and title_overlap == 0:
        score -= 8
    if q_artist_tokens and artist_overlap == 0 and len(q_artist_tokens) > 0:
        score -= 4
    return score


async def search_track_candidates_multiquery(queries, limit: int = 3):
    normalized_queries = []
    seen_queries = set()
    for q in queries or []:
        nq = normalize_query_text(q)
        if not nq or nq in seen_queries:
            continue
        seen_queries.add(nq)
        normalized_queries.append(q.strip())
    if not normalized_queries:
        return []

    scored = {}
    for query_index, base_query in enumerate(normalized_queries[:8]):
        base_weight = max(2, 20 - query_index * 2)
        pattern_bonus = 6 if _looks_like_artist_title(base_query) else 0
        for variant in generate_translit_variants(base_query):
            variant_bonus = 4 if _normalized_variant_key(variant) == _normalized_variant_key(base_query) else 2
            found = await search_track_candidates_by_text(variant, limit=5)
            for item in found:
                track_url = item.get("track_url", "")
                if not track_url:
                    continue
                match_score = _candidate_match_score(item, base_query)
                score = base_weight + pattern_bonus + variant_bonus + match_score
                if match_score <= 0:
                    score -= 6
                canonical_url = canonicalize_track_url(track_url)
                if canonical_url not in scored:
                    scored[canonical_url] = {**item, "track_url": track_url, "_score": score, "_hits": 1}
                else:
                    scored[canonical_url]["_score"] += score
                    scored[canonical_url]["_hits"] += 1
                    if len(f"{item.get('artist', '')} {item.get('title', '')}") > len(
                        f"{scored[canonical_url].get('artist', '')} {scored[canonical_url].get('title', '')}"
                    ):
                        scored[canonical_url]["artist"] = item.get("artist", "")
                        scored[canonical_url]["title"] = item.get("title", "")
                        scored[canonical_url]["thumbnail_url"] = item.get("thumbnail_url", "")

    ranked = sorted(
        scored.values(),
        key=lambda x: (x.get("_score", 0), x.get("_hits", 0)),
        reverse=True,
    )
    result = []
    for candidate in ranked[:limit]:
        result.append(
            {
                "track_url": candidate.get("track_url", ""),
                "title": candidate.get("title", ""),
                "artist": candidate.get("artist", ""),
                "thumbnail_url": candidate.get("thumbnail_url", ""),
            }
        )
    return result



async def _fetch_itunes_candidates(query: str, limit: int = 3):
    timeout = aiohttp.ClientTimeout(total=20)
    params = {"term": query, "entity": "song", "limit": max(1, min(limit, 10))}
    endpoint = "https://itunes.apple.com/search"
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(endpoint, params=params) as resp:
            if resp.status != 200:
                raise ValueError(f"itunes_non_200_{resp.status}")
            data = await resp.json(content_type=None)
    rows = data.get("results") or []
    candidates = []
    for row in rows:
        track_url = row.get("trackViewUrl") or row.get("collectionViewUrl") or ""
        if not track_url:
            continue
        candidates.append(
            {
                "track_url": track_url,
                "title": row.get("trackName", "") or "",
                "artist": row.get("artistName", "") or "",
                "thumbnail_url": row.get("artworkUrl100", "") or "",
            }
        )
        if len(candidates) >= limit:
            break
    return candidates


async def search_track_candidates_by_text(query: str, limit: int = 3):
    norm = normalize_query_text(query)
    if not norm:
        return []
    cache_key = make_api_cache_key("search", f"{norm}:{limit}")
    payload = await fetch_with_cache(
        provider="search",
        cache_key=cache_key,
        fetcher_callable=lambda: _fetch_itunes_candidates(norm, limit=limit),
        default_payload=[],
    )
    if not isinstance(payload, list):
        return []
    return payload[:limit]


async def search_track_url_by_text(query: str):
    candidates = await search_track_candidates_multiquery([query], limit=1)
    if not candidates:
        return {}
    best = candidates[0]
    return {
        "track_url": best.get("track_url", ""),
        "title": best.get("title", ""),
        "artist": best.get("artist", ""),
    }

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
def ensure_column(conn, table, column, ddl):
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    columns = {row[1] for row in c.fetchall()}
    if column not in columns:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")

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
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE themes ADD COLUMN submitted_name TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    ensure_column(conn, "tracks", "song_links", "song_links TEXT DEFAULT ''")
    ensure_column(conn, "tracks", "track_title", "track_title TEXT DEFAULT ''")
    ensure_column(conn, "tracks", "track_artist", "track_artist TEXT DEFAULT ''")
    ensure_column(conn, "tracks", "track_thumbnail", "track_thumbnail TEXT DEFAULT ''")
    # Дедупликация старых голосов перед уникальным индексом
    c.execute("""
        DELETE FROM votes
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM votes
            GROUP BY session_id, voter_id
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS nominations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS session_nominations (
            session_id INTEGER NOT NULL,
            nomination_id INTEGER NOT NULL,
            nomination_name_snapshot TEXT NOT NULL,
            PRIMARY KEY (session_id, nomination_id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS nomination_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            nomination_id INTEGER NOT NULL,
            voter_id INTEGER NOT NULL,
            track_id INTEGER NOT NULL,
            voted_at TEXT NOT NULL,
            updated_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS session_main_winner (
            session_id INTEGER PRIMARY KEY,
            track_id INTEGER NOT NULL,
            votes INTEGER NOT NULL,
            recorded_at TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS session_nomination_winners (
            session_id INTEGER NOT NULL,
            nomination_id INTEGER NOT NULL,
            nomination_name_snapshot TEXT NOT NULL,
            track_id INTEGER NOT NULL,
            votes INTEGER NOT NULL,
            recorded_at TEXT NOT NULL,
            PRIMARY KEY (session_id, nomination_id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS point_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT UNIQUE NOT NULL,
            session_id INTEGER,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            points INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS api_cache (
            cache_key TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS submit_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            session_id INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            query_text TEXT DEFAULT '',
            candidates_json TEXT NOT NULL,
            selected_index INTEGER DEFAULT NULL,
            description TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS group_pins (
            chat_id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL,
            pin_type TEXT NOT NULL,
            session_id INTEGER NOT NULL,
            pinned_at TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS group_welcomes (
            chat_id INTEGER PRIMARY KEY,
            welcomed_at TEXT NOT NULL
        )
    """)
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_votes_session_voter ON votes(session_id, voter_id)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_nom_votes_session_nom_voter ON nomination_votes(session_id, nomination_id, voter_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_submit_candidates_user_session ON submit_candidates(user_id, session_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_api_cache_expires ON api_cache(expires_at)")
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

def get_week_base():
    return get_current_week()

def parse_week_suffix(week_value: str, base_week: str):
    if week_value == base_week:
        return 1
    prefix = f"{base_week}-"
    if week_value.startswith(prefix):
        tail = week_value[len(prefix):]
        if tail.isdigit():
            return int(tail)
    return None

def list_week_sessions(base_week=None):
    base_week = base_week or get_week_base()
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT * FROM sessions WHERE week = ? OR week LIKE ? ORDER BY id", (base_week, f"{base_week}-%"))
    rows = c.fetchall()
    conn.close()
    valid = []
    for row in rows:
        suffix = parse_week_suffix(row[1], base_week)
        if suffix is not None:
            valid.append((suffix, row))
    valid.sort(key=lambda x: x[0])
    return [row for _, row in valid]

def get_latest_week_session(base_week=None):
    sessions = list_week_sessions(base_week)
    return sessions[-1] if sessions else None

def next_week_suffix(base_week=None):
    base_week = base_week or get_week_base()
    sessions = list_week_sessions(base_week)
    if not sessions:
        return None
    suffixes = [parse_week_suffix(s[1], base_week) for s in sessions]
    suffixes = [x for x in suffixes if x is not None]
    if not suffixes:
        return None
    max_suffix = max(suffixes)
    return max_suffix + 1 if max_suffix >= 1 else 2

def get_active_week_session(base_week=None):
    sessions = list_week_sessions(base_week or get_week_base())
    collecting = [s for s in sessions if s[3] == "collecting"]
    if collecting:
        return collecting[-1]
    voting = [s for s in sessions if s[3] == "voting"]
    if voting:
        return voting[-1]
    return None

def get_current_session():
    sessions = list_week_sessions(get_week_base())
    if not sessions:
        return None
    collecting = [s for s in sessions if s[3] == "collecting"]
    if collecting:
        return collecting[-1]
    voting = [s for s in sessions if s[3] == "voting"]
    if voting:
        return voting[-1]
    finished = [s for s in sessions if s[3] == "finished"]
    if finished:
        return finished[-1]
    return sessions[-1]

def create_session(theme, week_override=None):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    week = week_override or get_current_week()
    now = datetime.now().isoformat()
    c.execute(
        "INSERT INTO sessions (week, theme, state, created_at) VALUES (?, ?, 'collecting', ?)",
        (week, theme, now),
    )
    conn.commit()
    session_id = c.lastrowid
    conn.close()
    return session_id

def get_or_create_collecting_session_for_submit():
    base_week = get_week_base()
    active = get_active_week_session(base_week)
    if active and active[3] == "voting":
        return None, False, "voting"
    if active and active[3] == "collecting":
        return active, False, "existing"

    week_sessions = list_week_sessions(base_week)
    if not week_sessions:
        new_week = base_week
    else:
        suffix = next_week_suffix(base_week)
        new_week = base_week if suffix is None else f"{base_week}-{suffix}"
    theme = get_random_theme()
    session_id = create_session(theme, week_override=new_week)

    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
    session = c.fetchone()
    conn.close()
    if not session:
        return None, False, "create_failed"
    return session, True, "created"

def get_user_track_in_session(user_id, session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT * FROM tracks WHERE user_id = ? AND session_id = ?", (user_id, session_id))
    track = c.fetchone()
    conn.close()
    return track


def delete_user_track_in_session(user_id, session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("DELETE FROM tracks WHERE user_id = ? AND session_id = ?", (user_id, session_id))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    return deleted > 0

def set_group_pin(chat_id, message_id, pin_type, session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO group_pins (chat_id, message_id, pin_type, session_id, pinned_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (chat_id, message_id, pin_type, session_id, now_iso()),
    )
    conn.commit()
    conn.close()

def get_group_pin(chat_id, pin_type=None):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    if pin_type:
        c.execute(
            "SELECT chat_id, message_id, pin_type, session_id, pinned_at FROM group_pins WHERE chat_id = ? AND pin_type = ?",
            (chat_id, pin_type),
        )
    else:
        c.execute(
            "SELECT chat_id, message_id, pin_type, session_id, pinned_at FROM group_pins WHERE chat_id = ?",
            (chat_id,),
        )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "chat_id": row[0],
        "message_id": row[1],
        "pin_type": row[2],
        "session_id": row[3],
        "pinned_at": row[4],
    }

def delete_group_pin(chat_id, pin_type=None):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    if pin_type:
        c.execute("DELETE FROM group_pins WHERE chat_id = ? AND pin_type = ?", (chat_id, pin_type))
    else:
        c.execute("DELETE FROM group_pins WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


def is_group_welcomed(chat_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT 1 FROM group_welcomes WHERE chat_id = ?", (chat_id,))
    row = c.fetchone()
    conn.close()
    return row is not None


def mark_group_welcomed(chat_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO group_welcomes (chat_id, welcomed_at) VALUES (?, ?)",
        (chat_id, now_iso()),
    )
    conn.commit()
    conn.close()

def add_track(
    session_id,
    user_id,
    username,
    full_name,
    track_url,
    description,
    song_links_json="",
    track_title="",
    track_artist="",
    track_thumbnail="",
):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute("""
        INSERT INTO tracks (
            session_id, user_id, username, full_name, track_url, track_description, submitted_at, song_links,
            track_title, track_artist, track_thumbnail
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        session_id, user_id, username, full_name, track_url, description, now, song_links_json,
        track_title, track_artist, track_thumbnail
    ))
    conn.commit()
    conn.close()

def get_session_tracks(session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("""
        SELECT
            id, session_id, user_id, username, full_name, track_url, track_description, submitted_at,
            song_links, track_title, track_artist, track_thumbnail
        FROM tracks
        WHERE session_id = ?
    """, (session_id,))
    tracks = c.fetchall()
    conn.close()
    return tracks

async def save_or_update_track_submission(session_id, user_id, username, full_name, track_url, description):
    existing = get_user_track_in_session(user_id, session_id)
    if existing:
        song_data = await get_song_links(track_url)
        song_links = song_data.get("links", {})
        metadata = song_data.get("metadata", {})
        song_links_json = json.dumps(song_links, ensure_ascii=False)
        conn2 = sqlite3.connect("trackday.db")
        c2 = conn2.cursor()
        c2.execute(
            """
            UPDATE tracks
            SET track_url = ?, track_description = ?, song_links = ?, track_title = ?, track_artist = ?, track_thumbnail = ?
            WHERE id = ?
            """,
            (
                track_url,
                description,
                song_links_json,
                metadata.get("title", ""),
                metadata.get("artist", ""),
                metadata.get("thumbnail_url", ""),
                existing[0],
            ),
        )
        conn2.commit()
        conn2.close()
        links_count = len(song_links)
        links_info = f"Нашёл на {links_count} площадках 🎯" if links_count > 1 else ""
        track_label = format_track_label(metadata.get("artist", ""), metadata.get("title", ""), fallback=track_url)
        return "updated", links_info, track_label, track_url
    song_data = await get_song_links(track_url)
    song_links = song_data.get("links", {})
    metadata = song_data.get("metadata", {})
    song_links_json = json.dumps(song_links, ensure_ascii=False)
    add_track(
        session_id,
        user_id,
        username,
        full_name,
        track_url,
        description,
        song_links_json,
        track_title=metadata.get("title", ""),
        track_artist=metadata.get("artist", ""),
        track_thumbnail=metadata.get("thumbnail_url", ""),
    )
    links_count = len(song_links)
    links_info = f"Нашёл на {links_count} площадках 🎯" if links_count > 1 else "Ссылки на другие площадки не найдены"
    track_label = format_track_label(metadata.get("artist", ""), metadata.get("title", ""), fallback=track_url)
    return "new", links_info, track_label, track_url

def is_track_in_session(track_id, session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT 1 FROM tracks WHERE id = ? AND session_id = ?", (track_id, session_id))
    result = c.fetchone()
    conn.close()
    return result is not None

def is_nomination_in_session(nomination_id, session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute(
        "SELECT 1 FROM session_nominations WHERE nomination_id = ? AND session_id = ?",
        (nomination_id, session_id)
    )
    result = c.fetchone()
    conn.close()
    return result is not None

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
    try:
        c.execute("SELECT id FROM votes WHERE session_id = ? AND voter_id = ?", (session_id, voter_id))
        existing = c.fetchone()
        if existing:
            c.execute(
                "UPDATE votes SET track_id = ?, voted_at = ? WHERE id = ?",
                (track_id, now, existing[0])
            )
            conn.commit()
            return "updated"
        c.execute(
            "INSERT INTO votes (session_id, voter_id, track_id, voted_at) VALUES (?, ?, ?, ?)",
            (session_id, voter_id, track_id, now)
        )
        conn.commit()
        return "new"
    except sqlite3.IntegrityError:
        return "blocked"
    finally:
        conn.close()

def add_nomination(name):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    try:
        c.execute("INSERT INTO nominations (name, is_active, created_at) VALUES (?, 1, ?)", (name, now))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def get_nominations(active_only=False):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    if active_only:
        c.execute("SELECT id, name, is_active FROM nominations WHERE is_active = 1 ORDER BY id")
    else:
        c.execute("SELECT id, name, is_active FROM nominations ORDER BY id")
    rows = c.fetchall()
    conn.close()
    return rows

def get_nomination_db_id_by_seq(seq_num):
    nominations = get_nominations(active_only=False)
    if 1 <= seq_num <= len(nominations):
        nomination_id, name, is_active = nominations[seq_num - 1]
        return nomination_id, name
    return None

def toggle_nomination(nomination_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT is_active FROM nominations WHERE id = ?", (nomination_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    new_state = 0 if row[0] else 1
    c.execute("UPDATE nominations SET is_active = ? WHERE id = ?", (new_state, nomination_id))
    conn.commit()
    conn.close()
    return new_state

def delete_nomination(nomination_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("DELETE FROM nominations WHERE id = ?", (nomination_id,))
    affected = c.rowcount
    conn.commit()
    conn.close()
    return affected > 0

def create_session_nomination_snapshot(session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM session_nominations WHERE session_id = ?", (session_id,))
    if c.fetchone()[0] > 0:
        c.execute(
            "SELECT nomination_id, nomination_name_snapshot FROM session_nominations WHERE session_id = ? ORDER BY nomination_id",
            (session_id,)
        )
        rows = c.fetchall()
        conn.close()
        return rows
    c.execute("SELECT id, name FROM nominations WHERE is_active = 1 ORDER BY id")
    active = c.fetchall()
    for nomination_id, name in active:
        c.execute(
            "INSERT INTO session_nominations (session_id, nomination_id, nomination_name_snapshot) VALUES (?, ?, ?)",
            (session_id, nomination_id, name)
        )
    conn.commit()
    c.execute(
        "SELECT nomination_id, nomination_name_snapshot FROM session_nominations WHERE session_id = ? ORDER BY nomination_id",
        (session_id,)
    )
    rows = c.fetchall()
    conn.close()
    return rows

def get_session_nominations(session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute(
        "SELECT nomination_id, nomination_name_snapshot FROM session_nominations WHERE session_id = ? ORDER BY nomination_id",
        (session_id,)
    )
    rows = c.fetchall()
    conn.close()
    return rows

def add_nomination_vote(session_id, nomination_id, voter_id, track_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    try:
        c.execute(
            "SELECT id FROM nomination_votes WHERE session_id = ? AND nomination_id = ? AND voter_id = ?",
            (session_id, nomination_id, voter_id)
        )
        existing = c.fetchone()
        if existing:
            c.execute(
                "UPDATE nomination_votes SET track_id = ?, updated_at = ? WHERE id = ?",
                (track_id, now, existing[0])
            )
            conn.commit()
            return "updated"
        c.execute(
            "INSERT INTO nomination_votes (session_id, nomination_id, voter_id, track_id, voted_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (session_id, nomination_id, voter_id, track_id, now, now)
        )
        conn.commit()
        return "new"
    except sqlite3.IntegrityError:
        return "blocked"
    finally:
        conn.close()

def user_completed_all_nominations(session_id, voter_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM session_nominations WHERE session_id = ?", (session_id,))
    total = c.fetchone()[0]
    if total == 0:
        conn.close()
        return True
    c.execute(
        "SELECT COUNT(*) FROM nomination_votes WHERE session_id = ? AND voter_id = ?",
        (session_id, voter_id)
    )
    voted = c.fetchone()[0]
    conn.close()
    return voted >= total

def get_nomination_winners(session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute(
        "SELECT nomination_id, nomination_name_snapshot FROM session_nominations WHERE session_id = ? ORDER BY nomination_id",
        (session_id,)
    )
    nominations = c.fetchall()
    winners = []
    for nomination_id, nom_name in nominations:
        c.execute(
            """
            SELECT t.id, t.full_name, t.username, t.track_url, COUNT(v.id) as vote_count
            FROM tracks t
            LEFT JOIN nomination_votes v
              ON t.id = v.track_id
             AND v.nomination_id = ?
             AND v.session_id = ?
            WHERE t.session_id = ?
            GROUP BY t.id
            ORDER BY vote_count DESC, t.id ASC
            LIMIT 1
            """,
            (nomination_id, session_id, session_id)
        )
        winner = c.fetchone()
        if winner:
            winners.append((nomination_id, nom_name, winner[0], winner[1], winner[2], winner[3], winner[4]))
    conn.close()
    return winners

def save_main_winner(session_id, track_id, votes):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute(
        "INSERT OR REPLACE INTO session_main_winner (session_id, track_id, votes, recorded_at) VALUES (?, ?, ?, ?)",
        (session_id, track_id, votes, now)
    )
    conn.commit()
    conn.close()

def save_nomination_winner(session_id, nomination_id, nomination_name_snapshot, track_id, votes):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute(
        """
        INSERT OR REPLACE INTO session_nomination_winners
        (session_id, nomination_id, nomination_name_snapshot, track_id, votes, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (session_id, nomination_id, nomination_name_snapshot, track_id, votes, now)
    )
    conn.commit()
    conn.close()

def get_history_page(page=0, per_page=10):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM session_main_winner")
    total = c.fetchone()[0]
    offset = page * per_page
    c.execute(
        """
        SELECT s.week, s.theme, t.full_name, t.track_url, w.votes, t.track_artist, t.track_title
        FROM session_main_winner w
        JOIN sessions s ON s.id = w.session_id
        JOIN tracks t ON t.id = w.track_id
        ORDER BY s.week DESC
        LIMIT ? OFFSET ?
        """,
        (per_page, offset)
    )
    rows = c.fetchall()
    conn.close()
    return rows, total

def format_track_label(artist, title, fallback="Трек"):
    artist = (artist or "").strip()
    title = (title or "").strip()
    if artist and title:
        return f"{artist} - {title}"
    if title:
        return title
    if artist:
        return artist
    return fallback

def short_button_text(text, limit=60):
    if len(text) <= limit:
        return text
    return text[:limit - 1] + "…"

def upsert_submit_candidates(user_id, session_id, source_type, query_text, description, candidates):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = now_iso()
    expires_at = (datetime.now() + timedelta(minutes=SUBMIT_CANDIDATE_TTL_MINUTES)).isoformat()
    c.execute(
        "DELETE FROM submit_candidates WHERE user_id = ? AND session_id = ?",
        (user_id, session_id),
    )
    c.execute(
        """
        INSERT INTO submit_candidates (
            user_id, session_id, source_type, query_text, candidates_json, selected_index, description, created_at, expires_at
        )
        VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?)
        """,
        (
            user_id,
            session_id,
            source_type,
            query_text or "",
            json.dumps(candidates, ensure_ascii=False),
            description or "",
            now,
            expires_at,
        ),
    )
    token = str(c.lastrowid)
    conn.commit()
    conn.close()
    return token


def get_submit_candidates(token):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute(
        """
        SELECT id, user_id, session_id, source_type, query_text, candidates_json, selected_index, description, expires_at
        FROM submit_candidates
        WHERE id = ?
        """,
        (token,),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    if row[8] < now_iso():
        delete_submit_candidates(token)
        return None
    try:
        candidates = json.loads(row[5] or "[]")
    except (TypeError, json.JSONDecodeError):
        candidates = []
    return {
        "id": row[0],
        "user_id": row[1],
        "session_id": row[2],
        "source_type": row[3],
        "query_text": row[4] or "",
        "candidates": candidates if isinstance(candidates, list) else [],
        "selected_index": row[6],
        "description": row[7] or "",
        "expires_at": row[8],
    }


def set_submit_selected_index(token, selected_index):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute(
        "UPDATE submit_candidates SET selected_index = ? WHERE id = ?",
        (selected_index, token),
    )
    conn.commit()
    conn.close()


def delete_submit_candidates(token):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("DELETE FROM submit_candidates WHERE id = ?", (token,))
    conn.commit()
    conn.close()


def format_candidate_label(candidate, fallback="Трек"):
    if not isinstance(candidate, dict):
        return fallback
    return format_track_label(candidate.get("artist", ""), candidate.get("title", ""), fallback=fallback)


def build_submit_candidates_keyboard(token, candidates):
    rows = []
    for idx, candidate in enumerate(candidates):
        label = format_candidate_label(candidate, fallback=f"Вариант {idx + 1}")
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🎯 {idx + 1}. {short_button_text(label, 45)}",
                    callback_data=f"cand_{token}_{idx}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel_submit_{token}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_submit_confirm_keyboard(token):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_{token}")],
            [InlineKeyboardButton(text="🔁 Выбрать другой", callback_data=f"retryfind_{token}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancel_submit_{token}")],
        ]
    )


def build_replace_track_keyboard(session_id):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔁 Заменить трек", callback_data=f"replace_track_{session_id}")]
        ]
    )


def build_mytrack_keyboard(session_id, allow_delete):
    if allow_delete:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Заменить", callback_data=f"my_replace_{session_id}")],
                [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"my_delete_{session_id}")],
                [InlineKeyboardButton(text="❎ Закрыть", callback_data="my_close")],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔁 Заменить", callback_data=f"my_replace_{session_id}")],
            [InlineKeyboardButton(text="ℹ️ Закрыто", callback_data="my_close")],
        ]
    )


def build_main_menu_keyboard(is_admin=False):
    keyboard = [
        [KeyboardButton(text=MENU_SUBMIT_TEXT)],
        [KeyboardButton(text=MENU_MY_TRACK_TEXT)],
        [KeyboardButton(text=MENU_STATS_TEXT)],
        [KeyboardButton(text=MENU_LEADERBOARD_TEXT)],
        [KeyboardButton(text=MENU_HISTORY_TEXT)],
        [KeyboardButton(text=MENU_ADDTHEME_TEXT)],
    ]
    if is_admin:
        keyboard.append([KeyboardButton(text=MENU_ADMIN_TEXT)])
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выберите действие",
    )


def build_cancel_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=MENU_CANCEL_TEXT)]],
        resize_keyboard=True,
    )


def build_admin_panel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎛 Темы", callback_data="admin_themes")],
        [InlineKeyboardButton(text="🏅 Номинации", callback_data="admin_nominations")],
        [InlineKeyboardButton(text="▶️ Запустить сбор", callback_data="admin_startcollection")],
        [InlineKeyboardButton(text="🗳 Запустить голосование", callback_data="admin_startvoting")],
        [InlineKeyboardButton(text="✅ Завершить голосование", callback_data="admin_finishvoting")],
        [InlineKeyboardButton(text="📊 Обновить таблицу", callback_data="admin_updatesheets")],
    ])

def get_vote_results(session_id):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("""
        SELECT t.id, t.full_name, t.username, t.track_url, t.track_description, COUNT(v.id) as vote_count
        FROM tracks t
        LEFT JOIN votes v ON t.id = v.track_id AND v.session_id = ?
        WHERE t.session_id = ?
        GROUP BY t.id
        ORDER BY vote_count DESC
    """, (session_id, session_id))
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

def apply_points_event(
    user_id,
    username,
    full_name,
    points_delta,
    event_key,
    event_type,
    session_id=None,
    is_win=False,
    is_participation=False,
):
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    now = datetime.now().isoformat()
    try:
        c.execute(
            """
            INSERT INTO point_events (event_key, session_id, user_id, event_type, points, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_key, session_id, user_id, event_type, points_delta, now),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return False
    conn.close()
    update_points(
        user_id,
        username,
        full_name,
        points_delta,
        is_win=is_win,
        is_participation=is_participation,
    )
    return True

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
    try:
        base_week = get_week_base()
        active = get_active_week_session(base_week)
        if active:
            logging.info(f"start_collection skipped: active session already exists for {base_week}, state={active[3]}")
            return
        theme = get_random_theme()
        existing = list_week_sessions(base_week)
        if existing:
            # Для автозапуска после finished не создаём новую suffix-сессию.
            latest = existing[-1]
            if latest[3] == "finished":
                logging.info(f"start_collection skipped: latest session for {base_week} is finished")
                return
        session_id = create_session(theme, week_override=base_week)
        logging.info(f"Session created: id={session_id}, theme={theme}")
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
        sent = await bot.send_message(GROUP_ID, text, parse_mode="HTML")
        logging.info("start_collection: message sent to group")
        if await bot_can_manage_pins(GROUP_ID):
            stale = get_group_pin(GROUP_ID, pin_type="collection_main")
            if stale:
                try:
                    await bot.unpin_chat_message(chat_id=GROUP_ID, message_id=stale["message_id"])
                except Exception:
                    pass
                finally:
                    delete_group_pin(GROUP_ID, pin_type="collection_main")
            try:
                await bot.pin_chat_message(chat_id=GROUP_ID, message_id=sent.message_id, disable_notification=False)
                set_group_pin(GROUP_ID, sent.message_id, "collection_main", session_id)
            except Exception as e:
                logging.warning("Failed to pin collection message: %r", e)
    except Exception as e:
        logging.error(f"start_collection error: {e}")

async def send_wednesday_reminder():
    try:
        text = (
            "⏰ <b>Напоминание Track Day</b>\n\n"
            "Сегодня <b>последний день</b>, когда можно отправить трек 🎵\n"
            "Голосование стартует уже <b>сегодня в 22:00</b> 🗳\n\n"
            "Отправляйте треки мне в личку (@track0_day_bot) — кнопка «🎵 Отправить трек»."
        )
        await bot.send_message(GROUP_ID, text, parse_mode="HTML")
        logging.info("wednesday reminder sent to group")
    except Exception as e:
        logging.error("send_wednesday_reminder error: %r", e)


async def send_collection_closing_reminder():
    """Среда 21:00 — через час закроется приём треков и начнётся голосование."""
    try:
        text = (
            "⏰ <b>Остался 1 час!</b>\n\n"
            "В <b>22:00</b> приём треков закрывается и начинается голосование 🗳\n"
            "Успей отправить свой трек — напиши боту в личку (@track0_day_bot), кнопка «🎵 Отправить трек»."
        )
        await bot.send_message(GROUP_ID, text, parse_mode="HTML")
        logging.info("collection closing reminder sent to group")
    except Exception as e:
        logging.error("send_collection_closing_reminder error: %r", e)


async def send_voting_closing_reminder():
    """Четверг 11:00 — через час закроется голосование."""
    try:
        text = (
            "⏰ <b>Остался 1 час!</b>\n\n"
            "В <b>12:00</b> голосование заканчивается 🗳\n"
            "Успей проголосовать, если ещё не успел!"
        )
        await bot.send_message(GROUP_ID, text, parse_mode="HTML")
        logging.info("voting closing reminder sent to group")
    except Exception as e:
        logging.error("send_voting_closing_reminder error: %r", e)

async def bot_can_manage_pins(chat_id):
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=bot.id)
    except Exception as e:
        logging.warning("Failed to fetch bot permissions: chat_id=%s err=%r", chat_id, e)
        return False
    status = getattr(member, "status", "")
    if status not in ("administrator", "creator"):
        return False
    can_pin_messages = getattr(member, "can_pin_messages", None)
    can_edit_messages = getattr(member, "can_edit_messages", None)
    if can_pin_messages is False and can_edit_messages is False:
        return False
    return True

async def start_voting():
    session = get_current_session()
    if not session:
        return False, "no_session"
    if session[3] == 'voting':
        logging.info(f"start_voting skipped: session {session[0]} already in voting")
        return False, "already_voting"
    if session[3] == 'finished':
        logging.info(f"start_voting skipped: session {session[0]} already finished")
        return False, "already_finished"
    if session[3] != 'collecting':
        logging.info(f"start_voting skipped: session {session[0]} state={session[3]}")
        return False, "invalid_state"
    session_id = session[0]
    tracks = get_session_tracks(session_id)
    if not tracks:
        await bot.send_message(GROUP_ID, "😔 Никто не скинул трек на этой неделе...")
        return False, "no_tracks"
    session_nominations = create_session_nomination_snapshot(session_id)
    update_session_state(session_id, 'voting')
    for track in tracks:
        apply_points_event(
            track[2],
            track[3],
            track[4],
            1,
            event_key=f"participation:{session_id}:{track[2]}",
            event_type="participation",
            session_id=session_id,
            is_participation=True,
        )
    text = (
        f"🗳 <b>ГОЛОСОВАНИЕ НАЧАЛОСЬ!</b>\n\n"
        f"Тема: <b>{session[2]}</b>\n\n"
        "Сначала выбери победителей во всех номинациях (если они есть), затем проголосуй за лучший трек недели.\n\n"
        "⏰ Голосование закроется завтра в 12:00"
    )
    await bot.send_message(GROUP_ID, text, parse_mode="HTML")

    # Отправляем каждый трек отдельным сообщением с кнопками площадок
    track_seq = {}
    for i, track in enumerate(tracks, 1):
        track_seq[track[0]] = i
        desc = f"\n💬 {track[6]}" if track[6] else ""
        title = track[9] if len(track) > 9 and track[9] else ""
        artist = track[10] if len(track) > 10 and track[10] else ""
        thumbnail = track[11] if len(track) > 11 and track[11] else ""
        track_label = format_track_label(artist, title, fallback=f"Трек #{i}")
        track_text = f"🎵 <b>{track_label}</b>{desc}"
        
        buttons = []
        
        # Кнопки площадок
        song_links = {}
        if len(track) > 8 and track[8]:
            try:
                loaded = json.loads(track[8])
                # backward compatibility: ранее могли хранить полный объект с metadata/links
                if isinstance(loaded, dict) and "links" in loaded and isinstance(loaded["links"], dict):
                    song_links = loaded["links"]
                elif isinstance(loaded, dict):
                    song_links = loaded
            except (json.JSONDecodeError, TypeError):
                song_links = {}
        
        if song_links:
            platform_row = []
            for key, info in song_links.items():
                platform_row.append(InlineKeyboardButton(text=info["name"], url=info["url"]))
                if len(platform_row) == 3:
                    buttons.append(platform_row)
                    platform_row = []
            if platform_row:
                buttons.append(platform_row)
        else:
            # Если ссылки не найдены — просто кнопка на оригинал
            buttons.append([InlineKeyboardButton(text="🔗 Слушать", url=track[5])])

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        if thumbnail:
            try:
                await bot.send_photo(
                    GROUP_ID,
                    photo=thumbnail,
                    caption=track_text,
                    reply_markup=keyboard,
                    parse_mode="HTML",
                )
            except Exception:
                fallback_text = f"{track_text}\n🔗 {track[5]}"
                await bot.send_message(
                    GROUP_ID,
                    fallback_text,
                    reply_markup=keyboard,
                    parse_mode="HTML",
                )
        else:
            fallback_text = f"{track_text}\n🔗 {track[5]}"
            await bot.send_message(
                GROUP_ID,
                fallback_text,
                reply_markup=keyboard,
                parse_mode="HTML",
            )

    # Номинации: отдельный блок по каждой номинации
    for nomination_id, nomination_name in session_nominations:
        nom_buttons = []
        for track in tracks:
            t_title = track[9] if len(track) > 9 and track[9] else ""
            t_artist = track[10] if len(track) > 10 and track[10] else ""
            label = format_track_label(t_artist, t_title, fallback=f"Трек #{track_seq[track[0]]}")
            nom_buttons.append([
                InlineKeyboardButton(
                    text=f"🏅 {short_button_text(label)}",
                    callback_data=f"nomvote_{session_id}_{nomination_id}_{track[0]}"
                )
            ])
        nom_text = f"🏅 <b>Номинация: {nomination_name}</b>\n\nВыбери трек-победитель:"
        await bot.send_message(
            GROUP_ID,
            nom_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nom_buttons),
            parse_mode="HTML"
        )

    # Финальный этап: лучший трек недели
    main_buttons = []
    for track in tracks:
        t_title = track[9] if len(track) > 9 and track[9] else ""
        t_artist = track[10] if len(track) > 10 and track[10] else ""
        label = format_track_label(t_artist, t_title, fallback=f"Трек #{track_seq[track[0]]}")
        main_buttons.append([
            InlineKeyboardButton(
                text=f"✅ {short_button_text(label)}",
                callback_data=f"mainvote_{session_id}_{track[0]}"
            )
        ])
    main_text = "🏆 <b>Финал: Лучший трек недели</b>\n\nВыбери победителя после голосования по номинациям."
    sent_main = await bot.send_message(
        GROUP_ID,
        main_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=main_buttons),
        parse_mode="HTML"
    )
    can_manage_pins = await bot_can_manage_pins(GROUP_ID)
    if not can_manage_pins:
        delete_group_pin(GROUP_ID, pin_type="voting_main")
        logging.warning("Skip pin voting message: insufficient rights in chat_id=%s", GROUP_ID)
        return True, "started"
    # Снимаем старое закрепление сбора треков
    collection_pin = get_group_pin(GROUP_ID, pin_type="collection_main")
    if collection_pin:
        try:
            await bot.unpin_chat_message(chat_id=GROUP_ID, message_id=collection_pin["message_id"])
        except Exception:
            pass
        finally:
            delete_group_pin(GROUP_ID, pin_type="collection_main")
    # Снимаем старое закрепление голосования (если было)
    stale_pin = get_group_pin(GROUP_ID, pin_type="voting_main")
    if stale_pin:
        try:
            await bot.unpin_chat_message(chat_id=GROUP_ID, message_id=stale_pin["message_id"])
        except Exception as e:
            logging.warning("Failed to unpin stale voting message: chat_id=%s message_id=%s err=%r", GROUP_ID, stale_pin["message_id"], e)
        finally:
            delete_group_pin(GROUP_ID, pin_type="voting_main")
    try:
        await bot.pin_chat_message(chat_id=GROUP_ID, message_id=sent_main.message_id, disable_notification=False)
        set_group_pin(GROUP_ID, sent_main.message_id, "voting_main", session_id)
    except Exception as e:
        logging.warning("Failed to pin voting message: chat_id=%s message_id=%s err=%r", GROUP_ID, sent_main.message_id, e)
    return True, "started"

async def finish_voting():
    session = get_current_session()
    if not session:
        return
    session_id = session[0]
    if session[3] != 'voting':
        return
    update_session_state(session_id, 'finished')
    voting_pin = get_group_pin(GROUP_ID, pin_type="voting_main")
    if voting_pin:
        can_manage_pins = await bot_can_manage_pins(GROUP_ID)
        if can_manage_pins:
            try:
                await bot.unpin_chat_message(chat_id=GROUP_ID, message_id=voting_pin["message_id"])
            except Exception as e:
                logging.warning(
                    "Failed to unpin voting message on finish: chat_id=%s message_id=%s err=%r",
                    GROUP_ID,
                    voting_pin["message_id"],
                    e,
                )
            finally:
                delete_group_pin(GROUP_ID, pin_type="voting_main")
        else:
            delete_group_pin(GROUP_ID, pin_type="voting_main")
            logging.warning("Skip unpin voting message: insufficient rights in chat_id=%s", GROUP_ID)
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

    save_main_winner(session_id, winner[0], winner[5])
    if winner[5] > 0:
        winner_user_id = get_track_user_id(winner[0])
        if winner_user_id:
            apply_points_event(
                winner_user_id,
                winner[2],
                winner[1],
                3,
                event_key=f"main_win:{session_id}:{winner_user_id}",
                event_type="main_win",
                session_id=session_id,
                is_win=True,
            )
        text += f"\n🎉 Победитель: <b>{winner[1]}</b> (+3 очка!)\n"

    # Победители номинаций
    nomination_winners = get_nomination_winners(session_id)
    if nomination_winners:
        text += "\n🏅 <b>Победители номинаций:</b>\n"
    for nomination_id, nom_name, track_id, full_name, username, track_url, votes in nomination_winners:
        if votes <= 0:
            text += f"• {nom_name}: никто не проголосовал\n"
            continue
        save_nomination_winner(session_id, nomination_id, nom_name, track_id, votes)
        winner_user_id = get_track_user_id(track_id)
        if winner_user_id:
            apply_points_event(
                winner_user_id,
                username,
                full_name,
                1,
                event_key=f"nom_win:{session_id}:{nomination_id}:{winner_user_id}",
                event_type="nomination_win",
                session_id=session_id,
            )
        text += f"• {nom_name}: <a href='{track_url}'>{full_name}</a> — {votes} голос(ов) (+1 очко)\n"

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

@dp.my_chat_member()
async def on_bot_added_to_chat(event: ChatMemberUpdated):
    try:
        chat = event.chat
        if chat.type not in ("group", "supergroup"):
            return
        if event.new_chat_member.user.id != bot.id:
            return
        if event.new_chat_member.status not in ("member", "administrator"):
            return
        chat_id = chat.id
        if is_group_welcomed(chat_id):
            return
        text = (
            "👋 Всем привет! Я <b>Track Day Bot</b>.\n\n"
            "Я помогаю проводить музыкальный баттл в группе:\n"
            "• собираю треки участников,\n"
            "• запускаю вечернее голосование,\n"
            "• считаю очки и веду историю победителей.\n\n"
            "Как участвовать:\n"
            "• напишите мне в личку (@track0_day_bot) и нажмите кнопку «🎵 Отправить трек» в среду с 10:00 до 22:00,\n"
            "• голосуйте в группе, когда откроется голосование."
        )
        await bot.send_message(chat_id, text, parse_mode="HTML")
        mark_group_welcomed(chat_id)
    except Exception as e:
        logging.warning("on_bot_added_to_chat failed: err=%r", e)

@dp.message(Command("start"))
async def cmd_start(message: Message):
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
    is_admin = message.from_user.id == ADMIN_ID
    menu_markup = build_main_menu_keyboard(is_admin=is_admin) if message.chat.type == "private" else None
    if is_admin:
        await message.answer(
            "👋 Привет, админ! Я бот для <b>Track Day</b>!\n\n"
            "Используй кнопки меню ниже для управления ботом.\n\n"
            f"📊 <a href='{sheet_url}'>Таблица лидеров онлайн</a>",
            parse_mode="HTML",
            reply_markup=menu_markup,
        )
    else:
        await message.answer(
            "👋 Привет! Я бот для <b>Track Day</b>!\n\n"
            "Каждую среду мы выбираем лучший трек недели 🎵\n\n"
            "Используй кнопки меню ниже 👇\n\n"
            f"📊 <a href='{sheet_url}'>Таблица лидеров онлайн</a>",
            parse_mode="HTML",
            reply_markup=menu_markup,
        )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    is_admin = message.from_user.id == ADMIN_ID
    menu_markup = build_main_menu_keyboard(is_admin=is_admin) if message.chat.type == "private" else None
    text = (
        "🎵 <b>Track Day</b>\n\n"
        "Используй кнопки меню для навигации.\n"
        "Приём треков — только в среду с 10:00 до 22:00."
    )
    await message.answer(text, parse_mode="HTML", reply_markup=menu_markup)

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
async def themes_add_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(ThemeStates.waiting_theme_admin)
    await callback.message.answer(
        "✏️ Напиши новую тему и отправь мне.\n\n"
        "Можно добавить эмодзи в начале, например:\n"
        "<i>🔥 Трек который слушаешь перед важным делом</i>",
        parse_mode="HTML",
        reply_markup=build_cancel_keyboard(),
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
async def themes_delete_menu(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(ThemeStates.waiting_delete_theme_id)
    await callback.message.answer(
        "🗑 Напиши <b>номер темы</b> которую хочешь удалить.\n"
        "Номер можно найти в списке тем (кнопка «Просмотреть все темы»).",
        parse_mode="HTML",
        reply_markup=build_cancel_keyboard(),
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

@dp.message(Command("nominations"))
async def cmd_nominations(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.chat.type != "private":
        await message.answer("🔒 Управление номинациями только в личке!")
        return
    nominations = get_nominations(active_only=False)
    active_count = sum(1 for _, _, is_active in nominations if is_active)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить номинацию", callback_data="nom_add")],
        [InlineKeyboardButton(text="📋 Список номинаций", callback_data="nom_list_0")],
        [InlineKeyboardButton(text="🗑 Удалить номинацию", callback_data="nom_del_menu")],
    ])
    await message.answer(
        f"🏅 <b>Управление номинациями</b>\n\n"
        f"Всего: <b>{len(nominations)}</b>\n"
        f"Активных: <b>{active_count}</b>\n\n"
        "Что делаем?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "nom_add")
async def nominations_add_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(NominationStates.waiting_nomination_name)
    await callback.message.answer(
        "✏️ Напиши название новой номинации.",
        parse_mode="HTML",
        reply_markup=build_cancel_keyboard(),
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("nom_list_"))
async def nominations_list(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    page = int(callback.data.split("_")[2])
    text, markup = render_nominations_page(page)
    await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    await callback.answer()

def render_nominations_page(page):
    nominations = get_nominations(active_only=False)
    per_page = 10
    total_pages = (len(nominations) + per_page - 1) // per_page
    start = page * per_page
    page_rows = nominations[start:start + per_page]
    if not nominations:
        return (
            "🏅 Номинаций пока нет.",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="nom_back")]
            ])
        )
    text = f"🏅 <b>Номинации (страница {page + 1}/{max(1, total_pages)}):</b>\n\n"
    buttons = []
    for idx, (nomination_id, name, is_active) in enumerate(page_rows, start + 1):
        status = "✅" if is_active else "⛔"
        text += f"{status} <b>{idx}.</b> {name}\n"
        toggle_label = "⛔ Выключить" if is_active else "✅ Включить"
        buttons.append([InlineKeyboardButton(text=f"{toggle_label}: {idx}", callback_data=f"nom_toggle_{nomination_id}_{page}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"nom_list_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"nom_list_{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="nom_back")])
    return text, InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.callback_query(F.data.startswith("nom_toggle_"))
async def nominations_toggle(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    parts = callback.data.split("_")
    nomination_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    new_state = toggle_nomination(nomination_id)
    if new_state is None:
        await callback.answer("❌ Номинация не найдена", show_alert=True)
        return
    state_text = "✅ Номинация включена" if new_state == 1 else "⛔ Номинация выключена"
    await callback.answer(state_text, show_alert=False)
    text, markup = render_nominations_page(page)
    await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")

@dp.callback_query(F.data == "nom_del_menu")
async def nominations_delete_menu(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(NominationStates.waiting_nomination_delete_id)
    await callback.message.answer(
        "🗑 Напиши <b>номер номинации</b> из списка (кнопка «📋 Список номинаций»).",
        parse_mode="HTML",
        reply_markup=build_cancel_keyboard(),
    )
    await callback.answer()

@dp.callback_query(F.data == "nom_back")
async def nominations_back(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    nominations = get_nominations(active_only=False)
    active_count = sum(1 for _, _, is_active in nominations if is_active)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить номинацию", callback_data="nom_add")],
        [InlineKeyboardButton(text="📋 Список номинаций", callback_data="nom_list_0")],
        [InlineKeyboardButton(text="🗑 Удалить номинацию", callback_data="nom_del_menu")],
    ])
    await callback.message.edit_text(
        f"🏅 <b>Управление номинациями</b>\n\n"
        f"Всего: <b>{len(nominations)}</b>\n"
        f"Активных: <b>{active_count}</b>\n\n"
        "Что делаем?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@dp.message(Command("cancel"))
@dp.message(F.chat.type == "private", F.text == MENU_CANCEL_TEXT)
async def cmd_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    is_admin = message.from_user.id == ADMIN_ID
    menu = build_main_menu_keyboard(is_admin=is_admin)
    if current_state:
        data = await state.get_data()
        pending_token = data.get("pending_token")
        if pending_token:
            delete_submit_candidates(pending_token)
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=menu)
    else:
        await message.answer("Нет активного действия для отмены.", reply_markup=menu)

# ==================== SUBMIT ====================

async def build_candidates_for_input(source_type: str, raw_value: str):
    if source_type == "url":
        raw_url = (raw_value or "").strip()
        if not raw_url:
            return []
        song_data = await get_song_links(raw_url)
        metadata = song_data.get("metadata", {}) if isinstance(song_data, dict) else {}
        candidate = {
            "track_url": metadata.get("canonical_url") or raw_url,
            "title": metadata.get("title", "") or "",
            "artist": metadata.get("artist", "") or "",
            "thumbnail_url": metadata.get("thumbnail_url", "") or "",
        }
        return [candidate]
    if source_type == "text":
        return await search_track_candidates_multiquery([raw_value], limit=3)
    return []


async def send_candidates_prompt(
    message: Message,
    state: FSMContext,
    session_id: int,
    source_type: str,
    query_text: str,
    description: str,
    candidates,
):
    token = upsert_submit_candidates(
        user_id=message.from_user.id,
        session_id=session_id,
        source_type=source_type,
        query_text=query_text,
        description=description or "",
        candidates=candidates,
    )
    lines = ["Выбери подходящий трек:"]
    for idx, candidate in enumerate(candidates, start=1):
        label = format_candidate_label(candidate, fallback=f"Вариант {idx}")
        lines.append(f"{idx}. {label}")
    await message.answer("\n".join(lines), reply_markup=build_submit_candidates_keyboard(token, candidates))
    await state.set_state(SubmitStates.waiting_candidate_choice)
    await state.update_data(session_id=session_id, pending_token=token, source_type=source_type)

@dp.message(Command("addtheme"))
async def cmd_addtheme(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("📩 Темы добавляю только в личке! Напиши мне: @track0_day_bot")
        return
    await state.set_state(ThemeStates.waiting_theme_user)
    await message.answer(
        "✏️ Напиши тему и отправь мне!\n\n"
        "Просто текст без эмодзи, например:\n"
        "<i>Песня которую слушал в детстве</i>",
        parse_mode="HTML",
        reply_markup=build_cancel_keyboard(),
    )

@dp.message(Command("submit"))
async def cmd_submit(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("📩 Треки принимаю только в личке! Напиши мне сюда: @track0_day_bot")
        return
    now = datetime.now()
    if now.weekday() != 2 or now.hour < 10 or now.hour >= 22:
        await message.answer(
            "⏰ Приём треков открыт только в <b>среду с 10:00 до 22:00</b>.\n"
            "Возвращайся в следующую среду!",
            parse_mode="HTML"
        )
        return
    session, created_now, status = get_or_create_collecting_session_for_submit()
    if status == "voting":
        await message.answer("⏰ Приём треков уже закрыт, идёт голосование!")
        return
    if not session:
        await message.answer("❌ Не удалось открыть сессию приёма треков. Попробуй чуть позже.")
        return
    if created_now:
        await message.answer(
            f"✅ Открыл новый сбор треков: <b>{session[1]}</b>\n"
            f"Тема: <b>{session[2]}</b>",
            parse_mode="HTML",
        )
    existing = get_user_track_in_session(message.from_user.id, session[0])
    if existing:
        await message.answer(
            f"Ты уже скинул трек на этой неделе:\n{existing[5]}\n\nХочешь заменить? Просто пришли новую ссылку."
        )
    await message.answer(
        f"🎵 Тема недели: <b>{session[2]}</b>\n\n"
        "Пришли ссылку на трек (Spotify, YouTube, VK Music, Tidal — любая)\n"
        "Или просто напиши название трека (например: Rick Astley - Never Gonna Give You Up) 👇",
        parse_mode="HTML"
    )
    old_data = await state.get_data()
    old_token = old_data.get("pending_token")
    if old_token:
        delete_submit_candidates(old_token)
    await state.set_state(SubmitStates.waiting_track_input)
    await state.update_data(session_id=session[0], pending_token=None, source_type=None)

@dp.message(F.chat.type == "private", F.text == MENU_SUBMIT_TEXT)
async def menu_submit_button(message: Message, state: FSMContext):
    await cmd_submit(message, state)

@dp.message(F.chat.type == "private", F.text == MENU_MY_TRACK_TEXT)
async def menu_mytrack_button(message: Message):
    await cmd_mytrack(message)

@dp.message(F.chat.type == "private", F.text == MENU_STATS_TEXT)
async def menu_stats_button(message: Message):
    await cmd_mystats(message)

@dp.message(F.chat.type == "private", F.text == MENU_LEADERBOARD_TEXT)
async def menu_leaderboard_button(message: Message):
    await cmd_leaderboard(message)

@dp.message(F.chat.type == "private", F.text == MENU_HISTORY_TEXT)
async def menu_history_button(message: Message):
    await cmd_history(message)

@dp.message(F.chat.type == "private", F.text == MENU_ADDTHEME_TEXT)
async def menu_addtheme_button(message: Message, state: FSMContext):
    await cmd_addtheme(message, state)

@dp.message(F.chat.type == "private", F.text == MENU_ADMIN_TEXT)
async def menu_admin_button(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("⚙️ <b>Панель управления</b>", parse_mode="HTML", reply_markup=build_admin_panel_keyboard())

@dp.callback_query(F.data == "admin_themes")
async def cb_admin_themes(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.answer()
    total, unused = get_themes_count()
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить тему", callback_data="themes_add")],
        [InlineKeyboardButton(text="📋 Просмотреть все темы", callback_data="themes_list_0")],
        [InlineKeyboardButton(text="🗑 Удалить тему", callback_data="themes_delete_menu")],
        [InlineKeyboardButton(text="🔄 Сбросить флаги использования", callback_data="themes_reset")],
    ])
    await callback.message.answer(
        f"🎛 <b>Управление темами</b>\n\n"
        f"📊 Всего тем: <b>{total}</b>\n"
        f"✅ Неиспользованных: <b>{unused}</b>\n\n"
        "Что хочешь сделать?",
        reply_markup=keyboard,
        parse_mode="HTML",
    )

@dp.callback_query(F.data == "admin_nominations")
async def cb_admin_nominations(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.answer()
    nominations = get_nominations(active_only=False)
    active_count = sum(1 for _, _, is_active in nominations if is_active)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить номинацию", callback_data="nom_add")],
        [InlineKeyboardButton(text="📋 Список номинаций", callback_data="nom_list_0")],
        [InlineKeyboardButton(text="🗑 Удалить номинацию", callback_data="nom_del_menu")],
    ])
    await callback.message.answer(
        f"🏅 <b>Управление номинациями</b>\n\n"
        f"Всего: <b>{len(nominations)}</b>\n"
        f"Активных: <b>{active_count}</b>\n\n"
        "Что делаем?",
        reply_markup=keyboard,
        parse_mode="HTML",
    )

@dp.callback_query(F.data == "admin_startcollection")
async def cb_admin_startcollection(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.answer()
    try:
        base_week = get_week_base()
        active = get_active_week_session(base_week)
        if active:
            await callback.message.answer(f"⚠️ Сессия уже существует (состояние: {active[3]}). Сначала заверши текущую.")
            return
        theme = get_random_theme()
        week_sessions = list_week_sessions(base_week)
        if not week_sessions:
            new_week = base_week
        else:
            suffix = next_week_suffix(base_week)
            new_week = base_week if suffix is None else f"{base_week}-{suffix}"
        session_id = create_session(theme, week_override=new_week)
        conn = sqlite3.connect("trackday.db")
        c = conn.cursor()
        c.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
        session = c.fetchone()
        conn.close()
        if session:
            sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
            text = (
                f"🎵 <b>TRACK DAY!</b>\n\n"
                f"Тема этой недели: <b>{session[2]}</b>\n\n"
                f"Скидывайте свои треки мне в <b>личку</b> (@track0_day_bot) до 22:00 🎧\n\n"
                f"📌 Правила:\n"
                f"• Один трек от каждого\n"
                f"• Ссылка + можно описание почему именно этот\n"
                f"• Треки будут анонимными до результатов\n\n"
                f"🏆 <a href='{sheet_url}'>Таблица лидеров</a> | За участие: +1 очко"
            )
            await bot.send_message(GROUP_ID, text, parse_mode="HTML")
            await callback.message.answer(f"✅ Сбор треков запущен!\nНеделя: {session[1]}\nТема: {session[2]}")
        else:
            await callback.message.answer("❌ Ошибка — сессия не создалась. Проверь лог.")
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")
        logging.error(f"cb_admin_startcollection error: {e}")

@dp.callback_query(F.data == "admin_startvoting")
async def cb_admin_startvoting(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.answer()
    try:
        session = get_current_session()
        if not session:
            await callback.message.answer("❌ Нет активной сессии. Сначала запусти сбор треков (⚙️ Управление → Запустить сбор).")
            return
        started, status = await start_voting()
        if started:
            await callback.message.answer("✅ Голосование запущено!")
        elif status == "already_voting":
            await callback.message.answer("⚠️ Голосование уже запущено.")
        elif status == "already_finished":
            await callback.message.answer("⚠️ Голосование уже завершено.")
        elif status == "no_tracks":
            await callback.message.answer("⚠️ Нет треков для голосования.")
        else:
            await callback.message.answer("⚠️ Запуск голосования недоступен в текущем состоянии сессии.")
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")
        logging.error(f"cb_admin_startvoting error: {e}")

@dp.callback_query(F.data == "admin_finishvoting")
async def cb_admin_finishvoting(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.answer()
    try:
        session = get_current_session()
        if not session:
            await callback.message.answer("❌ Нет активной сессии.")
            return
        await finish_voting()
        await callback.message.answer("✅ Голосование завершено!")
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")
        logging.error(f"cb_admin_finishvoting error: {e}")

@dp.callback_query(F.data == "admin_updatesheets")
async def cb_admin_updatesheets(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.answer()
    try:
        update_leaderboard_sheet()
        sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        await callback.message.answer(f"✅ Таблица обновлена! <a href='{sheet_url}'>Открыть</a>", parse_mode="HTML")
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")
        logging.error(f"cb_admin_updatesheets error: {e}")


@dp.message(F.chat.type == "private", F.text, ~F.text.startswith("/"))
async def handle_private_message(message: Message, state: FSMContext):
    user_id = message.from_user.id
    message_text = (message.text or "").strip()
    current_state = await state.get_state()

    # Обработка добавления темы
    if current_state == ThemeStates.waiting_theme_admin.state and user_id == ADMIN_ID:
        if not message_text:
            await message.answer("❌ Тема не должна быть пустой. Напиши текст темы или нажми «❌ Отмена».")
            return
        theme_with_emoji = add_emoji_to_theme(message_text)
        if add_theme_to_db(theme_with_emoji, user_id=user_id, user_name=message.from_user.full_name):
            total, unused = get_themes_count()
            await message.answer(
                f"✅ Тема добавлена!\n\n<b>{theme_with_emoji}</b>\n\n"
                f"Всего тем: {total} | Неиспользованных: {unused}\n\n"
                "Пиши следующую тему или нажми «❌ Отмена» для выхода.",
                parse_mode="HTML"
            )
        else:
            await message.answer("❌ Такая тема уже есть! Пиши следующую или нажми «❌ Отмена».")
        return

    if current_state == ThemeStates.waiting_theme_user.state:
        if not message_text:
            await message.answer("❌ Тема не должна быть пустой. Напиши текст темы или нажми «❌ Отмена».")
            return
        if len(message_text) < 5:
            await message.answer("❌ Тема слишком короткая. Нужно минимум 5 символов.")
            return
        theme_with_emoji = add_emoji_to_theme(message_text)
        if add_theme_to_db(theme_with_emoji, user_id=user_id, user_name=message.from_user.full_name):
            is_admin = user_id == ADMIN_ID
            await message.answer(
                f"✅ Тема принята!\n\n<b>{theme_with_emoji}</b>\n\n"
                "Спасибо за идею 💡",
                parse_mode="HTML",
                reply_markup=build_main_menu_keyboard(is_admin=is_admin),
            )
            await state.clear()
        else:
            await message.answer("❌ Такая тема уже есть! Попробуй другую или нажми «❌ Отмена».")
        return

    if current_state == NominationStates.waiting_nomination_name.state and user_id == ADMIN_ID:
        if not message_text:
            await message.answer("❌ Название номинации не должно быть пустым.")
            return
        if len(message_text) < 3:
            await message.answer("❌ Название слишком короткое (минимум 3 символа).")
            return
        if add_nomination(message_text):
            await message.answer(
                f"✅ Номинация добавлена: <b>{message_text}</b>\n\n"
                "Добавь следующую или нажми «❌ Отмена».",
                parse_mode="HTML"
            )
        else:
            await message.answer("❌ Такая номинация уже есть.")
        return

    if current_state == NominationStates.waiting_nomination_delete_id.state and user_id == ADMIN_ID:
        try:
            seq_num = int(message_text)
        except ValueError:
            await message.answer("❌ Введи номер номинации числом.")
            return
        result = get_nomination_db_id_by_seq(seq_num)
        if not result:
            await message.answer("❌ Номинации с таким номером нет.")
            return
        nomination_id, nom_name = result
        if delete_nomination(nomination_id):
            await message.answer(f"✅ Номинация удалена: <b>{nom_name}</b>", parse_mode="HTML")
        else:
            await message.answer("❌ Не удалось удалить номинацию.")
        return

    # Обработка удаления темы
    if current_state == ThemeStates.waiting_delete_theme_id.state and user_id == ADMIN_ID:
        try:
            seq_num = int(message_text)
            result = get_theme_db_id_by_seq(seq_num)
            if result:
                db_id, theme_text = result
                delete_theme_from_db(db_id)
                total, unused = get_themes_count()
                await message.answer(
                    f"✅ Тема #{seq_num} удалена:\n<i>{theme_text}</i>\n\n"
                    f"Осталось тем: {total}\n"
                    "Ещё удалить? Пиши номер или нажми «❌ Отмена».",
                    parse_mode="HTML"
                )
            else:
                await message.answer("❌ Темы с таким номером нет. Проверь список через кнопку «📋 Просмотреть все темы».")
        except ValueError:
            await message.answer("❌ Введи число — порядковый номер из списка.")
        return

    # Обработка трека
    if current_state in (SubmitStates.waiting_candidate_choice.state, SubmitStates.waiting_confirmation.state):
        await message.answer("Выбери вариант кнопками ниже.")
        return
    if current_state != SubmitStates.waiting_track_input.state:
        is_admin = user_id == ADMIN_ID
        await message.answer(
            "Выбери действие из меню 👇",
            reply_markup=build_main_menu_keyboard(is_admin=is_admin),
        )
        return

    data = await state.get_data()
    session_id = data.get("session_id")
    if not session_id:
        await state.clear()
        await message.answer("❌ Сессия отправки устарела. Нажми «🎵 Отправить трек» снова.")
        return
    session_row = get_current_session()
    if not session_row or session_row[0] != session_id or session_row[3] != "collecting":
        rebound_session, created_now, status = get_or_create_collecting_session_for_submit()
        if status == "voting":
            pending_token = data.get("pending_token")
            if pending_token:
                delete_submit_candidates(pending_token)
            await state.clear()
            await message.answer("⏰ Приём треков уже закрыт, идёт голосование!")
            return
        if not rebound_session:
            pending_token = data.get("pending_token")
            if pending_token:
                delete_submit_candidates(pending_token)
            await state.clear()
            await message.answer("❌ Не удалось восстановить сессию отправки. Нажми «🎵 Отправить трек» снова.")
            return
        session_id = rebound_session[0]
        await state.update_data(session_id=session_id)
        if created_now:
            await message.answer(
                f"✅ Открыл новый сбор треков: <b>{rebound_session[1]}</b>\n"
                f"Тема: <b>{rebound_session[2]}</b>",
                parse_mode="HTML",
            )
    session_row = get_current_session()
    if not session_row or session_row[0] != session_id or session_row[3] != "collecting":
        pending_token = data.get("pending_token")
        if pending_token:
            delete_submit_candidates(pending_token)
        await state.clear()
        await message.answer("❌ Сессия отправки недоступна. Нажми «🎵 Отправить трек» снова.")
        return

    text = message.text or ""
    url_match = re.search(r"https?://\S+", text)
    description = ""
    source_type = "text"
    query_value = text.strip()

    if url_match:
        source_type = "url"
        track_url = url_match.group(0).rstrip(".,;!?)]")
        description = text.replace(url_match.group(0), "", 1).strip()
        query_value = track_url
        await message.answer("⏳ Проверяю ссылку и готовлю карточку подтверждения...")
    else:
        if len(query_value) < 3:
            await message.answer(
                "❌ Слишком короткий текст. Пришли ссылку или название трека (например: исполнитель - название)."
            )
            return
        await message.answer("🔎 Ищу варианты трека...")

    candidates = await build_candidates_for_input(source_type, query_value)
    if not candidates:
        await message.answer(
            "❌ Не удалось подобрать трек. Попробуй точнее (исполнитель - название) или отправь прямую ссылку."
        )
        return
    await send_candidates_prompt(
        message=message,
        state=state,
        session_id=session_id,
        source_type=source_type,
        query_text=query_value,
        description=description,
        candidates=candidates[:3],
    )


@dp.callback_query(F.data.startswith("cand_"))
async def handle_candidate_pick(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("Некорректный формат выбора", show_alert=True)
        return
    token, idx_raw = parts[1], parts[2]
    if not idx_raw.isdigit():
        await callback.answer("Некорректный индекс", show_alert=True)
        return
    idx = int(idx_raw)
    pending = get_submit_candidates(token)
    if not pending:
        await state.clear()
        await callback.answer("Время выбора истекло. Нажми «🎵 Отправить трек» снова.", show_alert=True)
        return
    if pending["user_id"] != callback.from_user.id:
        await callback.answer("Это не твой выбор трека.", show_alert=True)
        return
    candidates = pending["candidates"]
    if idx < 0 or idx >= len(candidates):
        await callback.answer("Вариант недоступен", show_alert=True)
        return
    set_submit_selected_index(token, idx)
    selected = candidates[idx]
    label = format_candidate_label(selected, fallback="Неизвестный трек")
    track_url = selected.get("track_url", "")
    await state.set_state(SubmitStates.waiting_confirmation)
    await state.update_data(session_id=pending["session_id"], pending_token=token, source_type=pending["source_type"])
    await callback.message.answer(
        f"Найдено:\n🎵 <b>{label}</b>\n🔗 {track_url}\n\nПодтверждаешь сохранение?",
        parse_mode="HTML",
        reply_markup=build_submit_confirm_keyboard(token),
    )
    await callback.answer("Выбран вариант")


@dp.callback_query(F.data.startswith("retryfind_"))
async def handle_candidate_retry(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) != 2:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    token = parts[1]
    pending = get_submit_candidates(token)
    if not pending:
        await state.clear()
        await callback.answer("Время выбора истекло. Нажми «🎵 Отправить трек» снова.", show_alert=True)
        return
    if pending["user_id"] != callback.from_user.id:
        await callback.answer("Это не твой выбор трека.", show_alert=True)
        return
    candidates = pending["candidates"][:3]
    if not candidates:
        await callback.answer("Список кандидатов пуст.", show_alert=True)
        return
    lines = ["Выбери подходящий трек:"]
    for idx, candidate in enumerate(candidates, start=1):
        lines.append(f"{idx}. {format_candidate_label(candidate, fallback=f'Вариант {idx}')}")
    await state.set_state(SubmitStates.waiting_candidate_choice)
    await state.update_data(session_id=pending["session_id"], pending_token=token, source_type=pending["source_type"])
    await callback.message.answer(
        "\n".join(lines),
        reply_markup=build_submit_candidates_keyboard(token, candidates),
    )
    await callback.answer("Выбери другой вариант")


@dp.callback_query(F.data.startswith("cancel_submit_"))
async def handle_submit_cancel(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    token = parts[2]
    pending = get_submit_candidates(token)
    if pending and pending["user_id"] != callback.from_user.id:
        await callback.answer("Это не твоя заявка.", show_alert=True)
        return
    delete_submit_candidates(token)
    await state.clear()
    await callback.message.answer("❌ Отправка трека отменена. Нажми «🎵 Отправить трек» чтобы начать заново.")
    await callback.answer("Отменено")


@dp.callback_query(F.data.startswith("confirm_"))
async def handle_submit_confirm(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) != 2:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    token = parts[1]
    pending = get_submit_candidates(token)
    if not pending:
        await state.clear()
        await callback.answer("Время подтверждения истекло. Нажми «🎵 Отправить трек» снова.", show_alert=True)
        return
    if pending["user_id"] != callback.from_user.id:
        await callback.answer("Это не твоя заявка.", show_alert=True)
        return
    selected_index = pending["selected_index"]
    candidates = pending["candidates"]
    if selected_index is None or selected_index < 0 or selected_index >= len(candidates):
        await callback.answer("Сначала выбери вариант трека.", show_alert=True)
        return
    session_row = get_current_session()
    if not session_row or session_row[0] != pending["session_id"] or session_row[3] != "collecting":
        delete_submit_candidates(token)
        await state.clear()
        await callback.answer("⏰ Приём треков уже закрыт.", show_alert=True)
        return
    selected = candidates[selected_index]
    track_url = selected.get("track_url", "")
    if not track_url:
        await callback.answer("У выбранного варианта нет ссылки.", show_alert=True)
        return
    try:
        await callback.answer("⏳ Сохраняю трек...")
    except TelegramBadRequest as e:
        logging.warning(
            "Confirm callback ack failed (query likely expired): user_id=%s token=%s err=%r",
            callback.from_user.id,
            token,
            e,
        )
    await callback.message.answer("⏳ Подтверждаю и сохраняю трек...")
    status, links_info, track_label, accepted_url = await save_or_update_track_submission(
        session_id=pending["session_id"],
        user_id=callback.from_user.id,
        username=callback.from_user.username or "",
        full_name=callback.from_user.full_name,
        track_url=track_url,
        description=pending["description"],
    )
    if status == "updated":
        await callback.message.answer(
            f"✅ Трек обновлён!\n"
            f"🎵 <b>{track_label}</b>\n"
            f"🔗 {accepted_url}\n\n"
            f"{links_info}\n\n"
            f"Ждём голосования в 22:00",
            parse_mode="HTML",
            reply_markup=build_replace_track_keyboard(pending["session_id"]),
        )
    else:
        await callback.message.answer(
            f"✅ Трек принят! Спасибо 🎵\n"
            f"🎵 <b>{track_label}</b>\n"
            f"🔗 {accepted_url}\n\n"
            f"{links_info}\n\n"
            "Голосование начнётся сегодня в 22:00\n"
            "Твой трек будет анонимным до объявления результатов 🕵️",
            parse_mode="HTML",
            reply_markup=build_replace_track_keyboard(pending["session_id"]),
        )
    delete_submit_candidates(token)
    await state.clear()


@dp.message(Command("mytrack"))
async def cmd_mytrack(message: Message):
    if message.chat.type != "private":
        await message.answer("📩 Эта функция доступна только в личке с ботом.")
        return
    session = get_current_session()
    if not session:
        await message.answer("Сейчас нет активной сессии.")
        return
    track = get_user_track_in_session(message.from_user.id, session[0])
    if not track:
        await message.answer("У тебя пока нет трека в текущей сессии. Нажми «🎵 Отправить трек».")
        return
    label = format_track_label(track[10] if len(track) > 10 else "", track[9] if len(track) > 9 else "", fallback=track[5])
    description = (track[6] or "").strip()
    text = (
        f"🎵 <b>Твой трек</b>\n\n"
        f"Неделя: <b>{session[1]}</b>\n"
        f"Тема: <b>{session[2]}</b>\n"
        f"Трек: <b>{label}</b>\n"
        f"Ссылка: {track[5]}"
    )
    if description:
        text += f"\nОписание: {description}"
    allow_delete = session[3] == "collecting"
    if not allow_delete:
        text += "\n\nУдаление закрыто: сбор уже завершён."
    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=build_mytrack_keyboard(session[0], allow_delete=allow_delete),
    )


@dp.callback_query(F.data.startswith("my_replace_"))
async def cb_mytrack_replace(callback: CallbackQuery, state: FSMContext):
    try:
        session_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные", show_alert=True)
        return
    session = get_current_session()
    if not session or session[0] != session_id:
        await callback.answer("Сессия уже недоступна.", show_alert=True)
        return
    if session[3] != "collecting":
        await callback.answer("Приём треков уже закрыт", show_alert=True)
        return
    if not get_user_track_in_session(callback.from_user.id, session_id):
        await callback.answer("У тебя нет трека в этой сессии", show_alert=True)
        return
    await state.set_state(SubmitStates.waiting_track_input)
    await state.update_data(session_id=session_id, pending_token=None, source_type=None)
    await callback.message.answer("🔁 Пришли новую ссылку, текст или скриншот. Сначала покажу варианты, потом подтверждение.")
    await callback.answer("Режим замены включен")


@dp.callback_query(F.data.startswith("my_delete_"))
async def cb_mytrack_delete(callback: CallbackQuery, state: FSMContext):
    try:
        session_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные", show_alert=True)
        return
    session = get_current_session()
    if not session or session[0] != session_id:
        await callback.answer("Сессия уже недоступна.", show_alert=True)
        return
    if session[3] != "collecting":
        await callback.answer("Удаление доступно только во время сбора.", show_alert=True)
        return
    deleted = delete_user_track_in_session(callback.from_user.id, session_id)
    data = await state.get_data()
    pending_token = data.get("pending_token")
    if pending_token:
        delete_submit_candidates(pending_token)
    await state.clear()
    if deleted:
        await callback.message.answer("🗑 Твой трек удалён. Можешь отправить новый — нажми «🎵 Отправить трек».")
        await callback.answer("Удалено")
    else:
        await callback.answer("Трек не найден", show_alert=True)


@dp.callback_query(F.data == "my_close")
async def cb_mytrack_close(callback: CallbackQuery):
    await callback.answer("Ок")

@dp.callback_query(F.data.startswith("replace_track_"))
async def handle_replace_track(callback: CallbackQuery, state: FSMContext):
    if callback.message.chat.type != "private":
        await callback.answer("Заменять трек можно только в личке с ботом", show_alert=True)
        return
    try:
        session_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные кнопки", show_alert=True)
        return

    session = get_current_session()
    if not session or session[0] != session_id:
        await callback.answer("Сессия уже недоступна для замены", show_alert=True)
        return
    if session[3] != "collecting":
        await callback.answer("Приём треков уже закрыт", show_alert=True)
        return
    existing = get_user_track_in_session(callback.from_user.id, session_id)
    if not existing:
        await callback.answer("У тебя нет трека в этой сессии", show_alert=True)
        return

    await state.set_state(SubmitStates.waiting_track_input)
    await state.update_data(session_id=session_id, pending_token=None, source_type=None)
    await callback.message.answer(
        "🔁 Режим замены включен.\n\nПришли новую ссылку, текст или скриншот."
    )
    await callback.answer("Ожидаю новый трек")

def build_history_page(page: int):
    per_page = 10
    rows, total = get_history_page(page=page, per_page=per_page)
    total_pages = (total + per_page - 1) // per_page
    if not rows:
        return (
            "📚 <b>История победителей</b>\n\nПока нет завершённых недель.",
            InlineKeyboardMarkup(inline_keyboard=[])
        )
    text = f"📚 <b>История победителей</b> (страница {page + 1}/{max(1, total_pages)})\n\n"
    for week, theme, winner_name, track_url, votes, track_artist, track_title in rows:
        track_label = format_track_label(track_artist, track_title, fallback="Неизвестный трек")
        text += (
            f"📅 <b>{week}</b>\n"
            f"Тема: {theme}\n"
            f"Победитель: <a href='{track_url}'>{winner_name}</a>\n"
            f"Трек: <b>{track_label}</b>\n"
            f"Голоса: {votes}\n\n"
        )
    buttons = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"history_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"history_{page+1}"))
    if nav:
        buttons.append(nav)
    return text, InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.callback_query(F.data.startswith("nomvote_"))
async def handle_nomination_vote(callback: CallbackQuery):
    parts = callback.data.split("_")
    session_id = int(parts[1])
    nomination_id = int(parts[2])
    track_id = int(parts[3])
    voter_id = callback.from_user.id
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT state FROM sessions WHERE id = ?", (session_id,))
    session = c.fetchone()
    conn.close()
    if not session or session[0] != "voting":
        await callback.answer("❌ Голосование уже закрыто.", show_alert=True)
        return
    if not is_nomination_in_session(nomination_id, session_id):
        await callback.answer("❌ Номинация недоступна для этой недели.", show_alert=True)
        return
    if not is_track_in_session(track_id, session_id):
        await callback.answer("❌ Трек не найден в текущей неделе.", show_alert=True)
        return
    own_track = get_user_track_in_session(voter_id, session_id)
    if own_track and own_track[0] == track_id:
        await callback.answer("Нельзя голосовать за свой трек 😄", show_alert=True)
        return
    status = add_nomination_vote(session_id, nomination_id, voter_id, track_id)
    if status == "new":
        await callback.answer("✅ Голос в номинации принят")
    elif status == "updated":
        await callback.answer("🔁 Голос в номинации обновлён")
    else:
        await callback.answer("❌ Не удалось сохранить голос", show_alert=True)

@dp.callback_query(F.data.startswith("mainvote_"))
async def handle_main_vote(callback: CallbackQuery):
    parts = callback.data.split("_")
    session_id = int(parts[1])
    track_id = int(parts[2])
    voter_id = callback.from_user.id
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT state FROM sessions WHERE id = ?", (session_id,))
    session = c.fetchone()
    conn.close()
    if not session or session[0] != "voting":
        await callback.answer("❌ Голосование уже закончилось!", show_alert=True)
        return
    if not is_track_in_session(track_id, session_id):
        await callback.answer("❌ Трек не найден в текущей неделе.", show_alert=True)
        return
    if not user_completed_all_nominations(session_id, voter_id):
        await callback.answer("Сначала выбери победителей во всех номинациях 🏅", show_alert=True)
        return
    own_track = get_user_track_in_session(voter_id, session_id)
    if own_track and own_track[0] == track_id:
        await callback.answer("Нельзя голосовать за свой трек 😄", show_alert=True)
        return
    vote_status = add_vote(session_id, voter_id, track_id)
    if vote_status == "new":
        apply_points_event(
            voter_id,
            callback.from_user.username or "",
            callback.from_user.full_name,
            1,
            event_key=f"main_vote:{session_id}:{voter_id}",
            event_type="main_vote",
            session_id=session_id,
        )
        await callback.answer("✅ Голос принят! +1 очко тебе 🏆", show_alert=True)
    elif vote_status == "updated":
        await callback.answer("🔁 Голос обновлён", show_alert=True)
    else:
        await callback.answer("❌ Не удалось сохранить голос", show_alert=True)

@dp.callback_query(F.data.startswith("vote_"))
async def handle_legacy_vote(callback: CallbackQuery):
    # Backward compatibility for old messages with vote_{session_id}_{track_id}
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("❌ Некорректный формат голоса", show_alert=True)
        return
    session_id = int(parts[1])
    track_id = int(parts[2])
    voter_id = callback.from_user.id
    conn = sqlite3.connect("trackday.db")
    c = conn.cursor()
    c.execute("SELECT state FROM sessions WHERE id = ?", (session_id,))
    session = c.fetchone()
    conn.close()
    if not session or session[0] != "voting":
        await callback.answer("❌ Голосование уже закончилось!", show_alert=True)
        return
    if not is_track_in_session(track_id, session_id):
        await callback.answer("❌ Трек не найден в текущей неделе.", show_alert=True)
        return
    if not user_completed_all_nominations(session_id, voter_id):
        await callback.answer("Сначала выбери победителей во всех номинациях 🏅", show_alert=True)
        return
    own_track = get_user_track_in_session(voter_id, session_id)
    if own_track and own_track[0] == track_id:
        await callback.answer("Нельзя голосовать за свой трек 😄", show_alert=True)
        return
    vote_status = add_vote(session_id, voter_id, track_id)
    if vote_status == "new":
        apply_points_event(
            voter_id,
            callback.from_user.username or "",
            callback.from_user.full_name,
            1,
            event_key=f"main_vote:{session_id}:{voter_id}",
            event_type="main_vote",
            session_id=session_id,
        )
        await callback.answer("✅ Голос принят! +1 очко тебе 🏆", show_alert=True)
    elif vote_status == "updated":
        await callback.answer("🔁 Голос обновлён", show_alert=True)
    else:
        await callback.answer("❌ Не удалось сохранить голос", show_alert=True)

@dp.message(Command("history"))
async def cmd_history(message: Message):
    text, keyboard = build_history_page(0)
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("history_"))
async def cb_history_page(callback: CallbackQuery):
    page = int(callback.data.split("_")[1])
    text, keyboard = build_history_page(page)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

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
    try:
        base_week = get_week_base()
        active = get_active_week_session(base_week)
        if active:
            await message.answer(f"⚠️ Сессия уже существует (состояние: {active[3]}). Сначала заверши текущую.")
            return
        theme = get_random_theme()
        week_sessions = list_week_sessions(base_week)
        if not week_sessions:
            new_week = base_week
        else:
            suffix = next_week_suffix(base_week)
            new_week = base_week if suffix is None else f"{base_week}-{suffix}"
        session_id = create_session(theme, week_override=new_week)
        session = None
        conn = sqlite3.connect("trackday.db")
        c = conn.cursor()
        c.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
        session = c.fetchone()
        conn.close()
        if session:
            sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
            text = (
                f"🎵 <b>TRACK DAY!</b>\n\n"
                f"Тема этой недели: <b>{session[2]}</b>\n\n"
                f"Скидывайте свои треки мне в <b>личку</b> (@track0_day_bot) до 22:00 🎧\n\n"
                f"📌 Правила:\n"
                f"• Один трек от каждого\n"
                f"• Ссылка + можно описание почему именно этот\n"
                f"• Треки будут анонимными до результатов\n\n"
                f"🏆 <a href='{sheet_url}'>Таблица лидеров</a> | За участие: +1 очко"
            )
            await bot.send_message(GROUP_ID, text, parse_mode="HTML")
            await message.answer(f"✅ Сбор треков запущен!\nНеделя: {session[1]}\nТема: {session[2]}")
        else:
            await message.answer("❌ Ошибка — сессия не создалась. Проверь лог.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        logging.error(f"cmd_force_start error: {e}")

@dp.message(Command("startvoting"))
async def cmd_force_voting(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        session = get_current_session()
        if not session:
            await message.answer("❌ Нет активной сессии. Сначала запусти сбор треков (⚙️ Управление → Запустить сбор).")
            return
        started, status = await start_voting()
        if started:
            await message.answer("✅ Голосование запущено!")
        elif status == "already_voting":
            await message.answer("⚠️ Голосование уже запущено.")
        elif status == "already_finished":
            await message.answer("⚠️ Голосование уже завершено.")
        elif status == "no_tracks":
            await message.answer("⚠️ Нет треков для голосования.")
        else:
            await message.answer("⚠️ Запуск голосования недоступен в текущем состоянии сессии.")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        logging.error(f"cmd_force_voting error: {e}")

@dp.message(Command("finishvoting"))
async def cmd_force_finish(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        session = get_current_session()
        if not session:
            await message.answer("❌ Нет активной сессии.")
            return
        await finish_voting()
        await message.answer("✅ Голосование завершено!")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        logging.error(f"cmd_force_finish error: {e}")

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
    cleanup_expired_runtime_data()
    register_all_handlers(dp)
    scheduler.add_job(start_collection, CronTrigger(day_of_week="wed", hour=10, minute=0))
    scheduler.add_job(send_wednesday_reminder, CronTrigger(day_of_week="wed", hour=10, minute=0))
    scheduler.add_job(send_collection_closing_reminder, CronTrigger(day_of_week="wed", hour=21, minute=0))
    scheduler.add_job(start_voting, CronTrigger(day_of_week="wed", hour=22, minute=0))
    scheduler.add_job(send_voting_closing_reminder, CronTrigger(day_of_week="thu", hour=11, minute=0))
    scheduler.add_job(finish_voting, CronTrigger(day_of_week="thu", hour=12, minute=0))
    scheduler.add_job(cleanup_runtime_data_job, IntervalTrigger(hours=1))
    scheduler.start()
    print("🎵 Track Day Bot запущен!")
    # Автопереподключение при обрыве
    while True:
        try:
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        except Exception as e:
            logging.error(f"Polling error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
