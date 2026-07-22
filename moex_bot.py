import os
import json
import csv
import io
import math
import asyncio
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from statistics import mean

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

import pandas as pd
import numpy as np
import pandas_ta as ta

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, BotCommand, ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    BasePersistence,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

try:
    from google import genai as google_genai
    from google.genai import types as genai_types
except ImportError:
    google_genai = None
    genai_types  = None

try:
    from groq import Groq
except ImportError:
    Groq = None

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TINKOFF_TOKEN  = os.getenv("TINKOFF_TOKEN", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
PORT = int(os.getenv("PORT", 8080))
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("moex_railway_bot")

if google_genai and GEMINI_API_KEY:
    try:
        _gemini_client = google_genai.Client(api_key=GEMINI_API_KEY)
        
        GEMINI_MODEL = "gemini-2.5-flash"
    except Exception as _ge:
        logger.warning(f"Gemini init error: {_ge}")
        _gemini_client = None
        GEMINI_MODEL   = ""
else:
    _gemini_client = None
    GEMINI_MODEL   = ""

gemini_model = _gemini_client

groq_client = Groq(api_key=GROQ_API_KEY) if Groq and GROQ_API_KEY else None
openrouter_client = None
if OPENROUTER_API_KEY:
    try:
        import httpx
        openrouter_client = httpx.Client(headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"})
    except Exception as e:
        logger.warning(f"OpenRouter init error: {e}")

GROQ_MODELS = [
    "llama-3.3-70b-versatile",
    "llama-3.1-8b-instant",
]

TINKOFF_API = "https://invest-public-api.tinkoff.ru/rest"

MOEX_STOCKS = {
    
    "GAZP":  ("BBG004730RP0", "Газпром",               "нефтегаз",
              {"газпром", "gazprom", "газовый холдинг", "миллер"}),
    "LKOH":  ("BBG004731032", "Лукойл",                "нефтегаз",
              {"лукойл", "lukoil", "алекперов"}),
    "ROSN":  ("BBG004731354", "Роснефть",              "нефтегаз",
              {"роснефть", "rosneft", "сечин"}),
    "NVTK":  ("BBG00475KKY8", "Новатэк",               "нефтегаз",
              {"новатэк", "novatek", "михельсон"}),
    "TATN":  ("BBG004731642", "Татнефть",              "нефтегаз",
              {"татнефть", "tatneft"}),
    "TATNP": ("BBG004731706", "Татнефть п.",            "нефтегаз",
              {"татнефть", "tatneft"}),
    "SNGS":  ("BBG004730JJ5", "Сургутнефтегаз",        "нефтегаз",
              {"сургутнефтегаз", "сургут", "surgutneftegas"}),
    "SNGSP": ("BBG0047315Y7", "Сургутнефтегаз п.",     "нефтегаз",
              {"сургутнефтегаз", "сургут"}),
    "TRNFP": ("BBG00475KHX6", "Транснефть п.",          "нефтегаз",
              {"транснефть", "transneft"}),
    "BANE":  ("BBG004S68758", "Башнефть",              "нефтегаз",
              {"башнефть", "bashneft"}),
    "BANEP": ("BBG004S687B4", "Башнефть п.",            "нефтегаз",
              {"башнефть", "bashneft"}),
    
    "SBER":  ("BBG004730N88", "Сбербанк",              "банки",
              {"сбербанк", "сбер", "sberbank", "греф"}),
    "SBERP": ("BBG004730N96", "Сбербанк п.",            "банки",
              {"сбербанк", "сбер", "sberbank", "греф"}),
    "VTBR":  ("BBG004730ZJ9", "ВТБ",                   "банки",
              {"втб", "vtb", "костин"}),
    "BSPB":  ("BBG0029SNB14", "БСП",                   "банки",
              {"банк санкт-петербург", "бсп", "bspb"}),
    "CBOM":  ("BBG009GSYN76", "МКБ",                   "банки",
              {"московский кредитный банк", "мкб", "mkb"}),
    "MOEX":  ("BBG004S68507", "МосБиржа",              "финансы",
              {"московская биржа", "мосбиржа", "moex exchange"}),
    "TCSG":  ("BBG00QPYJ5H0", "Т-Банк (TCS)",          "банки",
              {"т-банк", "тинькофф", "tcs", "tinkoff bank", "t"}),
    "T":     ("BBG00QPYJ5H0", "Т-Банк",                "банки",
              {"т-банк", "тинькофф", "t", "tcs", "tcsg", "tinkoff bank"}),
    "SVCB":  ("BBG00F9XX7H4", "Совкомбанк",            "банки",
              {"совкомбанк", "sovcombank"}),
    
    "GMKN":  ("BBG004731489", "Норникель",             "металлы",
              {"норникель", "norilsk", "норильский никель", "потанин"}),
    "CHMF":  ("BBG00475JZZ6", "Северсталь",            "металлы",
              {"северсталь", "severstal", "мордашов"}),
    "NLMK":  ("BBG004S68BH6", "НЛМК",                 "металлы",
              {"нлмк", "nlmk", "новолипецкий"}),
    "MAGN":  ("BBG004S681W1", "ММК",                   "металлы",
              {"ммк", "mmk", "магнитогорский"}),
    "RUAL":  ("BBG008F2T3T2", "РусАл",                 "металлы",
              {"русал", "rusal", "дерипаска"}),
    "ENPG":  ("BBG00F6NK0J8", "Эн+ Груп",              "металлы",
              {"эн+", "en+", "эн плюс", "enplus"}),
    "ALRS":  ("BBG004S68B31", "Алроса",                "горнодобыча",
              {"алроса", "alrosa", "якутские алмазы"}),
    "POLY":  ("BBG004PYF2N3", "Полюс",                 "золото",
              {"полюс", "polyus"}),
    "PLZL":  ("BBG000R607Y3", "Полюс Золото",          "золото",
              {"полюс", "polyus"}),
    "RASP":  ("BBG00475M5R0", "Распадская",            "уголь",
              {"распадская", "raspadskaya"}),
    "MTLR":  ("BBG004S68598", "Мечел",                 "уголь",
              {"мечел", "mechel"}),
    "MTLRP": ("BBG004S68716", "Мечел п.",               "уголь",
              {"мечел", "mechel"}),
    
    "YDEX":  ("BBG006L8G4H1", "Яндекс",               "IT",
              {"яндекс", "yandex", "волож"}),
    "VKCO":  ("BBG00178PGX3", "ВКонтакте",             "IT",
              {"вконтакте", "vk", "mail.ru group"}),
    "POSI":  ("BBG01FD18M82", "Позитив",               "кибербезопасность",
              {"positive technologies", "позитив технолоджис", "posi"}),
    "ASTR":  ("BBG016S3QJ60", "Астра",                 "IT",
              {"группа астра", "astra linux", "ГК астра"}),
    "HEAD":  ("BBG00DHTYPH4", "HeadHunter",            "IT",
              {"headhunter", "хедхантер", "hh.ru"}),
    "CIAN":  ("BBG009S39JX6", "ЦИАН",                  "IT",
              {"циан", "cian"}),
    "MTSS":  ("BBG004S68473", "МТС",                   "телеком",
              {"мтс", "мобильные телесистемы", "mts"}),
    "RTKM":  ("BBG004S681B4", "Ростелеком",            "телеком",
              {"ростелеком", "rostelecom"}),
    "RTKMP": ("BBG004S68696", "Ростелеком п.",          "телеком",
              {"ростелеком", "rostelecom"}),
    
    "MGNT":  ("BBG004RVFCY3", "Магнит",                "ритейл",
              {"магнит", "magnit"}),
    "X5":    ("BBG00JXPFBN0", "X5 Group",              "ритейл",
              {"x5", "x5 group", "пятёрочка", "перекрёсток", "икс5", "five"}),
    "OZON":  ("BBG00Y91R9T3", "Ozon",                  "e-commerce",
              {"озон", "ozon"}),
    "LENT":  ("BBG00264RNXT", "Лента",                 "ритейл",
              {"лента", "lenta"}),
    "MDMG":  ("BBG001M2SC01", "MD Medical",            "медицина",
              {"мать и дитя", "md medical", "mdmg"}),
    "FIXP":  ("BBG00ZHCX1X2", "Fix Price",             "ритейл",
              {"fix price", "фикс прайс"}),
    "FIXR":  ("BBG00ZHCX1X2", "Fix Price",             "ритейл",
              {"fix price", "фикс прайс", "fixr"}),
    
    "FEES":  ("BBG00475K6C3", "ФСК ЕЭС",              "энергетика",
              {"фск", "россети", "фск еэс"}),
    "HYDR":  ("BBG00475K2X9", "РусГидро",              "энергетика",
              {"русгидро", "rushydro"}),
    "IRAO":  ("BBG004S68829", "Интер РАО",             "энергетика",
              {"интер рао", "inter rao"}),
    "OGKB":  ("BBG004S686G4", "ОГК-2",                "энергетика",
              {"огк-2", "ogk-2", "огк2"}),
    "MSNG":  ("BBG004S686W0", "Мосэнерго",             "энергетика",
              {"мосэнерго", "mosenergo"}),
    "TGKA":  ("BBG004S68C23", "ТГК-1",                "энергетика",
              {"тгк-1", "тгк1", "tgk-1"}),
    
    "AFLT":  ("BBG004S683W7", "Аэрофлот",              "транспорт",
              {"аэрофлот", "aeroflot"}),
    "FLOT":  ("BBG000R04X57", "Совкомфлот",            "транспорт",
              {"совкомфлот", "sovcomflot", "scf group"}),
    
    "PIKK":  ("BBG004S68BF0", "ПИК",                   "девелопмент",
              {"пик", "pik group", "гк пик"}),
    "SMLT":  ("BBG005D1WCQ1", "Самолёт",               "девелопмент",
              {"самолёт", "samolet"}),
    "LSRG":  ("BBG0040F7B78", "ЛСР",                   "девелопмент",
              {"лср", "lsr group"}),
    "ETLN":  ("BBG00475JZY3", "Эталон",                "девелопмент",
              {"эталон", "etalon group"}),
    
    "PHOR":  ("BBG004S689R0", "ФосАгро",               "химия",
              {"фосагро", "phosagro"}),
    "KAZT":  ("BBG004S68614", "КуйбышевАзот",          "химия",
              {"куйбышевазот", "казань азот"}),
    "NKNC":  ("BBG004S681N6", "Нижнекамскнефтехим",    "химия",
              {"нижнекамскнефтехим", "нкнх"}),
    
    "SGZH":  ("BBG0100R9963", "Сегежа",                "лесопромышленность",
              {"сегежа", "segezha"}),
    "UWGN":  ("BBG008HD3V85", "ОВК",                   "машиностроение",
              {"объединённая вагонная", "овк", "uwgn"}),
    "GCHE":  ("BBG000RP9B63", "Черкизово",             "АПК",
              {"черкизово", "cherkizovo"}),
    "SFIN":  ("BBG00HGDC7N7", "ЭсЭфАй",               "финансы",
              {"сфи", "эсэфай", "sfin"}),
    "MFGS":  ("BBG004S68BR5", "Мегафон",               "телеком",
              {"мегафон", "megafon"}),
}

FUTURES = {
    "SiU6":  ("FUTSI0926000", "Доллар/Рубль (Si)",    "валюта",   1.0,    1000),  
    "EuU6":  ("FUTEU0926000", "Евро/Рубль (Eu)",      "валюта",   1.0,    1000),
    "RiU6":  ("FUTRI0926000", "Индекс РТС (Ri)",      "индекс",   10.0,   1),
    "MXU6":  ("FUTMX0926000", "Индекс MOEX (MIX)",    "индекс",   0.25,   1),
    "BRU6":  ("FUTBR0926000", "Нефть Brent (BR)",     "товар",    0.01,   10),
    "GDU6":  ("FUTGD0926000", "Золото (Gold)",         "товар",    0.1,    1),
    "NGU6":  ("FUTNG0926000", "Природный газ (NG)",    "товар",    0.001,  1),
    "SRU6":  ("FUTSR0926000", "Серебро (Silver)",      "товар",    0.01,   1),
    "SBU6":  ("FUTSB0926000", "Сбербанк (SBER)",      "акция",    1.0,    100),
    "GZU6":  ("FUTGZ0926000", "Газпром (GAZR)",        "акция",    1.0,    100),
    "LKU6":  ("FUTLK0926000", "Лукойл (LKOH)",         "акция",    1.0,    10),
    "GKU6":  ("FUTGK0926000", "Норникель (GMKN)",      "акция",    1.0,    10),
    "RNU6":  ("FUTRN0926000", "Роснефть (ROSN)",       "акция",    1.0,    100),
    "NKU6":  ("FUTNK0926000", "Новатэк (NVTK)",        "акция",    1.0,    10),
    "VBU6":  ("FUTVB0926000", "ВТБ (VTBR)",            "акция",    0.5,    10000),
    "MNU6":  ("FUTMN0926000", "МТС (MTSS)",            "акция",    1.0,    100),
    "AFU6":  ("FUTAF0926000", "Аэрофлот (AFLT)",       "акция",    1.0,    1000),
}

_instrument_currency: dict[str, str] = {}  

FUTURES_USD_PRICED = {"BRU6", "NGU6", "GDU6", "SRU6"}  
for _fut_code in FUTURES_USD_PRICED:
    if _fut_code in FUTURES:
        _instrument_currency[FUTURES[_fut_code][0]] = "usd"

def load_futures_watchlist() -> list[str]:
    try:
        data = _load_json(RK_FUT_WL, FUTURES_WATCHLIST_FILE, [])
        return [t for t in data if t in FUTURES] or ["SiU6", "RiU6", "BRU6", "GDU6", "SBU6", "GZU6"]
    except Exception:
        return ["SiU6", "RiU6", "BRU6", "GDU6", "SBU6", "GZU6"]

def save_futures_watchlist(tickers: list[str]):
    _save_json(RK_FUT_WL, FUTURES_WATCHLIST_FILE, tickers)

SECTOR_KEYWORDS = {
    "нефтегаз":        ["нефть", "газ", "brent", "urals", "опек", "opec", "экспорт нефти",
                        "пошлина нефть", "санкции нефть", "нефтяные"],
    "металлы":         ["сталь", "алюминий", "никель", "медь", "metal", "экспорт металл",
                        "пошлина металл", "санкции металл"],
    "банки":           ["ключевая ставка", "цб рф", "ставка", "ипотека", "резервы", "прибыль банк"],
    "IT":              ["it", "технологии", "санкции it", "импортозамещение"],
    "золото":          ["золото", "gold", "драгметалл"],
    "энергетика":      ["электроэнергия", "тариф", "энергосбыт"],
    "девелопмент":     ["ипотека", "льготная ипотека", "недвижимость", "строительство"],
    "ритейл":          ["ритейл", "торговля", "потребительский"],
    "telecom":         ["связь", "телеком", "5g", "тариф связь"],
    "транспорт":       ["транспорт", "авиация", "санкции авиа", "фрахт"],
    "горнодобыча":     ["алмазы", "добыча", "санкции добыча"],
    "уголь":           ["уголь", "coal", "экспорт уголь"],
    "лесопромышленность": ["лес", "целлюлоза", "бумага", "лесозаготовка"],
    "e-commerce":      ["электронная торговля", "маркетплейс", "онлайн торговля"],
    "финансы":         ["биржа", "торги", "ликвидность рынок"],
}

MARKET_KEYWORDS = [
    "цб рф", "ключевая ставка", "минфин", "минэкономразвития",
    "санкции", "нефть", "газ", "курс рубля", "рубль",
    "дивиденды", "байбек", "buyback", "допэмиссия", "сделка слияние",
    "ввп россия", "инфляция россия",
]

MOEX_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07",
    "2026-01-08", "2026-01-09",  
    "2026-02-23",                 
    "2026-03-09",                 
    "2026-05-01", "2026-05-04",   
    "2026-05-11",                 
    "2026-06-12",                 
    "2026-11-04",                 
    "2026-12-31",                 
}

def is_trading_day_moex() -> bool:
    """Возвращает True если MOEX сегодня торгует (учитывает праздники)."""
    now_msk = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    if now_msk.weekday() >= 5:
        return False

    date_str = now_msk.strftime("%Y-%m-%d")

    if now_msk.year != 2026:
        if not getattr(is_trading_day_moex, "_year_mismatch_logged", False):
            logger.error(
                f"is_trading_day_moex: MOEX_HOLIDAYS_2026 устарел - текущий год "
                f"{now_msk.year}, а список праздников только на 2026. Праздничные "
                f"дни этого года НЕ будут учтены. Нужно обновить список праздников."
            )
            is_trading_day_moex._year_mismatch_logged = True
        
        if now_msk.month == 1 and now_msk.day <= 8:
            return False
        return True

    return date_str not in MOEX_HOLIDAYS_2026

def _get_futures_suffix() -> str:
    """Возвращает актуальный суффикс фьючерса (M6, U6, Z6, H7...) по текущей дате."""
    now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    year2 = str(now.year)[-2:]
    
    month = now.month
    day   = now.day
    if month <= 3 and not (month == 3 and day > 15):
        return f"H{year2}"   
    elif month <= 6 and not (month == 6 and day > 15):
        return f"M{year2}"   
    elif month <= 9 and not (month == 9 and day > 15):
        return f"U{year2}"   
    elif month <= 12 and not (month == 12 and day > 15):
        return f"Z{year2}"   
    else:
        
        next_year = str(now.year + 1)[-2:]
        return f"H{next_year}"

def get_active_futures_code(base: str) -> str:
    """Возвращает актуальный код фьючерса: Si → SiM6 (или SiU6 после экспирации)."""
    suffix = _get_futures_suffix()
    return f"{base}{suffix}"

FUTURES_BASE_ASSET = {
    "SBU6": "SBER",  "GZU6": "GAZP",  "LKU6": "LKOH",
    "GKU6": "GMKN",  "RNU6": "ROSN",  "NKU6": "NVTK",
    "AFU6": "AFLT",  "MNU6": "MTSS",  "VBU6": "VTBR",
}
FUTURES_CORR_GROUPS = [
    {"MXU6", "RiU6"},                          
    {"SiU6", "EuU6"},                          
    {"BRU6", "NGU6"},                          
    {"GDU6", "SRU6"},                          
    {"SBU6", "GZU6", "LKU6", "GKU6",
     "RNU6", "NKU6", "AFU6", "MNU6", "VBU6"}, 
]

def filter_correlated_futures(sigs: list) -> tuple[list, list]:
    """
    Возвращает (лучшие, коррелирующие).
    Из каждой группы берём только сигнал с лучшим скором.
    Остальные - в 'смотри также'.
    """
    used    = set()
    best    = []
    also    = []
    
    sorted_sigs = sorted(sigs, key=lambda x: -x["tech_score"])
    for s in sorted_sigs:
        ticker = s["ticker"]
        
        group = next((g for g in FUTURES_CORR_GROUPS if ticker in g), None)
        if group is None:
            best.append(s)
            continue
        
        group_key = (frozenset(group), s["tech_signal"])
        if group_key not in used:
            used.add(group_key)
            best.append(s)
        else:
            also.append(s)
    return best, also

TF_MAP = {
    "5m":  ("CANDLE_INTERVAL_5_MIN",      5,  300),
    "15m": ("CANDLE_INTERVAL_15_MIN",    15,  300),
    "1h":  ("CANDLE_INTERVAL_HOUR",      60,  200),
    "4h":  ("CANDLE_INTERVAL_4_HOUR",   240,  150),
    "1d":  ("CANDLE_INTERVAL_DAY",     1440,  300),
    "1w":  ("CANDLE_INTERVAL_WEEK",   10080,  100),
}
DEFAULT_TF = "15m"
INTRADAY_TFS = {"5m", "15m", "1h"}

TIME_EXIT_MAP = {
    "5m":  60,    
    "15m": 120,   
    "1h":  240,   
    "4h":  960,   
    "1d":  2880,  
}

TRADE_MODES = {
    "low": {
        "label": "🟢 LOW (консерватив)",
        "rsi_oversold": 30, "rsi_overbought": 70,
        "min_score": 80,
        "min_vol_ratio": 1.8,
        "min_news_score": 5,
        "personality": "Консервативный трейдер. Только чёткие сигналы.",
    },
    "mid": {
        "label": "🟡 MID (баланс)",
        "rsi_oversold": 35, "rsi_overbought": 65,
        "min_score": 70,
        "min_vol_ratio": 1.3,
        "min_news_score": 6,
        "personality": "Сбалансированный трейдер на MOEX.",
    },
    "hard": {
        "label": "🔴 HARD (агрессив)",
        "rsi_oversold": 40, "rsi_overbought": 60,
        "min_score": 60,
        "min_vol_ratio": 1.1,
        "min_news_score": 5,
        "personality": "Агрессивный трейдер. Больше сигналов.",
    },
}

_cache: dict = {}
_news_cache: dict = {}
_pending_trades: dict = {}  

class _TinkoffRateLimiter:
    """
    Превентивный rate-limiter для запросов к Tinkoff API - распределяет
    запросы по времени равномерными слотами, а не token bucket с общим
    окном. Раньше защита была только реактивной (retry после получения
    429 в _analyze_with_semaphore) - семафор ограничивает лишь
    ОДНОВРЕМЕННОСТЬ запросов (5 параллельно), а не их ЧАСТОТУ. При скане
    30 тикеров, где каждый делает 6-7 запросов GetCandles (текущий ТФ +
    MTF 15m/1h/4h + дневные для HVN), расчётный пик - порядка 700+
    запросов/минуту, заметно выше официального лимита в 120/минуту на
    метод (T-Bank Dev Portal, лимитная политика).

    ВАЖНО - почему именно интервальные слоты, а не token bucket с окном:
    Первая версия держала список timestamps за последние 60 секунд и
    ждала "пока освободится окно" - но при параллельных запросах (семафор
    из 5, много тикеров) ВСЕ ожидающие корутины вычисляли одинаковый wait
    относительно одной и той же самой старой метки и просыпались massово
    ОДНОВРЕМЕННО, мгновенно повторно упираясь в лимит и снова блокируя
    друг друга по цепочке. Экспериментально: 20 запросов при лимите 5/мин
    занимали 180+ секунд вместо ожидаемых ~70, и это было прямой причиной
    того, что скан не укладывался в 5-минутный таймаут scanner_loop и
    обрывался без единого сигнала.

    Текущий подход: каждый вызов атомарно резервирует свой персональный
    следующий слот времени (под локом, без sleep внутри), затем спит
    ТОЛЬКО свой точный интервал вне лока - без общих window-пересчётов и
    без массовых одновременных пробуждений. Реалистичный сценарий (210
    запросов, лимит 100/мин) укладывается в ~125 секунд равномерно, без
    каскадных пиков.
    """
    def __init__(self, max_per_minute: int = 100):
        self.min_interval = 60.0 / max_per_minute
        self._next_slot = 0.0
        self._lock = None  

    async def acquire(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            now = time.time()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self.min_interval
            wait = slot - now
        if wait > 0:
            await asyncio.sleep(wait)

_tinkoff_rate_limiter = _TinkoffRateLimiter(max_per_minute=100)

class _AIRateLimiter:
    """
    Асинхронный rate-limiter для AI-вызовов (Gemini/Groq/OpenRouter).
    Использует asyncio.sleep, не замораживая потоки пула ThreadPoolExecutor.
    """
    def __init__(self, max_per_minute: int = 10):
        self.min_interval = 60.0 / max_per_minute
        self._next_slot = 0.0
        self._lock = None

    async def acquire(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            now = time.time()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self.min_interval
            wait = slot - now
        if wait > 0:
            await asyncio.sleep(wait)

_ai_rate_limiter = _AIRateLimiter(max_per_minute=10)


def _cleanup_pending_trades():
    """Удаляем записи старше 30 минут."""
    cutoff = time.time() - 1800
    expired = [k for k, v in _pending_trades.items() if v.get("ts", 0) < cutoff]
    for k in expired:
        del _pending_trades[k]
_http_session: aiohttp.ClientSession | None = None

def _get_http_session() -> aiohttp.ClientSession:
    """Возвращает единую HTTP-сессию для всего приложения."""
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=15)
        _http_session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; MOEXBot/1.0)"},
        )
    return _http_session

import redis as redis_lib

REDIS_URL = os.getenv("REDIS_URL", "")
_redis: redis_lib.Redis | None = None

def _get_redis() -> redis_lib.Redis | None:
    global _redis
    if _redis is not None:
        return _redis
    if not REDIS_URL:
        return None
    try:
        _redis = redis_lib.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
        _redis.ping()
        logger.info("Redis connected OK")
        return _redis
    except Exception as e:
        logger.warning(f"Redis connect failed: {e} - using local files")
        _redis = None
        return None

def _rget(key: str) -> str | None:
    r = _get_redis()
    if r:
        try:
            return r.get(key)
        except Exception as e:
            logger.warning(f"Redis GET {key}: {e}")
    return None

def _rset(key: str, value: str) -> bool:
    r = _get_redis()
    if r:
        try:
            r.set(key, value)
            return True
        except Exception as e:
            logger.warning(f"Redis SET {key}: {e}")
    return False

def _load_json(key: str, file_path: Path, default):
    """Загружает JSON из Redis (приоритет) или файла (fallback)."""
    raw = _rget(key)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    if file_path and file_path.exists():
        try:
            return json.loads(file_path.read_text())
        except Exception:
            pass
    return default

def _save_json(key: str, file_path: Path | None, data) -> None:
    """Сохраняет JSON в Redis И в файл (если путь задан)."""
    raw = json.dumps(data, ensure_ascii=False, indent=2)
    _rset(key, raw)
    if file_path:
        try:
            file_path.write_text(raw)
        except Exception:
            pass

def _rpush_log(key: str, entry: dict, max_len: int = 5000) -> None:
    """Добавляет запись в Redis-список (LPUSH) и обрезает до max_len.
    Не перезаписывает весь лог - атомарная операция, дешёвая по памяти."""
    r = _get_redis()
    if not r:
        return
    try:
        r.lpush(key, json.dumps(entry, ensure_ascii=False))
        r.ltrim(key, 0, max_len - 1)
    except Exception as e:
        logger.warning(f"Redis LPUSH {key}: {e}")

def _lrange_log(key: str, count: int = 1000) -> list[dict]:
    """Читает последние `count` записей лога (новые первыми)."""
    r = _get_redis()
    if not r:
        return []
    try:
        raw_items = r.lrange(key, 0, count - 1)
        out = []
        for raw in raw_items:
            try:
                out.append(json.loads(raw))
            except Exception:
                continue
        return out
    except Exception as e:
        logger.warning(f"Redis LRANGE {key}: {e}")
        return []

RK_SIGNAL_OUTCOMES = "moex:signal_outcomes"  

def _pending_outcomes_add(signal_id: str, data: dict) -> None:
    """Регистрирует сигнал для отложенной проверки исхода."""
    r = _get_redis()
    if not r:
        return
    try:
        r.hset(RK_SIGNAL_OUTCOMES, signal_id, json.dumps(data, ensure_ascii=False))
    except Exception as e:
        logger.warning(f"_pending_outcomes_add {signal_id}: {e}")

def _pending_outcomes_get_all() -> dict[str, dict]:
    """Возвращает все ещё не проверенные (pending) исходы."""
    r = _get_redis()
    if not r:
        return {}
    try:
        raw = r.hgetall(RK_SIGNAL_OUTCOMES)
        out = {}
        for k, v in raw.items():
            try:
                out[k] = json.loads(v)
            except Exception:
                continue
        return out
    except Exception as e:
        logger.warning(f"_pending_outcomes_get_all: {e}")
        return {}

def _pending_outcomes_remove(signal_id: str) -> None:
    """Удаляет проверенный сигнал из pending - он либо разрешился, либо устарел."""
    r = _get_redis()
    if not r:
        return
    try:
        r.hdel(RK_SIGNAL_OUTCOMES, signal_id)
    except Exception as e:
        logger.warning(f"_pending_outcomes_remove {signal_id}: {e}")

RK_OUTCOME_STATS = "moex:outcome_stats"  
RK_AI_MEMORY = "moex:ai_memory"

def _outcome_stats_append(entry: dict) -> None:
    _rpush_log(RK_OUTCOME_STATS, entry, max_len=2000)

RK_TRADES    = "moex:trades"
RK_WATCHLIST = "moex:watchlist"
RK_CALENDAR  = "moex:calendar"
RK_FIGI      = "moex:figi"
RK_FUT_WL    = "moex:futures_watchlist"
RK_FUT_EXP   = "moex:futures_expiry"
RK_CHAT_DATA = "moex:chat_data"
RK_SETTINGS  = "moex:settings"
RK_LAST_EOD  = "moex:last_eod_date"
RK_SECTOR_MODIFIERS = "moex:sector_modifiers"  

def _safe_redis_cleanup() -> None:
    """Удаляет старые ключи Redis, сохранённые как string, чтобы новые hset/lpush не падали с WRONGTYPE."""
    r = _get_redis()
    if not r:
        return
    hash_keys = [
        RK_CHAT_DATA,
        RK_SECTOR_MODIFIERS,
        RK_SIGNAL_OUTCOMES,
    ]
    list_keys = [
        RK_OUTCOME_STATS,
        RK_SIGNAL_LOG,
    ]
    for key in hash_keys + list_keys:
        try:
            if r.exists(key) and r.type(key) == "string":
                r.delete(key)
                logger.warning(f"Redis cleanup: удалён устаревший ключ {key}")
        except Exception as exc:
            logger.debug(f"Redis cleanup check {key}: {exc}")
    # Очистка старых секторальных модификаторов (сгенерированных старой AI-логикой)
    try:
        if r.exists(RK_SECTOR_MODIFIERS) and r.type(RK_SECTOR_MODIFIERS) == "hash":
            old_mods = r.hgetall(RK_SECTOR_MODIFIERS)
            if old_mods:
                r.delete(RK_SECTOR_MODIFIERS)
                logger.warning(f"Redis cleanup: удалены все {len(old_mods)} старых sector_modifiers (новая логика SECTOR_KEYS)")
    except Exception:
        pass
    logger.info("Redis cleanup: проверка старых ключей завершена")


ALL_SECTORS = {
    "нефтегаз", "банки", "финансы", "металлы", "горнодобыча", "золото",
    "уголь", "IT", "телеком", "ритейл", "e-commerce", "энергетика",
    "транспорт", "девелопмент", "химия", "лесопромышленность",
    "машиностроение", "АПК", "кибербезопасность", "медицина", "валюта",
}
BOT_SETTINGS_DEFAULTS: dict[str, float] = {
    "fact_weight_ai_trigger":    6,    
    "news_max_age_min":          120,  
    "news_max_age_liquid_min":   10,   
    "min_tech_score_confirmed":  61,   
    "sl_min_risk_pct":           1.0,  
    "sl_max_risk_pct":           3.0,  
    "time_exit_loss_threshold":  -1.5, 
    "time_exit_tp1_progress":    30,   
    "pump_dump_vol_ratio":       3.0,  
    "news_dedup_similarity":     80,   
}

BOT_SETTINGS_LABELS: dict[str, str] = {
    "fact_weight_ai_trigger":   "Порог веса события для вызова AI",
    "news_max_age_min":         "Макс. возраст новости (2-й эшелон), мин",
    "news_max_age_liquid_min":  "Макс. возраст новости (ликвидные), мин",
    "min_tech_score_confirmed": "Мин. tech_score для CONFIRMED",
    "sl_min_risk_pct":          "Минимальный риск SL, %",
    "sl_max_risk_pct":          "Максимальный риск SL, %",
    "time_exit_loss_threshold": "Порог убытка для закрытия по таймауту, %",
    "time_exit_tp1_progress":   "Мин. прогресс к TP1 для продолжения удержания, %",
    "pump_dump_vol_ratio":      "Множитель объёма для памп/дамп детектора",
    "news_dedup_similarity":    "Длина заголовка для дедупликации новостей",
}

_settings_cache: dict | None = None

def get_bot_settings() -> dict[str, float]:
    """Возвращает текущие настройки - из Redis, с фоллбэком на дефолты."""
    global _settings_cache
    if _settings_cache is not None:
        return _settings_cache
    raw = _rget(RK_SETTINGS)
    settings = dict(BOT_SETTINGS_DEFAULTS)
    if raw:
        try:
            saved = json.loads(raw)
            for k, v in saved.items():
                if k in BOT_SETTINGS_DEFAULTS:
                    settings[k] = v
        except Exception as e:
            logger.warning(f"get_bot_settings parse error: {e}")
    _settings_cache = settings
    return settings

def set_bot_setting(key: str, value: float) -> bool:
    """Сохраняет одну настройку в Redis, сбрасывает кэш."""
    global _settings_cache
    if key not in BOT_SETTINGS_DEFAULTS:
        return False
    settings = get_bot_settings()
    settings[key] = value
    ok = _rset(RK_SETTINGS, json.dumps(settings, ensure_ascii=False))
    if ok:
        _settings_cache = settings
    return bool(ok)

def reset_bot_settings() -> bool:
    """Сбрасывает все настройки к дефолтным значениям."""
    global _settings_cache
    ok = _rset(RK_SETTINGS, json.dumps(BOT_SETTINGS_DEFAULTS, ensure_ascii=False))
    if ok:
        _settings_cache = dict(BOT_SETTINGS_DEFAULTS)
    return bool(ok)

_ai_enabled_cache: bool | None = None

def get_ai_enabled() -> bool:
    """Возвращает True если ИИ включен (по умолчанию True)."""
    global _ai_enabled_cache
    if _ai_enabled_cache is not None:
        return _ai_enabled_cache
    raw = _rget(RK_AI_ENABLED)
    if raw is not None:
        _ai_enabled_cache = raw.lower() in ("1", "true", "yes", "on")
    else:
        _ai_enabled_cache = True
    return _ai_enabled_cache

def set_ai_enabled(enabled: bool) -> bool:
    """Включает/выключает все ИИ-функции бота."""
    global _ai_enabled_cache
    ok = _rset(RK_AI_ENABLED, "1" if enabled else "0")
    _ai_enabled_cache = enabled
    return ok


from telegram.ext import PersistenceInput

class RedisChatDataPersistence(BasePersistence):
    """
    Минимальный BasePersistence который хранит только chat_data в Redis.
    bot_data и user_data не используются - не сохраняем.

    Хранение: Redis HASH под ключом RK_CHAT_DATA, где поле - chat_id,
    значение - JSON конкретного чата. Раньше был один JSON-блоб всех чатов
    сразу (_load читал и переписывал ВЕСЬ объект на каждое обновление любого
    чата) - O(N) операций на каждое сообщение при N активных чатах. HSET/
    HGET по конкретному полю - O(1) относительно числа чатов.
    """

    def __init__(self):
        super().__init__(
            store_data=PersistenceInput(
                bot_data=False,
                chat_data=True,
                user_data=False,
                callback_data=False,
            )
        )

    def _load_one(self, chat_id) -> dict:
        r = _get_redis()
        if not r:
            return {}
        try:
            raw = r.hget(RK_CHAT_DATA, str(chat_id))
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.warning(f"RedisPersistence hget error ({chat_id}): {e}")
        return {}

    def _load_all(self) -> dict:
        """Используется только для get_chat_data() - единоразовая загрузка
        всех чатов при старте приложения, не на каждое сообщение."""
        r = _get_redis()
        if not r:
            return {}
        try:
            raw_map = r.hgetall(RK_CHAT_DATA)
            result = {}
            for chat_id_str, raw in raw_map.items():
                try:
                    result[chat_id_str] = json.loads(raw)
                except Exception:
                    continue
            return result
        except Exception as e:
            logger.warning(f"RedisPersistence hgetall error: {e}")
            return {}

    def _save_one(self, chat_id, data: dict) -> None:
        r = _get_redis()
        if not r:
            return
        try:
            r.hset(RK_CHAT_DATA, str(chat_id), json.dumps(data, ensure_ascii=False))
        except Exception as e:
            logger.warning(f"RedisPersistence hset error ({chat_id}): {e}")

    def _delete_one(self, chat_id) -> None:
        r = _get_redis()
        if not r:
            return
        try:
            r.hdel(RK_CHAT_DATA, str(chat_id))
        except Exception as e:
            logger.warning(f"RedisPersistence hdel error ({chat_id}): {e}")

    async def get_chat_data(self) -> dict:
        return self._load_all()

    async def update_chat_data(self, chat_id: int, data: object) -> None:
        self._save_one(chat_id, data)

    async def drop_chat_data(self, chat_id: int) -> None:
        self._delete_one(chat_id)

    async def get_bot_data(self) -> dict:
        return {}

    async def update_bot_data(self, data: object) -> None:
        pass

    async def get_user_data(self) -> dict:
        return {}

    async def update_user_data(self, user_id: int, data: object) -> None:
        pass

    async def drop_user_data(self, user_id: int) -> None:
        pass

    async def get_callback_data(self):
        return None

    async def update_callback_data(self, data: object) -> None:
        pass

    async def flush(self) -> None:
        pass  

    async def refresh_bot_data(self, bot_data: dict) -> None:
        pass  

    async def refresh_chat_data(self, chat_id: int, chat_data: dict) -> None:
        
        saved = self._load_one(chat_id)
        if saved:
            chat_data.update(saved)

    async def refresh_user_data(self, user_id: int, user_data: dict) -> None:
        pass  

    async def get_conversations(self, name: str):
        return {}

    async def update_conversation(self, name: str, key: tuple, new_state: object) -> None:
        pass  
RK_SCANNER_CHATS = "moex:scanner_chats"
RK_SIGNAL_LOG    = "moex:signal_log"
RK_AI_ENABLED   = "moex:ai_enabled"

TRADES_FILE            = Path("open_trades.json")
SCANNER_FILE           = Path("scanner_state.json")
WATCHLIST_FILE         = Path("watchlist.json")
FIGI_FILE              = Path("figi_result.json")
FUTURES_WATCHLIST_FILE = Path("futures_watchlist.json")
FUTURES_EXPIRY_FILE    = Path("futures_expiry.json")
CALENDAR_FILE          = Path("calendar_events.json")

ADV_MIN_CANDLES = 20   

LIQUID_TICKERS = {
    "SBER", "SBERP", "GAZP", "LKOH", "ROSN", "NVTK", "TATN", "GMKN",
    "CHMF", "NLMK", "YDEX", "MGNT", "X5", "VTBR", "MOEX", "TCSG",
    "AFLT", "POSI", "MAGN", "OZON", "SNGS",
}

def get_liquidity_tier(ticker: str, df: pd.DataFrame) -> tuple[str, str]:
    """
    Возвращает (tier, warning).
    tier: 'high' | 'medium' | 'low'
    warning: строка предупреждения или ''
    """
    if ticker in LIQUID_TICKERS:
        return "high", ""
    if "volume" not in df.columns or len(df) < ADV_MIN_CANDLES:
        return "medium", ""

    recent = df.tail(ADV_MIN_CANDLES)
    avg_vol = float(recent["volume"].mean())
    price   = float(df["close"].iloc[-1])
    
    avg_rub = avg_vol * price

    if avg_rub >= 50_000_000:    
        return "high", ""
    elif avg_rub >= 5_000_000:   
        return "medium", f"⚠️ Средняя ликвидность (~{avg_rub/1e6:.0f} млн руб/свеча)"
    else:                         
        return "low", f"🔴 Низкая ликвидность (~{avg_rub/1e6:.1f} млн руб/свеча) - спред может быть высоким"

def esc(text: str) -> str:
    """Экранирует спецсимволы HTML для Telegram parse_mode=HTML."""
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))

def load_trades() -> dict:
    try:
        return _load_json(RK_TRADES, TRADES_FILE, {})
    except Exception:
        return {}

def save_trades(d: dict):
    _save_json(RK_TRADES, TRADES_FILE, d)

def open_trade(ticker: str, direction: str, entry: float,
               sl: float, tp1: float, tp2: float, tp3: float,
               chat_id: int, tf: str = "15m",
               tech_score: int = 0, filter_status: str = "",
               anomaly: str = "", ai_summary: str = "") -> str:
    """Открывает сделку, сохраняя метаданные для подробного журнала."""
    if not entry or entry <= 0:
        logger.error(f"open_trade: некорректная цена входа entry={entry} для {ticker}, сделка не открыта")
        raise ValueError(f"Некорректная цена входа: {entry}")

    trades = load_trades()
    trade_id = f"{ticker}_{int(time.time())}"
    max_hold_min = TIME_EXIT_MAP.get(tf.lower(), TIME_EXIT_MAP["15m"])
    trades[trade_id] = {
        "ticker":               ticker,
        "direction":            direction.upper(),
        "entry":                entry,
        "sl":                   sl,
        "sl_original":          sl,
        "tp1":                  tp1,
        "tp2":                  tp2,
        "tp3":                  tp3,
        "status":               "open",
        "chat_id":              chat_id,
        "opened_at":            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "tf":                   tf,
        "sl_moved_to_be":       False,
        "sl_moved_to_tp1":      False,
        "high_price":           entry,
        "low_price":            entry,
        "max_holding_minutes":  max_hold_min,
        
        "tech_score":           tech_score,
        "filter_status":        filter_status,
        "anomaly":              anomaly,
        "ai_summary":           ai_summary,
    }
    save_trades(trades)
    return trade_id

def close_trade(trade_id: str, reason: str, price: float) -> dict | None:
    trades = load_trades()
    if trade_id not in trades:
        return None
    t = trades[trade_id]
    t["status"] = "closed"
    t["close_reason"] = reason
    t["close_price"] = price
    t["closed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    entry = t.get("entry", 0)
    if entry:
        if t["direction"] == "LONG":
            pnl_pct = (price - entry) / entry * 100
        else:
            pnl_pct = (entry - price) / entry * 100
    else:
        logger.warning(f"close_trade {trade_id}: entry=0, PnL не рассчитан")
        pnl_pct = 0.0
    t["pnl_pct"] = round(pnl_pct, 2)

    save_trades(trades)
    return t

def format_trade_status(t: dict) -> str:
    """Форматирует текущий статус сделки для Telegram."""
    direction_e = "🟩 LONG" if t["direction"] == "LONG" else "🟥 SHORT"
    status_map = {
        "open":      "🔵 Открыта",
        "tp1_hit":   "🟡 TP1 достигнут",
        "tp2_hit":   "🟠 TP2 достигнут",
        "closed":    "⚫ Закрыта",
    }
    status = status_map.get(t["status"], t["status"])
    sl_note = ""
    if t.get("sl_moved_to_tp1"):
        sl_note = " (SL на TP1)"
    elif t.get("sl_moved_to_be"):
        sl_note = " (SL в б/у)"

    lines = [
        f"<b>{t['ticker']}</b> {direction_e} | {status}",
        f"Вход: {t['entry']:,.2f} ₽  |  TF: {t['tf']}",
        f"SL: {t['sl']:,.2f} ₽{sl_note}",
        f"TP1: {t['tp1']:,.2f} ₽  TP2: {t['tp2']:,.2f} ₽  TP3: {t['tp3']:,.2f} ₽",
        f"Открыта: {t['opened_at'][:16].replace('T', ' ')}",
    ]
    if t.get("close_price"):
        pnl = t.get("pnl_pct", 0)
        pnl_e = "📈" if pnl >= 0 else "📉"
        lines.append(f"Закрыта по {t['close_price']:,.2f} ₽  {pnl_e} {pnl:+.2f}%")
    return "\n".join(lines)

async def _check_trade(trade_id: str, t: dict, price: float, trades_cache: dict = None) -> str | None:
    """Проверяет уровни для одной сделки. Обновляет SL. Возвращает alert или None.

    trades_cache: если вызывающий код уже загрузил trades (как
    monitor_trades_loop делает раз на весь цикл), передать его сюда -
    избавляет от повторного чтения из Redis на каждую сделку в каждом
    цикле мониторинга. Если не передан - загружает самостоятельно
    (сохраняет обратную совместимость с любым другим местом вызова)."""
    trades = trades_cache if trades_cache is not None else load_trades()
    if trade_id not in trades:
        return None

    direction = t["direction"]
    entry = t["entry"]
    sl = t["sl"]
    tp1 = t["tp1"]
    tp2 = t["tp2"]
    tp3 = t["tp3"]
    status = t["status"]
    is_long = direction == "LONG"
    changed = False

    if is_long:
        new_high = max(t.get("high_price", entry), price)
        if new_high != t.get("high_price", entry):
            trades[trade_id]["high_price"] = new_high
            changed = True
    else:
        new_low = min(t.get("low_price", entry), price)
        if new_low != t.get("low_price", entry):
            trades[trade_id]["low_price"] = new_low
            changed = True

    if status == "open":
        try:
            opened_dt = datetime.fromisoformat(t["opened_at"])
            if opened_dt.tzinfo is None:
                opened_dt = opened_dt.replace(tzinfo=timezone.utc)
            elapsed_min = (datetime.now(timezone.utc) - opened_dt).total_seconds() / 60
            max_hold = t.get("max_holding_minutes", 120)

            if elapsed_min >= max_hold:
                tp1_dist_total = abs(tp1 - entry)
                if tp1_dist_total > 0:
                    tp1_progress = (price - entry) / tp1_dist_total if is_long else (entry - price) / tp1_dist_total
                else:
                    tp1_progress = 0.0

                pnl_pct = (price - entry) / entry * 100 if is_long else (entry - price) / entry * 100

                days_held = int(elapsed_min // (24 * 60))

                force_close = False
                close_reason_text = ""
                _settings = get_bot_settings()
                loss_threshold = _settings["time_exit_loss_threshold"]
                tp1_threshold  = _settings["time_exit_tp1_progress"] / 100

                if days_held >= 2:
                    force_close = True
                    close_reason_text = f"Удержание {days_held} дн. - принудительное закрытие."
                elif tp1_progress < tp1_threshold and pnl_pct < loss_threshold:
                    force_close = True
                    close_reason_text = f"Прогресс к TP1: {tp1_progress*100:.0f}% | Потери: {pnl_pct:.2f}% - сигнал не отработал."

                if force_close:
                    closed = close_trade(trade_id, "Time Exit", price)
                    pnl = closed.get("pnl_pct", 0) if closed else 0
                    pnl_e = "📈" if pnl >= 0 else "📉"
                    return (
                        f"🕐 <b>Тайм-аут - {t['ticker']} {direction}</b>\n"
                        f"Цена: {price:,.2f} ₽ | Удержание: {elapsed_min:.0f} мин\n"
                        f"{close_reason_text}\n"
                        f"{pnl_e} Результат: {pnl:+.2f}%"
                    )
                else:
                    
                    if abs(elapsed_min - max_hold) < 5:  
                        pnl_e = "📈" if pnl_pct >= 0 else "📉"
                        return (
                            f"⏰ <b>{t['ticker']} {direction} - таймаут, сделка продолжается</b>\n"
                            f"Цена: {price:,.2f} ₽ {pnl_e} {pnl_pct:+.2f}%\n"
                            f"Прогресс к TP1: {tp1_progress*100:.0f}%\n"
                            f"<i>SL на месте ({sl:,.2f} ₽). Ждём следующую сессию.</i>"
                        )
        except Exception as te_err:
            logger.warning(f"Ошибка проверки Time-Exit для {trade_id}: {te_err}")

    sl_hit = (is_long and price <= sl) or (not is_long and price >= sl)
    if sl_hit:
        closed = close_trade(trade_id, "SL", price)
        pnl = closed.get("pnl_pct", 0) if closed else 0
        pnl_e = "📈" if pnl >= 0 else "📉"
        sl_type = "безубыток" if t.get("sl_moved_to_be") else ("TP1" if t.get("sl_moved_to_tp1") else "стоп")
        return (
            f"🛑 <b>SL сработал - {t['ticker']} {direction}</b>\n"
            f"Цена: {price:,.2f} ₽ | SL ({sl_type}): {sl:,.2f} ₽\n"
            f"{pnl_e} Результат: {pnl:+.2f}%\n"
            f"<i>Сделка закрыта.</i>"
        )

    tp3_hit = (is_long and price >= tp3) or (not is_long and price <= tp3)
    if tp3_hit and status in ("open", "tp1_hit", "tp2_hit"):
        closed = close_trade(trade_id, "TP3", price)
        pnl = closed.get("pnl_pct", 0) if closed else 0
        return (
            f"🎯🎯🎯 <b>TP3 достигнут - {t['ticker']}!</b>\n"
            f"Цена: {price:,.2f} ₽ | TP3: {tp3:,.2f} ₽\n"
            f"📈 Результат: +{pnl:.2f}%\n"
            f"<b>Отличная сделка!</b>"
        )

    tp2_hit = (is_long and price >= tp2) or (not is_long and price <= tp2)
    if tp2_hit and status == "tp1_hit":
        trades[trade_id]["status"] = "tp2_hit"
        if not t.get("sl_moved_to_tp1"):
            trades[trade_id]["sl"] = tp1
            trades[trade_id]["sl_moved_to_tp1"] = True
            changed = True
        save_trades(trades)
        sl_note = f"\n📌 SL перенесён на TP1 ({tp1:,.2f} ₽)" if changed else ""
        return (
            f"🎯🎯 <b>TP2 достигнут - {t['ticker']}!</b>\n"
            f"Цена: {price:,.2f} ₽ | TP2: {tp2:,.2f} ₽\n"
            f"Удерживаем позицию, цель TP3: {tp3:,.2f} ₽{sl_note}"
        )

    tp1_hit = (is_long and price >= tp1) or (not is_long and price <= tp1)
    if tp1_hit and status == "open":
        trades[trade_id]["status"] = "tp1_hit"
        if not t.get("sl_moved_to_be"):
            trades[trade_id]["sl"] = entry
            trades[trade_id]["sl_moved_to_be"] = True
            changed = True
        save_trades(trades)
        sl_note = f"\n📌 SL перенесён в безубыток ({entry:,.2f} ₽)" if changed else ""
        return (
            f"🎯 <b>TP1 достигнут - {t['ticker']}!</b>\n"
            f"Цена: {price:,.2f} ₽ | TP1: {tp1:,.2f} ₽\n"
            f"Ждём TP2: {tp2:,.2f} ₽{sl_note}"
        )

    if changed:
        save_trades(trades)
    return None

async def _force_close_all(app, fetch_price_fn, moex_stocks):
    """EOD закрытие сессии.
    Логика:
    - Фьючерсы: закрываем всегда (нет переноса)
    - Акции в минусе (pnl < 0) и SL не перенесён в б/у: закрываем
    - Акции в плюсе или SL в б/у: оставляем, уведомляем о переносе
    """
    trades = load_trades()
    open_trades = {k: v for k, v in trades.items() if v["status"] in ("open", "tp1_hit", "tp2_hit")}
    if not open_trades:
        return

    for trade_id, t in open_trades.items():
        ticker    = t["ticker"]
        direction = t["direction"]
        entry     = t["entry"]
        is_long   = direction == "LONG"
        is_future = ticker in FUTURES

        if ticker in moex_stocks:
            figi = moex_stocks[ticker][0]
        elif ticker in FUTURES:
            figi = FUTURES[ticker][0]
        else:
            figi = ""

        price = await fetch_price_fn(figi) if figi else None
        if not price:
            price = entry

        pnl_pct = (price - entry) / entry * 100 if is_long else (entry - price) / entry * 100
        pnl_e   = "📈" if pnl_pct >= 0 else "📉"
        sl_in_be = t.get("sl_moved_to_be") or t.get("sl_moved_to_tp1")

        should_close = is_future or (pnl_pct < 0 and not sl_in_be)

        try:
            if should_close:
                closed = close_trade(trade_id, "EOD", price)
                pnl    = closed.get("pnl_pct", 0) if closed else pnl_pct
                pnl_e2 = "📈" if pnl >= 0 else "📉"
                await app.bot.send_message(
                    t["chat_id"],
                    f"🕐 <b>EOD - {ticker} закрыт</b>\n"
                    f"{'(фьючерс - перенос запрещён)' if is_future else '(минус без защиты SL)'}\n"
                    f"Цена: {price:,.2f} ₽\n"
                    f"{pnl_e2} Результат: {pnl:+.2f}%",
                    parse_mode="HTML"
                )
            else:
                
                sl_note = f"SL в б/у ({t['sl']:,.2f} ₽)" if sl_in_be else f"SL: {t['sl']:,.2f} ₽"
                await app.bot.send_message(
                    t["chat_id"],
                    f"🌙 <b>{ticker} {direction} - перенос на завтра</b>\n"
                    f"Цена: {price:,.2f} ₽  {pnl_e} {pnl_pct:+.2f}%\n"
                    f"{sl_note}\n"
                    f"<i>Сделка продолжается. Основные движения в 10:00 и 15:00 МСК.</i>",
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.warning(f"EOD close/notify alert {ticker}: {e}")

async def monitor_trades_loop(app, fetch_price_fn):
    """Фоновый цикл мониторинга открытых сделок."""
    logger.info("Trade monitor started")
    while True:
        try:
            trades = load_trades()
            open_trades = {k: v for k, v in trades.items() if v["status"] in ("open", "tp1_hit", "tp2_hit")}

            if open_trades:
                for trade_id, t in list(open_trades.items()):
                    ticker = t["ticker"]
                    figi = None
                    if ticker in MOEX_STOCKS:
                        figi = MOEX_STOCKS[ticker][0]
                    elif ticker in FUTURES:
                        figi = FUTURES[ticker][0]

                    if not figi:
                        continue

                    price = await fetch_price_fn(figi)
                    if not price:
                        continue
                    alert_msg = await _check_trade(trade_id, t, price, trades_cache=trades)
                    if alert_msg:
                        try:
                            await app.bot.send_message(t["chat_id"], alert_msg, parse_mode="HTML")
                        except Exception as e:
                            logger.warning(f"Alert send failed: {e}")

            now_msk = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
            if now_msk.hour == 23 and now_msk.minute >= 48:
                today_str = now_msk.strftime("%Y-%m-%d")
                
                r = _get_redis()
                should_run_eod = False
                if r:
                    try:
                        should_run_eod = bool(r.set(RK_LAST_EOD, today_str, nx=True, ex=3600))
                    except Exception as e:
                        logger.warning(f"EOD Redis SETNX failed: {e}")
                        
                        should_run_eod = getattr(monitor_trades_loop, "_last_eod_date", "") != today_str
                        if should_run_eod:
                            monitor_trades_loop._last_eod_date = today_str
                else:
                    should_run_eod = getattr(monitor_trades_loop, "_last_eod_date", "") != today_str
                    if should_run_eod:
                        monitor_trades_loop._last_eod_date = today_str

                if should_run_eod:
                    await _force_close_all(app, fetch_price_fn, MOEX_STOCKS)

        except Exception as e:
            logger.error(f"monitor_trades_loop: {e}")

        await asyncio.sleep(120)

DEFAULT_WATCHLIST = [
    "SBER", "GAZP", "LKOH", "GMKN", "ROSN", "NVTK", "YDEX", "TATN",
    "CHMF", "NLMK", "MOEX", "VTBR", "MGNT", "AFLT",
    
    "MAGN", "RUAL", "ALRS", "PLZL", "POSI", "OZON", "MTSS", "PHOR",
    "SNGS", "TRNFP", "IRAO", "HYDR", "FEES", "PIKK", "RTKM",
]

def load_watchlist() -> list[str]:
    """
    Загружает ватчлист. Никогда не должна тихо схлопываться до горстки
    тикеров - если данные битые, полностью отсутствуют, или в неожиданном
    формате, всегда возвращает полный DEFAULT_WATCHLIST (30 тикеров),
    а не куцый аварийный список.
    """
    try:
        data = _load_json(RK_WATCHLIST, WATCHLIST_FILE, [])
    except Exception as e:
        logger.warning(f"load_watchlist: _load_json упал ({e}), использую дефолт")
        return list(DEFAULT_WATCHLIST)

    if not isinstance(data, list):
        if data:  
            logger.warning(f"load_watchlist: данные не список (type={type(data).__name__}), "
                           f"использую дефолт")
        return list(DEFAULT_WATCHLIST)

    try:
        cleaned = [t.upper() for t in data if isinstance(t, str) and t.upper() in MOEX_STOCKS]
    except Exception as e:
        logger.warning(f"load_watchlist: ошибка обработки списка ({e}), использую дефолт")
        return list(DEFAULT_WATCHLIST)

    return cleaned if cleaned else list(DEFAULT_WATCHLIST)

def save_watchlist(tickers: list[str]):
    _save_json(RK_WATCHLIST, WATCHLIST_FILE, tickers)

def add_to_watchlist(ticker: str) -> tuple[bool, str]:
    ticker = ticker.upper().strip()
    if ticker not in MOEX_STOCKS:
        similar = [t for t in MOEX_STOCKS if t.startswith(ticker[:3])]
        hint = f" Похожие: {', '.join(similar[:4])}" if similar else ""
        return False, f"❌ Тикер {ticker} не найден в базе.{hint}"
    wl = load_watchlist()
    if ticker in wl:
        return False, f"ℹ️ {ticker} уже в ватчлисте."
    if len(wl) >= 100:
        return False, "⚠️ Максимум 100 инструментов. Сначала удали что-нибудь: /remove TICKER"
    wl.append(ticker)
    save_watchlist(wl)
    _, name, sector, *_ = MOEX_STOCKS[ticker]
    return True, f"✅ <b>{ticker}</b> ({name}, {sector}) добавлен в ватчлист.\nВсего: {len(wl)}/100"

def remove_from_watchlist(ticker: str) -> tuple[bool, str]:
    ticker = ticker.upper().strip()
    wl = load_watchlist()
    if ticker not in wl:
        return False, f"ℹ️ {ticker} не найден в твоём ватчлисте."
    wl.remove(ticker)
    save_watchlist(wl)
    return True, f"🗑 <b>{ticker}</b> удалён из ватчлиста. Осталось: {len(wl)}"

def get_msk_time() -> tuple[int, int, int]:
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc.astimezone(timezone(timedelta(hours=3)))
    return now_msk.weekday(), now_msk.hour, now_msk.minute

def msk_now() -> datetime:
    """
    Возвращает текущее время как datetime с явным смещением МСК (UTC+3).
    Использовать ВЕЗДЕ, где время подписывается '... МСК' в сообщениях -
    datetime.now() без таймзоны берёт локальное время сервера (обычно UTC
    на Railway), из-за чего подпись 'МСК' была враньём на 3 часа.
    """
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))

def is_acceptable_entry_time(tf: str) -> tuple[bool, str]:
    if tf not in INTRADAY_TFS:
        return True, ""
    _, hour, minute = get_msk_time()
    current_minutes = hour * 60 + minute
    close_minutes = 23 * 60 + 50
    remaining_minutes = close_minutes - current_minutes

    if remaining_minutes <= 0:
        return False, "Сессия уже закрыта."
    if tf == "1h" and remaining_minutes < 180:
        return False, f"⚠️ До закрытия сессии менее 3 часов ({remaining_minutes} мин)."
    elif tf == "15m" and remaining_minutes < 90:
        return False, f"⚠️ До закрытия сессии менее 1.5 часов ({remaining_minutes} мин)."
    elif tf == "5m" and remaining_minutes < 45:
        return False, f"⚠️ До закрытия сессии менее 45 минут."
    return True, ""

def _tinkoff_headers() -> dict:
    return {
        "Authorization": f"Bearer {TINKOFF_TOKEN}",
        "Content-Type": "application/json",
    }

_usd_rub_rate: float = 0.0
_usd_rub_ts:   float = 0.0

async def fetch_last_price_tinkoff(figi: str) -> float | None:
    try:
        await _tinkoff_rate_limiter.acquire()
        session = _get_http_session()
        async with session.post(
            f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices",
            headers=_tinkoff_headers(), json={("figi" if figi.startswith("BBG") else "instrument_id"): [figi]},
        ) as r:
            if r.status != 200:
                error_text = await r.text()
                logger.warning(f"Tinkoff last price {figi}: HTTP {r.status} - {error_text[:200]}")
                return None
            data = await r.json()
            prices = data.get("lastPrices", [])
            if prices:
                p = prices[0]["price"]
                return float(p.get("units", 0)) + float(p.get("nano", 0)) / 1e9
    except Exception as e:
        logger.warning(f"Tinkoff last price {figi}: {e}")
    return None

async def _get_usd_rub() -> float:
    """Получаем курс USD/RUB через Tinkoff - USDRUBF фьючерс или CNYRUB_TOM."""
    global _usd_rub_rate, _usd_rub_ts
    now = time.time()
    if _usd_rub_rate > 0 and now - _usd_rub_ts < 300:
        return _usd_rub_rate
    
    usd_figi = "BBG0013HGFT4"
    try:
        price = await fetch_last_price_tinkoff(usd_figi)
        if price and price > 10:
            _usd_rub_rate = price
            _usd_rub_ts   = now
            return price
    except Exception:
        pass
    return _usd_rub_rate if _usd_rub_rate > 0 else 90.0  

async def fetch_candles_tinkoff(figi: str, interval: str, limit: int) -> pd.DataFrame | None:
    cache_key = f"candles_{figi}_{interval}_{limit}"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 120:
        return _cache[cache_key]["df"]

    # ИСПРАВЛЕНИЕ: Увеличиваем период для 4h и 1h свечей, чтобы набралось >30 свечей для расчета тренда
    max_days_by_interval = {
        "CANDLE_INTERVAL_1_MIN":   1,
        "CANDLE_INTERVAL_5_MIN":   1,
        "CANDLE_INTERVAL_15_MIN":  2,   # 2 дня = ~50 свечей
        "CANDLE_INTERVAL_HOUR":    14,  # 14 дней = ~100 свечей
        "CANDLE_INTERVAL_4_HOUR":  30,  # 30 дней = ~70 свечей (достаточно для EMA20/EMA50)
        "CANDLE_INTERVAL_DAY":     90,
        "CANDLE_INTERVAL_WEEK":   180,
    }
    delta_days = max_days_by_interval.get(interval, 2)

    _utcnow = datetime.now(timezone.utc).replace(tzinfo=None)
    dt_from = _utcnow - timedelta(days=delta_days)
    dt_to = _utcnow

    id_field = "figi" if figi.startswith("BBG") else "instrument_id"
    body = {
        id_field: figi,
        "from": dt_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to": dt_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "interval": interval,
    }

    try:
        data = None
        session = _get_http_session()
        for attempt in range(3):
            await _tinkoff_rate_limiter.acquire()
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles",
                headers=_tinkoff_headers(), json=body,
            ) as r:
                if r.status == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Tinkoff 429 {figi}, retry {attempt+1} in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if r.status != 200:
                    error_text = await r.text()
                    logger.warning(f"Tinkoff candles {figi}: HTTP {r.status} - {error_text[:200]}")
                    return None
                data = await r.json()
                break
        if data is None:
            return None
    except Exception as e:
        logger.error(f"Tinkoff candles {figi}: {e}")
        return None

    candles = data.get("candles", [])
    if not candles:
        logger.warning(f"Tinkoff candles {figi} [{interval}]: пустой ответ - нет свечей в данных")
        return None

    rows = []
    for c in candles:
        try:
            def units_nano(q):
                return float(q.get("units", 0)) + float(q.get("nano", 0)) / 1e9
            rows.append({
                "timestamp": pd.Timestamp(c["time"]),
                "open": units_nano(c["open"]),
                "high": units_nano(c["high"]),
                "low": units_nano(c["low"]),
                "close": units_nano(c["close"]),
                "volume": float(c.get("volume", 0)),
            })
        except Exception:
            continue

    if not rows:
        return None

    df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
    df = df.tail(limit)

    currency = _instrument_currency.get(figi, "rub").lower()
    if currency == "usd":
        rate = await _get_usd_rub()
        if rate > 10:
            for col in ("open", "high", "low", "close"):
                df[col] = df[col] * rate
            logger.debug(f"Converted {figi} prices USD→RUB at rate {rate:.2f}")

    _cache[cache_key] = {"df": df, "ts": now}
    return df

# === MOEX ISS fallback for candles ===
_MOEX_INTERVAL_MAP = {
    "CANDLE_INTERVAL_1_MIN": 1,
    "CANDLE_INTERVAL_5_MIN": 10,   # MOEX ISS поддерживает только 1, 10, 60, 24
    "CANDLE_INTERVAL_15_MIN": 10,  # MOEX ISS поддерживает только 1, 10, 60, 24
    "CANDLE_INTERVAL_HOUR": 60,
    "CANDLE_INTERVAL_4_HOUR": 60,
    "CANDLE_INTERVAL_DAY": 24,
    "CANDLE_INTERVAL_WEEK": 7,
}

async def _is_futures_ticker(ticker: str) -> bool:
    """Определяет, является ли тикер фьючерсом (есть в словаре FUTURES)."""
    return ticker in FUTURES

async def fetch_candles_moex(ticker: str, interval: str, limit: int) -> pd.DataFrame | None:
    """Fallback: загружает свечи с MOEX ISS когда Tinkoff API недоступен.
    Поддерживает акции (TQBR) и фьючерсы (FORTS)."""
    moex_interval = _MOEX_INTERVAL_MAP.get(interval, 10)
    _utcnow = datetime.now(timezone.utc).replace(tzinfo=None)
    dt_from = _utcnow - timedelta(days=30)
    dt_to = _utcnow
    dt_from_str = dt_from.strftime("%Y-%m-%d")
    dt_to_str = dt_to.strftime("%Y-%m-%d")

    if await _is_futures_ticker(ticker):
        base = f"https://iss.moex.com/iss/engines/futures/markets/forts/securities/{ticker}"
    else:
        base = f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}"

    url = (
        f"{base}/candles.json?interval={moex_interval}"
        f"&from={dt_from_str}&till={dt_to_str}"
        f"&start=0&iss.meta=off"
    )
    try:
        async with _get_http_session().get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                logger.warning(f"MOEX candles {ticker}: HTTP {r.status}")
                return None
            data = await r.json()
    except Exception as e:
        logger.debug(f"MOEX candles {ticker}: {e}")
        return None

    raw = data.get("candles", {}).get("data", [])
    if not raw:
        logger.debug(f"MOEX candles {ticker}: нет данных")
        return None

    columns = data["candles"]["columns"]
    rows = []
    for row in raw:
        try:
            rows.append({
                "timestamp": (pd.Timestamp(row[columns.index("begin")]).tz_localize("Europe/Moscow").tz_convert("UTC") if pd.Timestamp(row[columns.index("begin")]).tzinfo is None else pd.Timestamp(row[columns.index("begin")]).tz_convert("UTC")),
                "open": float(row[columns.index("open")]),
                "high": float(row[columns.index("high")]),
                "low": float(row[columns.index("low")]),
                "close": float(row[columns.index("close")]),
                "volume": float(row[columns.index("volume")]),
            })
        except Exception:
            continue

    if not rows:
        return None

    df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
    df = df.tail(limit)
    # Конвертация USD→RUB для долларовых фьючерсов
    if ticker in FUTURES_USD_PRICED:
        rate = await _get_usd_rub()
        if rate > 10:
            for col in ("open", "high", "low", "close"):
                df[col] = df[col] * rate
            logger.debug(f"MOEX ISS {ticker}: USD→RUB at rate {rate:.2f}")
    logger.info(f"MOEX candles {ticker}: загружено {len(df)} свечей (MOEX ISS fallback)")

async def fetch_stock_data(ticker: str, tf: str = DEFAULT_TF):
    info = MOEX_STOCKS.get(ticker.upper())
    if not info:
        return None, None
    figi, name, sector, *_ = info
    interval, _, limit = TF_MAP.get(tf, TF_MAP[DEFAULT_TF])
    df = await fetch_candles_tinkoff(figi, interval, limit)

    if df is None or len(df) < 20:
        # Пробуем MOEX ISS как fallback
        logger.info(f"fetch_stock_data {ticker}: Тинькофф отдал {len(df) if df is not None else 0} свечей, пробую MOEX ISS...")
        try:
            df_moex = await fetch_candles_moex(ticker, interval, limit)
            if df_moex is not None and len(df_moex) >= 20:
                df = df_moex
                logger.info(f"fetch_stock_data {ticker}: использован MOEX ISS fallback ({len(df)} свечей)")
            else:
                logger.warning(f"fetch_stock_data {ticker}: данных нет даже через MOEX ISS. "
                               f"Свечей: {len(df_moex) if df_moex is not None else 0}")
                return None, {"figi": figi, "name": name, "sector": sector, "ticker": ticker}
        except Exception as moex_err:
            logger.error(f"MOEX ISS fallback error for {ticker}: {moex_err}")
            return None, {"figi": figi, "name": name, "sector": sector, "ticker": ticker}

    if df is not None and len(df) >= 20:
        median_close = float(df["close"].tail(20).median())
        if median_close > 0:
            bad_mask = (df["close"] < median_close / 5) | (df["close"] > median_close * 5)
            if bad_mask.any():
                n_bad = int(bad_mask.sum())
                logger.warning(f"fetch_stock_data {ticker}: отсечено {n_bad} глитч-свечей "
                               f"(медиана {median_close:.2f}, аномалия за пределами x5)")
                df = df[~bad_mask].reset_index(drop=True)

    return df, {"figi": figi, "name": name, "sector": sector, "ticker": ticker}

async def fetch_imoex_regime() -> dict:
    cache_key = "imoex_regime"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 1200:
        return _cache[cache_key]["val"]
    try:
        # ИСПРАВЛЕНИЕ: Берем тикер MOEX (МосБиржа) как прямой прокси рынка
        imoex_ticker = "MOEX" if "MOEX" in MOEX_STOCKS else "SBER"
        figi = MOEX_STOCKS[imoex_ticker][0]
        df = await fetch_candles_tinkoff(figi, "CANDLE_INTERVAL_DAY", 120)
        
        # Если по MOEX данных нет, пробуем SBER как запасной
        if df is None or len(df) < 30:
            imoex_ticker = "SBER"
            figi = MOEX_STOCKS["SBER"][0]
            df = await fetch_candles_tinkoff(figi, "CANDLE_INTERVAL_DAY", 120)

        if df is None or len(df) < 30:
            raise ValueError("Недостаточно данных для IMOEX-прокси")

        close = df["close"].values
        price = close[-1]
        s = pd.Series(close)
        ema20 = float(s.ewm(span=20).mean().iloc[-1])
        ema50 = float(s.ewm(span=50).mean().iloc[-1])
        ema50_arr = s.ewm(span=50).mean().values
        slope_10d = (ema50_arr[-1] - ema50_arr[-10]) / ema50_arr[-10] * 100
        slope_20d = (ema50_arr[-1] - ema50_arr[-20]) / ema50_arr[-20] * 100

        is_bull = price > ema20 > ema50 and slope_10d > 0.05
        is_bear = price < ema20 < ema50 and slope_10d < -0.05

        if is_bull:
            strong = slope_20d > 1.5
            regime = "bull"
            label = ("🟢🟢 IMOEX: сильный бычий тренд" if strong else "🟢 IMOEX: умеренный бычий тренд")
        elif is_bear:
            strong = slope_20d < -1.5
            regime = "bear"
            label = ("🔴🔴 IMOEX: нисходящий тренд" if strong else "🔴 IMOEX: локальная слабость рынка")
        else:
            regime = "neutral"
            label = "⚪ IMOEX: консолидация / боковой рынок"

        result = {
            "regime": regime, "label": label,
            "price": round(price, 2), "ema20": round(ema20, 2), "ema50": round(ema50, 2),
            "slope_10d": round(slope_10d, 3), "slope_20d": round(slope_20d, 3),
            "ticker_used": imoex_ticker,
        }
        _cache[cache_key] = {"val": result, "ts": now}
        return result
    except Exception as e:
        logger.warning(f"fetch_imoex_regime: {e}")
        fallback = {"regime": "neutral", "label": "⚪ IMOEX: тренд неопределен",
                     "price": 0, "ema20": 0, "ema50": 0, "slope_10d": 0, "slope_20d": 0, "ticker_used": "MOEX"}
        _cache[cache_key] = {"val": fallback, "ts": now - 900}
        return fallback

def load_calendar_events() -> list:
    try:
        return _load_json(RK_CALENDAR, CALENDAR_FILE, [])
    except Exception:
        return []

def save_calendar_events(events: list):
    _save_json(RK_CALENDAR, CALENDAR_FILE, events)

def get_upcoming_events(hours_ahead: int = 4) -> list:
    events = load_calendar_events()
    now = datetime.now(timezone.utc)
    upcoming = []
    for ev in events:
        try:
            ev_time = datetime.fromisoformat(ev["datetime_utc"])
            if ev_time.tzinfo is None:
                ev_time = ev_time.replace(tzinfo=timezone.utc)
            diff_h = (ev_time - now).total_seconds() / 3600
            if -1 < diff_h <= hours_ahead:
                ev["hours_ahead"] = round(diff_h, 1)
                upcoming.append(ev)
        except Exception:
            continue
    return sorted(upcoming, key=lambda x: x["hours_ahead"])

def check_dividend_cutoff(ticker: str) -> dict:
    """
    Специальная проверка дивидендной отсечки для конкретного тикера.

    Логика:
    - В день отсечки цена гэпит вниз на размер дивиденда (T+1 расчёты -
      реестр закрывается, новый покупатель уже не получает выплату).
      Это не риск, это МЕХАНИЧЕСКОЕ движение биржи, известное заранее.
    - Окно предупреждения - 3 торговых дня вперёд (в отличие от общего
      check_calendar_block с окном 2 часа - отсечка не сюрприз, она в
      календаре за недели, нет смысла узнавать о ней в последний момент).
    - LONG перед отсечкой - жёстко блокируется (гарантированный гэп вниз).
    - Если отсечка была вчера/сегодня утром - движение уже произошло,
      это ИНФОРМАЦИЯ, а не блок (цена уже скорректировалась).

    Возвращает:
        block:        True если нужно жёстко заблокировать LONG вход
        already_passed: True если отсечка уже прошла (гэп уже был)
        days_until:    дней до отсечки (может быть отрицательным если прошла)
        dividend_info: строка с описанием для алерта
    """
    events = load_calendar_events()
    now = datetime.now(timezone.utc)
    ticker_upper = ticker.upper()

    relevant = []
    for ev in events:
        if ev.get("type") != "dividend_cutoff":
            continue
        if ticker_upper not in [t.upper() for t in ev.get("tickers", [])]:
            continue
        try:
            ev_time = datetime.fromisoformat(ev["datetime_utc"])
            if ev_time.tzinfo is None:
                ev_time = ev_time.replace(tzinfo=timezone.utc)
            days_diff = (ev_time - now).total_seconds() / 86400
            relevant.append((ev, ev_time, days_diff))
        except Exception:
            continue

    if not relevant:
        return {"block": False, "already_passed": False, "days_until": None,
                "dividend_info": ""}

    ev, ev_time, days_diff = min(relevant, key=lambda x: abs(x[2]))

    if days_diff < -0.5:
        return {
            "block": False, "already_passed": True,
            "days_until": round(days_diff, 1),
            "dividend_info": f"Отсечка была {ev_time.strftime('%d.%m')} - гэп уже отыгран",
        }

    if days_diff <= 4.5:
        raw_name = ev.get("name", "")
        div_amount = ""
        if "(" in raw_name and ")" in raw_name:
            div_amount = raw_name.split("(")[-1].rstrip(")")
        amount_str = f" ({div_amount})" if div_amount else ""
        return {
            "block": True, "already_passed": False,
            "days_until": round(days_diff, 1),
            "dividend_info": f"⚠️ Дивидендная отсечка {ev_time.strftime('%d.%m')}{amount_str}",
        }

    return {"block": False, "already_passed": False,
            "days_until": round(days_diff, 1), "dividend_info": ""}

def check_calendar_block(ticker: str = "") -> dict:
    upcoming = get_upcoming_events(hours_ahead=2)
    if not upcoming:
        return {"block": False, "warning": "", "events": [], "score_penalty": 0}
    relevant = []
    for ev in upcoming:
        tickers = ev.get("tickers", [])
        impact  = ev.get("impact", "low")
        if (not tickers or ticker.upper() in [t.upper() for t in tickers]
                or impact in ("high", "critical")):
            relevant.append(ev)
    if not relevant:
        return {"block": False, "warning": "", "events": [], "score_penalty": 0}

    block_events = [e for e in relevant
                    if e["impact"] in ("high", "critical") and e["hours_ahead"] <= 0.5]

    score_penalty = 0
    for ev in relevant:
        h = ev["hours_ahead"]
        if h <= 2:
            if ev["impact"] == "critical":
                score_penalty = max(score_penalty, 25)
            elif ev["impact"] == "high":
                score_penalty = max(score_penalty, 15)
            elif ev["impact"] == "medium":
                score_penalty = max(score_penalty, 5)

    warnings = []
    for ev in relevant[:3]:
        h = ev["hours_ahead"]
        time_str = (f"{abs(h)*60:.0f}м назад" if h < 0 else
                    f"через {h*60:.0f} мин" if h < 1 else f"через {h:.1f} ч")
        imp_e = {"critical": "\U0001f534", "high": "\U0001f7e0", "medium": "\U0001f7e1"}.get(ev["impact"], "\u26aa")
        warnings.append(f"{imp_e} {ev['name']} - {time_str}")

    return {
        "block": len(block_events) > 0,
        "warning": "\n".join(warnings),
        "events": relevant,
        "score_penalty": score_penalty
    }

async def fetch_moex_dividends() -> list:
    events = []
    url = "https://iss.moex.com/iss/statistics/engines/stock/markets/shares/dividends.json"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as r:
                if r.status != 200:
                    return []
                data = await r.json(content_type=None)
                columns = data.get("dividends", {}).get("columns", [])
                rows    = data.get("dividends", {}).get("data", [])
                if not columns or not rows:
                    return []
                col_idx = {c: i for i, c in enumerate(columns)}
                now = datetime.now(timezone.utc)
                for row in rows[:100]:
                    try:
                        ticker_val   = str(row[col_idx["secid"]])
                        cutoff_date  = str(row[col_idx["registryclosedate"]])
                        dividend_val = row[col_idx["value"]]
                        if not cutoff_date or cutoff_date == "None":
                            continue
                        ev_dt = datetime.strptime(cutoff_date, "%Y-%m-%d").replace(
                            hour=7, tzinfo=timezone.utc)
                        if ev_dt < now - timedelta(days=1):
                            continue
                        events.append({
                            "name":         f"Дивидендная отсечка {ticker_val} ({dividend_val} руб.)",
                            "datetime_utc": ev_dt.isoformat(),
                            "impact":       "high",
                            "tickers":      [ticker_val],
                            "type":         "dividend_cutoff",
                            "source":       "moex",
                        })
                    except Exception:
                        continue
    except Exception as e:
        logger.warning(f"MOEX dividends fetch: {e}")
    return events

def get_cbr_dates_2026() -> list:
    """Заседания ЦБ РФ 2026-2027."""
    cbr_dates = [
        "2026-02-14", "2026-03-21", "2026-04-25",
        "2026-06-06", "2026-07-25", "2026-09-12",
        "2026-10-24", "2026-12-19",
        
        "2027-02-12", "2027-03-19", "2027-04-23",
        "2027-06-04", "2027-07-23", "2027-09-10",
        "2027-10-22", "2027-12-17",
    ]
    events = []
    now = datetime.now(timezone.utc)
    for d in cbr_dates:
        try:
            ev_dt = datetime.strptime(d, "%Y-%m-%d").replace(hour=10, minute=30,
                                                              tzinfo=timezone.utc)
            if ev_dt < now - timedelta(days=1):
                continue
            events.append({
                "name":         "🏦 Заседание ЦБ РФ - ключевая ставка",
                "datetime_utc": ev_dt.isoformat(),
                "impact":       "critical",
                "tickers":      [],
                "type":         "cbr_rate",
                "source":       "cbr",
                "note":         "Не торгуй за 2ч до и 1ч после. Волатильность аномальная.",
            })
        except Exception:
            continue
    return events

async def fetch_moex_reports() -> list:
    events = []
    url = "https://iss.moex.com/iss/statistics/engines/stock/markets/shares/securities/reports.json"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params={"limit": 50}) as r:
                if r.status != 200:
                    return []
                data = await r.json(content_type=None)
                cols = data.get("reports", {}).get("columns", [])
                rows = data.get("reports", {}).get("data", [])
                if not cols or not rows:
                    return []
                idx = {c: i for i, c in enumerate(cols)}
                now = datetime.now(timezone.utc)
                for row in rows:
                    try:
                        ticker   = str(row[idx.get("secid", 0)])
                        rep_date = str(row[idx.get("reportdate", 1)])
                        rep_type = str(row[idx.get("reporttype", 2)])
                        if not rep_date or rep_date == "None":
                            continue
                        ev_dt = datetime.strptime(rep_date[:10], "%Y-%m-%d").replace(
                            hour=6, tzinfo=timezone.utc)
                        if ev_dt < now - timedelta(days=1) or ev_dt > now + timedelta(days=14):
                            continue
                        impact = "high" if "МСФО" in rep_type or "IFRS" in rep_type.upper() else "medium"
                        events.append({
                            "name":         f"📋 Отчёт {ticker} - {rep_type}",
                            "datetime_utc": ev_dt.isoformat(),
                            "impact":       impact,
                            "tickers":      [ticker],
                            "type":         "earnings",
                            "source":       "moex",
                        })
                    except Exception:
                        continue
    except Exception as e:
        logger.debug(f"MOEX reports: {e}")
    return events

async def auto_update_figi_loop():
    """Обновляем FIGI раз в 24 часа - не при каждом старте."""
    await asyncio.sleep(60)  
    while True:
        try:
            count, err, updated_list, not_found_list, fut_count = await update_figi_data()
            if count:
                nf_str = f", не найдены: {', '.join(not_found_list[:10])}" if not_found_list else ""
                logger.info(f"Auto FIGI update: stocks {len(updated_list)} OK, futures {fut_count} OK{nf_str}")
            if err:
                logger.warning(f"Auto FIGI update error: {err}")
        except Exception as e:
            logger.warning(f"Auto FIGI update exception: {e}")
        await asyncio.sleep(86400)  

async def auto_update_futures_loop():
    """
    Обновляет FIGI фьючерсов каждые 6 часов.

    Раньше fetch_nearest_futures() вызывалась ОДИН раз при старте бота и
    больше никогда - если процесс не перезапускался в момент квартальной
    экспирации (март/июнь/сентябрь/декабрь, 15 число), FIGI в памяти
    оставались привязаны к уже истёкшему контракту, и Tinkoff API переставал
    отдавать по нему данные - фьючерсы просто переставали приходить.
    """
    while True:
        try:
            result = await fetch_nearest_futures()
            if result:
                logger.info(f"Futures auto-refresh: {len(result)} base assets updated")
        except Exception as e:
            logger.warning(f"auto_update_futures_loop: {e}")
        await asyncio.sleep(6 * 3600)  

async def auto_update_calendar():
    try:
        cbr_events = get_cbr_dates_2026()
        div_events = await fetch_moex_dividends()
        rep_events = await fetch_moex_reports()

        all_events = cbr_events + div_events + rep_events

        seen = set()
        unique = []
        for ev in all_events:
            key = f"{ev['name']}_{ev['datetime_utc'][:10]}"
            if key not in seen:
                seen.add(key)
                unique.append(ev)

        save_calendar_events(unique)
        logger.info(f"Calendar updated: {len(cbr_events)} ЦБ + {len(div_events)} дивиденды + {len(rep_events)} отчёты = {len(unique)} событий")
        return len(unique)
    except Exception as e:
        logger.error(f"auto_update_calendar: {e}")
        return 0

RSSHUB_URL = os.getenv("RSSHUB_URL", "https://rsshub.app").strip().rstrip("/")
if RSSHUB_URL and not RSSHUB_URL.startswith(("http://", "https://")):
    RSSHUB_URL = "https://" + RSSHUB_URL

def _tg(channel: str) -> str:
    """Строит URL Telegram-канала через текущий RSSHub инстанс."""
    return f"{RSSHUB_URL}/telegram/channel/{channel}"

RSS_SOURCE_TIER = {
    "www.moex.com":           1,
    "www.interfax.ru":        1,
    "tass.ru":                1,
    "www.kommersant.ru":      2,
    "smart-lab.ru":           2,
}

RUSSIAN_NEWS_RSS = [
    "https://www.moex.com/export/news.aspx?mode=rss",
    "https://www.interfax.ru/rss.asp",
    "https://tass.ru/rss/v2.xml",
    "https://www.kommersant.ru/RSS/news.xml",
    "https://smart-lab.ru/blog/feed/",
    "https://1prime.ru/export/rss2/index.xml",
    "https://www.finam.ru/analysis/conews/rsspoint/",
    "https://rbc.ru/rss",
    
    _tg("interfaxonline"),   
    _tg("cbr_official"),     
    _tg("markettwits"),      
    _tg("rosbiznessman"),    
    _tg("moexpert"),         
    _tg("smartlabonline"),   
    _tg("russianmacro"),     
    _tg("selfinvestor"),     
]

COMMODITY_NEWS_RSS = [
    
    "https://oilprice.com/rss/main",
    
    "https://www.mining.com/feed/",
    
    "https://www.kitco.com/rss/KitcoNews.xml",
    
    "https://www.marketwatch.com/rss/topstories",
    
    "https://finance.yahoo.com/rss/2.0/headline?s=CL=F,GC=F,SI=F,HG=F",
]

COMMODITY_SECTOR_MAP: dict[str, list[str]] = {
    "нефтегаз": [
        
        "oil", "crude", "brent", "wti", "opec", "natural gas", "lng",
        "energy", "barrel", "petroleum", "refinery", "pipeline",
        "urals", "oil price", "oil production", "oil demand",
        
        "iran", "hormuz", "strait of hormuz", "saudi arabia", "aramco",
        "venezuela", "libya", "iraq", "kuwait", "opec+",
        "tanker", "oil sanctions", "energy crisis", "fuel supply",
        "middle east conflict", "gulf tension", "red sea",
    ],
    "металлы": [
        "steel", "aluminium", "aluminum", "nickel", "copper", "iron ore",
        "metal", "mining", "palladium", "platinum", "cobalt",
        "stainless", "hot-rolled", "cold-rolled", "rebar",
        
        "lme ban", "london metal exchange", "metal sanctions",
        "china export ban", "rare earth ban", "nickel sanctions",
    ],
    "горнодобыча": [
        "mining", "ore", "diamond", "coal", "potash", "lithium",
        "rare earth", "extraction", "mineral",
    ],
    "золото": [
        "gold", "silver", "precious metal", "bullion", "xau",
        "gold price", "gold demand", "central bank gold",
        "safe haven", "flight to safety",
    ],
    "уголь": [
        "coal", "coking coal", "thermal coal", "coke",
    ],
    "химия": [
        "fertilizer", "ammonia", "nitrogen", "potash", "chemical",
        "urea", "phosphate",
    ],
}

COMMODITY_SECTORS = set(COMMODITY_SECTOR_MAP.keys())

SECTOR_NEWS_MAP: dict[str, list[str]] = {
    "банки": [
        "банковский сектор", "норматив цб", "банковский надзор",
        "капитал банков", "резервы банков", "просроченная задолженность",
        "ипотечное кредитование", "потребительское кредитование",
        "санкции против банков", "swift", "корреспондентские счета",
        "отзыв лицензии банка", "докапитализация банка",
    ],
    "финансы": [
        "фондовый рынок", "мосбиржа санкции", "клиринг санкции",
        "депозитарий санкции", "торги приостановлены", "делистинг",
        "иностранные инвесторы россия", "заморозка активов",
    ],
    "IT": [
        "санкции на экспорт технологий", "запрет поставок микрочипов",
        "экспортный контроль полупроводники", "уход it-компаний",
        "параллельный импорт электроники", "блокировка сервисов россия",
        "отечественное по", "импортозамещение по",
    ],
    "телеком": [
        "санкции на оборудование связи", "5g запрет", "роскомнадзор",
        "блокировка интернет-сервисов", "отключение от сетей",
    ],
    "ритейл": [
        "потребительский спрос", "инфляция продукты", "цены на товары",
        "уход ритейлеров россия", "параллельный импорт товары",
        "продовольственное эмбарго", "запрет на импорт товаров",
    ],
    "e-commerce": [
        "санкции на платежи", "блокировка платёжных систем",
        "трансграничная торговля запрет",
    ],
    "медицина": [
        "запрет на поставки лекарств", "санкции на медоборудование",
        "дефицит лекарств", "локализация производства лекарств",
    ],
    "транспорт": [
        "санкции на авиаперевозки", "запрет полётов", "лизинг самолётов санкции",
        "морские перевозки санкции", "порты санкции", "страхование судов запрет",
    ],
    "девелопмент": [
        "льготная ипотека отмена", "ключевая ставка недвижимость",
        "субсидированная ипотека", "спрос на жильё",
    ],
}

SECTOR_NEWS_SECTORS = set(SECTOR_NEWS_MAP.keys())

MACRO_KEYWORDS = [
    
    "ключевая ставка", "цб рф подняла", "цб рф снизил", "заседание цб",
    "набиуллина", "инфляция в россии", "цб повысил", "цб понизил",
    
    "курс рубля", "рубль ослаб", "рубль укрепил", "доллар превысил",
    
    "новый пакет санкций", "санкции против россии", "минфин сша санкции",
    "запрет на импорт", "нефтяное эмбарго", "swift россия",
    
    "federal reserve", "fed rate", "фрс повысил", "фрс снизил",
    "ecb rate", "interest rate decision",
    
    "военная эскалация", "мобилизация", "военное положение",
    "g7 summit", "g20 summit",
]

# === SECTOR_KEYS с очищенными ключами ===
SECTOR_KEYS = {
    "нефтегаз":    {"нефть", "газ", "oil", "gas", "ormuz", "опек", "лукойл", "rosneft", "баррель", "brent", "wti"},
    "металлы":     {"золото", "silver", "металл", "горнодобыча", "gold", "copper", "никель", "сталь"},
    "банки":       {"банк", "сбер", "втб", "цб", "ставка", "рубль", "ключевая ставка", "рефинансирование"},
    "финансы":     {"финансы", "фондовый", "индекс", "moex", "rts", "биржа", "ipo"},
    "энергетика":  {"электроэнергия", "уголь", "энергоноситель", "iea", "генерация", "мощность"},
    "АПК":         {"агро", "зерно", "удобрение", "сельхоз", "пшеница", "урожай"},
    "IT":          {"айти", "технологии", "software", "софт", "цифровизация", "искусственный интеллект", "it-сектор", "it-компани"},
    "телеком":     {"телеком", "связь", "мобильный", "интернет", "роуминг"},
    "ритейл":      {"ритейл", "retail", "магазин", "торговля", "потребительский"},
    "транспорт":   {"транспорт", "логистика", "перевозк", "жд", "авиа", "порт"},
    "девелопмент": {"девелопмент", "строительство", "недвижимость", "жильё", "ипотека"},
    "химия":       {"химия", "химический", "удобрение", "минеральный"},
    "медицина":    {"медицина", "фармацевтика", "здравоохранение", "больница"},
    "e-commerce":   {"e-commerce", "ozon", "wildberries", "маркетплейс", "интернет-торговля"},
    "горнодобыча": {"горнодобыча", "рудник", "добыча", "уголь"},
    "золото":      {"золото", "gold", "драгметалл"},
    "машиностроение": {"машиностроение", "автопром", "автомобиль", "завод"},
    "кибербезопасность": {"кибербезопасность", "безопасность", "хакер", "утечка данных"},
    "валюта":      {"валюта", "доллар", "юань", "евро", "курс рубля"},
}

def tag_sectors_by_text(text: str) -> list[str]:
    """Определяет сектора по ключевым словам с точной проверкой границ слов (Word Boundaries)."""
    text_low = text.lower()
    matched = []
    for sector, keywords in SECTOR_KEYS.items():
        for kw in keywords:
            # Для коротких слов (<=4 символов) проверяем строго изолированное слово (\b)
            if len(kw) <= 4:
                if re.search(r'\b' + re.escape(kw) + r'\b', text_low):
                    matched.append(sector)
                    break
            else:
                if kw in text_low:
                    matched.append(sector)
                    break
    return matched

NEWS_MAX_AGE_MINUTES = 120   

NEWS_MAX_AGE_LIQUID_MINUTES = 10
NEWS_CACHE_TTL       = 90    

MILITARY_GEO_STOPWORDS = {
    "харьков", "херсон", "запорожье", "донецк", "луганск", "мариуполь",
    "бахмут", "авдеевка", "купянск", "одесса", "николаев", "киев",
    "украин", "нато", "фронт", "обстрел", "взрыв", "ракетн", "дрон",
    "газораспределительн", "электростанц", "подстанц", "теплоэлектр",
    "военн", "армия ударила", "армия нанесла", "вс рф", "всу",
    "мобилизац", "эвакуац", "беженц", "гуманитарн коридор",
    "изра", "газа", "ливан", "сирия", "иран", "йемен", "хути",
}

OFFTOPIC_STOPWORDS = {
    "теннис", "футбол", "хоккей", "олимпиад", "чемпионат мира по",
    "sportaran", "sport24", "матч тв", "спортивный обзор",
    "актриса", "певиц", "певец", "концерт", "кинопремьера", "сериал",
    "выступать за сборную", "выступать за", "тренер сборной",
    "погода в", "прогноз погоды", "гороскоп",
}

FACT_PATTERNS: list[tuple[str, str, int, bool]] = [
    
    ("не выплачивать дивиденд",  "отмена дивидендов",      -9,  True),
    ("не выплачивать дивиденды", "отмена дивидендов",      -9,  True),
    ("рекомендовал не выплач",   "отмена дивидендов",      -9,  True),
    ("отказался от дивиденд",    "отмена дивидендов",      -9,  True),
    ("отменил дивиденд",         "отмена дивидендов",      -9,  True),
    ("не будет дивидендов",      "отмена дивидендов",      -9,  True),
    ("дивиденды не рекомендован","отмена дивидендов",      -9,  True),
    ("без дивидендов",           "отмена дивидендов",      -8,  True),
    ("дивиденды не предусмотр",  "отмена дивидендов",      -8,  True),
    
    ("рекомендовал дивиденд",    "рекомендация дивидендов", 9,  True),
    ("рекомендует дивиденд",     "рекомендация дивидендов", 9,  True),
    ("дивиденды за",             "дивиденды объявлены",     8,  True),
    ("дивиденды выше",           "дивиденды выше ожиданий", 10, True),
    
    ("обратный выкуп",           "байбек",                  9,  True),
    ("buyback",                  "байбек",                  9,  True),
    ("байбек",                   "байбек",                  9,  True),
    
    ("рекордная прибыль",        "рекордная прибыль",       8,  True),
    ("прибыль выросла",          "рост прибыли",            7,  True),
    ("выручка выросла",          "рост выручки",            6,  True),
    ("чистая прибыль выросла",   "рост прибыли",            7,  True),
    ("прибыль увеличилась",      "рост прибыли",            6,  True),
    
    ("дополнительная эмиссия",   "допэмиссия",             -10, True),
    ("допэмиссия",               "допэмиссия",             -10, True),
    ("spo",                      "SPO",                    -8,  True),
    
    ("чистый убыток",            "убыток",                 -8,  True),
    ("получил убыток",           "убыток",                 -8,  True),
    ("убыток вырос",             "рост убытка",            -9,  True),
    
    ("новые санкции",            "санкции",                -9,  False),
    ("санкции против",           "санкции",                -9,  False),
    ("попал под санкции",        "санкции",                -10, False),
]

OPINION_PATTERNS = [
    "считает аналитик", "по мнению", "эксперт полагает", "аналитики ожидают",
    "прогноз аналитик", "целевая цена", "рекомендация покупать",
    "обзор рынка", "итоги торгов", "утренний обзор",
]

def classify_news_item(title: str) -> dict:
    tl = title.lower()
    is_opinion = any(op in tl for op in OPINION_PATTERNS)
    for pattern, event_label, weight, is_corp in FACT_PATTERNS:
        
        if len(pattern) <= 4 and re.fullmatch(r'[a-z0-9]+', pattern):
            if re.search(r'\b' + re.escape(pattern) + r'\b', tl):
                return {"event": event_label, "weight": weight, "is_corporate": is_corp,
                        "is_opinion": False, "is_fact": True}
        elif pattern in tl:
            return {"event": event_label, "weight": weight, "is_corporate": is_corp,
                    "is_opinion": False, "is_fact": True}
    return {"event": "новость", "weight": 0, "is_corporate": False,
            "is_opinion": is_opinion, "is_fact": False}

async def _fetch_rss(session: aiohttp.ClientSession, url: str, headers: dict) -> list[dict]:
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status != 200:
                logger.warning(f"RSS HTTP {r.status}: {url}")
                return []
            text = await r.text(errors="replace")
            root = ET.fromstring(text)
            items = []
            for item in root.findall(".//item")[:20]:
                items.append({
                    "title": item.findtext("title", ""),
                    "link": item.findtext("link", ""),
                    "pub": item.findtext("pubDate", "")[:16],
                    "desc": item.findtext("description", "")[:300],
                })
            
            if "/telegram/channel/" in url:
                ch_name = url.split("/")[-1]
                logger.info(f"✅ Telegram RSSHub OK: {ch_name} ({len(items)} новостей)")
            return items
    except Exception as e:
        now_rss = time.time()
        if not hasattr(_fetch_rss, "_last_errors"):
            _fetch_rss._last_errors = {}
        last_ts = _fetch_rss._last_errors.get(url, 0)
        if now_rss - last_ts > 300:
            logger.warning(f"RSS error {url}: {e}")
            _fetch_rss._last_errors[url] = now_rss
        return []

def _word_match(word: str, text: str) -> bool:
    """Word-boundary матч для тикеров и коротких слов."""
    if not word:
        return False
    return bool(re.search(r'\b' + re.escape(word.lower()) + r'\b', text.lower()))

def _marker_match(marker: str, text: str) -> bool:
    """
    Умный матч маркеров компании:
    - Спецсимволы (эн+, en+) - substring
    - Короткие <=5 символов - word-boundary + защита от дефисных слов (мтс-банк)
    - Длинные - stem-матч для учёта падежей (мосбиржи → мосбирж)
    """
    if not marker:
        return False
    marker_l = marker.lower()
    text_l   = text.lower()

    if re.search(r'[+\-\.]', marker_l):
        return marker_l in text_l

    if len(marker_l) <= 5:
        if not re.search(r'\b' + re.escape(marker_l) + r'\b', text_l):
            return False
        idx = text_l.find(marker_l)
        while idx != -1:
            end = idx + len(marker_l)
            if end < len(text_l) and text_l[end] == '-':
                idx = text_l.find(marker_l, end)
                continue
            return True
        return False
    else:
        stem = marker_l[:-2] if len(marker_l) > 5 else marker_l
        return stem in text_l

async def fetch_russian_news(ticker: str = "", sector: str = "") -> list[dict]:
    cache_key = f"news_{ticker}_{sector}"
    now = time.time()
    if cache_key in _news_cache and now - _news_cache[cache_key]["ts"] < NEWS_CACHE_TTL:
        return _news_cache[cache_key]["items"]

    company_name = ""
    company_markers: set[str] = set()
    if ticker and ticker.upper() in MOEX_STOCKS:
        entry = MOEX_STOCKS[ticker.upper()]
        _, company_name, _, *rest = entry
        company_markers = rest[0] if rest else set()

    specific_words: set[str] = set()
    generic_words:  set[str] = set()

    if ticker:
        specific_words.add(ticker.lower())

    if company_markers:
        specific_words.update(company_markers)
    elif company_name:
        
        GENERIC_NAME_PARTS = {
            "груп", "групп", "банк", "нефт", "газ", "энерг", "холд",
            "корп", "акци", "инвест", "финанс", "капит", "фонд",
        }
        for w in company_name.lower().split():
            if len(w) > 4 and not any(g in w for g in GENERIC_NAME_PARTS):
                specific_words.add(w)

    generic_words.update([
        "дивиденд", "байбек", "buyback", "обратный выкуп",
        "допэмиссия", "spo", "санкции", "прибыль", "выручка", "убыток",
    ])
    if sector and sector in SECTOR_KEYWORDS:
        generic_words.update(SECTOR_KEYWORDS[sector])

    all_search_words = specific_words | generic_words

    raw_items: list[dict] = []
    now_dt = datetime.now(timezone.utc)

    session = _get_http_session()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MOEXBot/1.0)"}
    tasks = [_fetch_rss(session, url, headers) for url in RUSSIAN_NEWS_RSS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for url, result in zip(RUSSIAN_NEWS_RSS, results):
        if isinstance(result, Exception) or not result:
            continue
        for item in result:
            title = item.get("title", "").strip()
            if not title:
                continue

            title_lower = title.lower()
            if any(sw in title_lower for sw in MILITARY_GEO_STOPWORDS):
                continue

            if any(sw in title_lower for sw in OFFTOPIC_STOPWORDS):
                continue

            full    = (title + " " + item.get("desc", "")).lower()
            matched = [w for w in all_search_words if w in full]
            if not matched:
                continue

            is_specific = (
                _word_match(ticker, title) or
                any(_marker_match(m, title) for m in company_markers)
            )

            other_mentions = False
            for other_tk, other_entry in MOEX_STOCKS.items():
                if other_tk == ticker:
                    continue
                _, other_name, _, *other_rest = other_entry
                other_markers = other_rest[0] if other_rest else set()

                other_found = (
                    _word_match(other_tk, title) or
                    any(_marker_match(m, title) for m in other_markers) or
                    (len(other_name) > 5 and other_name.lower() in title.lower())
                )
                if not other_found:
                    continue

                our_found = (
                    _word_match(ticker, title) or
                    any(_marker_match(m, title) for m in company_markers)
                )
                if not our_found:
                    other_mentions = True
                    break

            if not other_mentions:
                if "софтлайн" in title.lower() or "sofl" in title.lower():
                    if not (_word_match(ticker, title) or
                            any(_marker_match(m, title) for m in company_markers)):
                        other_mentions = True

            if other_mentions:
                is_specific = False

            only_generic_matched = all(w in generic_words for w in matched)
            if only_generic_matched and not is_specific:
                continue

            cls = classify_news_item(title)

            is_sector_relevant = bool(
                sector and sector in SECTOR_KEYWORDS and
                any(kw in full for kw in SECTOR_KEYWORDS[sector])
            )
            if cls["is_fact"] and cls["weight"] != 0 and not is_specific and not is_sector_relevant:
                cls = {**cls, "weight": 0, "is_fact": False}

            if cls["is_corporate"] and cls["is_fact"] and not is_specific:
                continue

            pub_str = item.get("pub", "")
            news_age_min = 0
            try:
                pub_dt = parsedate_to_datetime(pub_str)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                news_age_min = (now_dt - pub_dt).total_seconds() / 60
            except Exception:
                pass

            source_domain = url.split("/")[2] if "/" in url else url
            raw_items.append({
                "title": title[:200], "link": item.get("link", ""),
                "pub": pub_str, "source": source_domain,
                "source_tier": RSS_SOURCE_TIER.get(source_domain, 2),
                "is_specific": is_specific, "is_fact": cls["is_fact"],
                "is_opinion": cls["is_opinion"], "is_corporate": cls.get("is_corporate", False),
                "event": cls["event"], "weight": cls["weight"],
                "matched": [w for w in matched if w in specific_words][:3] or matched[:3],
                "age_min": round(news_age_min, 0),
                "effective_weight": cls["weight"] if news_age_min <= NEWS_MAX_AGE_MINUTES else 0,
            })

    dedup_len = int(get_bot_settings()["news_dedup_similarity"])
    seen_events, seen_titles, unique = set(), set(), []
    for it in raw_items:
        event_key = f"{ticker}_{it.get('event', '')}" if it.get("event") else ""
        norm = re.sub(r'[^\w\s]', '', it["title"].lower())[:dedup_len].strip()

        if event_key and event_key in seen_events:
            continue
        if norm and norm in seen_titles:
            continue

        if event_key:
            seen_events.add(event_key)
        if norm:
            seen_titles.add(norm)
        unique.append(it)

    def sort_key(x):
        freshness = 0 if x["age_min"] <= 30 else (1 if x["age_min"] <= 120 else 2)
        if x["is_corporate"] and x["is_fact"]:
            return (freshness, -abs(x["weight"]))
        if x["is_fact"]:
            return (freshness + 1, -abs(x["weight"]))
        if x["is_specific"] and not x["is_opinion"]:
            return (freshness + 2, 0)
        return (5, 0)

    unique.sort(key=sort_key)
    unique = unique[:10]
    _news_cache[cache_key] = {"items": unique, "ts": now}
    return unique

async def fetch_market_news() -> list[dict]:
    return await fetch_russian_news()


async def fetch_macro_news() -> list[dict]:
    """Obshcherynochnye/makro novosti (alias na obshchiy russkiy feed)."""
    return await fetch_russian_news()

async def fetch_commodity_news(sector: str) -> list[dict]:
    """
    Загружает мировые сырьевые новости для указанного сектора.
    Работает только для секторов из COMMODITY_SECTORS.
    Возвращает до 5 свежих релевантных новостей на английском.
    """
    if sector not in COMMODITY_SECTORS:
        return []

    cache_key = f"commodity_{sector}"
    now = time.time()
    if cache_key in _news_cache and now - _news_cache[cache_key]["ts"] < NEWS_CACHE_TTL:
        return _news_cache[cache_key]["items"]

    keywords = COMMODITY_SECTOR_MAP.get(sector, [])
    if not keywords:
        return []

    session  = _get_http_session()
    headers  = {"User-Agent": "Mozilla/5.0 (compatible; MOEXBot/1.0)"}
    now_dt   = datetime.now(timezone.utc)

    tasks   = [_fetch_rss(session, url, headers) for url in COMMODITY_NEWS_RSS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    raw: list[dict] = []
    for url, result in zip(COMMODITY_NEWS_RSS, results):
        if isinstance(result, Exception) or not result:
            continue
        for item in result:
            title = item.get("title", "").strip()
            if not title:
                continue
            full  = (title + " " + item.get("desc", "")).lower()
            matched = [kw for kw in keywords if kw in full]
            if not matched:
                continue

            news_age_min = 0
            pub_str = item.get("pub", "")
            try:
                pub_dt = parsedate_to_datetime(pub_str)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                news_age_min = (now_dt - pub_dt).total_seconds() / 60
            except Exception:
                pass

            if news_age_min > NEWS_MAX_AGE_MINUTES:
                continue

            source_domain = url.split("/")[2] if "/" in url else url
            raw.append({
                "title":    title[:200],
                "link":     item.get("link", ""),
                "pub":      pub_str,
                "source":   source_domain,
                "matched":  matched[:3],
                "age_min":  round(news_age_min, 0),
                "is_commodity": True,
            })

    dedup_len = int(get_bot_settings()["news_dedup_similarity"])
    seen, unique = set(), []
    for it in raw:
        norm = re.sub(r'[^\w\s]', '', it["title"].lower())[:dedup_len].strip()
        if norm and norm not in seen:
            seen.add(norm)
            unique.append(it)

    unique.sort(key=lambda x: x["age_min"])
    unique = unique[:5]

    _news_cache[cache_key] = {"items": unique, "ts": now}
    return unique

async def fetch_sector_news(sector: str) -> list[dict]:
    """
    Загружает секторальные регуляторные/макро новости для нефинансовых
    секторов (банки, IT, ритейл, телеком, медицина, транспорт,
    девелопмент) через русскоязычные источники (RUSSIAN_NEWS_RSS) -
    в отличие от fetch_commodity_news, которая использует англоязычные
    commodity-издания и не годится для регуляторики ЦБ/санкций/потребрынка.
    Возвращает до 5 свежих релевантных новостей.
    """
    if sector not in SECTOR_NEWS_SECTORS:
        return []

    cache_key = f"sectornews_{sector}"
    now = time.time()
    if cache_key in _news_cache and now - _news_cache[cache_key]["ts"] < NEWS_CACHE_TTL:
        return _news_cache[cache_key]["items"]

    keywords = SECTOR_NEWS_MAP.get(sector, [])
    if not keywords:
        return []

    session  = _get_http_session()
    headers  = {"User-Agent": "Mozilla/5.0 (compatible; MOEXBot/1.0)"}
    now_dt   = datetime.now(timezone.utc)

    tasks   = [_fetch_rss(session, url, headers) for url in RUSSIAN_NEWS_RSS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    raw: list[dict] = []
    for url, result in zip(RUSSIAN_NEWS_RSS, results):
        if isinstance(result, Exception) or not result:
            continue
        for item in result:
            title = item.get("title", "").strip()
            if not title:
                continue

            title_lower = title.lower()
            if any(sw in title_lower for sw in MILITARY_GEO_STOPWORDS):
                continue
            if any(sw in title_lower for sw in OFFTOPIC_STOPWORDS):
                continue

            full    = (title + " " + item.get("desc", "")).lower()
            matched = [kw for kw in keywords if kw in full]
            if not matched:
                continue

            news_age_min = 0
            pub_str = item.get("pub", "")
            try:
                pub_dt = parsedate_to_datetime(pub_str)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                news_age_min = (now_dt - pub_dt).total_seconds() / 60
            except Exception:
                pass

            if news_age_min > SECTOR_MODIFIER_DEFAULT_DAYS * 24 * 60:
                continue

            source_domain = url.split("/")[2] if "/" in url else url
            raw.append({
                "title":    title[:200],
                "link":     item.get("link", ""),
                "pub":      pub_str,
                "source":   source_domain,
                "matched":  matched[:3],
                "age_min":  round(news_age_min, 0),
                "is_sector_news": True,
            })

    dedup_len = int(get_bot_settings()["news_dedup_similarity"])
    seen, unique = set(), []
    for it in raw:
        norm = re.sub(r'[^\w\s]', '', it["title"].lower())[:dedup_len].strip()
        if norm and norm not in seen:
            seen.add(norm)
            unique.append(it)

    unique.sort(key=lambda x: x["age_min"])
    unique = unique[:5]

    _news_cache[cache_key] = {"items": unique, "ts": now}
    return unique
    """
    Загружает макро-новости уровня 'весь рынок MOEX' - ставка ЦБ, курс рубля,
    санкции общего характера, глобальные геополитические события.
    В отличие от fetch_commodity_news не привязана к сектору тикера -
    показывается для любого запроса, если найдено что-то свежее.
    """
    cache_key = "macro_news"
    now = time.time()
    if cache_key in _news_cache and now - _news_cache[cache_key]["ts"] < NEWS_CACHE_TTL:
        return _news_cache[cache_key]["items"]

    session = _get_http_session()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MOEXBot/1.0)"}
    now_dt  = datetime.now(timezone.utc)

    all_sources = RUSSIAN_NEWS_RSS + COMMODITY_NEWS_RSS
    tasks   = [_fetch_rss(session, url, headers) for url in all_sources]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    raw: list[dict] = []
    for url, result in zip(all_sources, results):
        if isinstance(result, Exception) or not result:
            continue
        for item in result:
            title = item.get("title", "").strip()
            if not title:
                continue
            title_lower = title.lower()
            if any(sw in title_lower for sw in MILITARY_GEO_STOPWORDS):
                
                if not any(mk in title_lower for mk in
                          ("военная эскалация", "мобилизация", "военное положение")):
                    continue

            if any(sw in title_lower for sw in OFFTOPIC_STOPWORDS):
                continue

            full    = (title + " " + item.get("desc", "")).lower()
            matched = [kw for kw in MACRO_KEYWORDS if kw in full]
            if not matched:
                continue

            news_age_min = 0
            pub_str = item.get("pub", "")
            try:
                pub_dt = parsedate_to_datetime(pub_str)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                news_age_min = (now_dt - pub_dt).total_seconds() / 60
            except Exception:
                pass

            if news_age_min > NEWS_MAX_AGE_MINUTES:
                continue

            source_domain = url.split("/")[2] if "/" in url else url
            raw.append({
                "title":    title[:200],
                "link":     item.get("link", ""),
                "pub":      pub_str,
                "source":   source_domain,
                "matched":  matched[:3],
                "age_min":  round(news_age_min, 0),
                "is_macro": True,
            })

    dedup_len = int(get_bot_settings()["news_dedup_similarity"])
    seen, unique = set(), []
    for it in raw:
        norm = re.sub(r'[^\w\s]', '', it["title"].lower())[:dedup_len].strip()
        if norm and norm not in seen:
            seen.add(norm)
            unique.append(it)

    unique.sort(key=lambda x: x["age_min"])
    unique = unique[:3]  

    _news_cache[cache_key] = {"items": unique, "ts": now}
    return unique

SECTOR_MODIFIER_DEFAULT_DAYS = 3  
                                  
def _load_sector_modifiers() -> dict[str, dict]:
    """Читает все активные секторальные модификаторы из Redis, отфильтровывая
    протухшие по expires_at на лету."""
    r = _get_redis()
    if not r:
        return {}
    try:
        raw_map = r.hgetall(RK_SECTOR_MODIFIERS)
    except Exception as e:
        logger.warning(f"_load_sector_modifiers hgetall: {e}")
        return {}

    now = time.time()
    result = {}
    expired_keys = []
    for sector, raw in raw_map.items():
        try:
            mod = json.loads(raw)
            if mod.get("expires_at", 0) > now:
                result[sector] = mod
            else:
                expired_keys.append(sector)
        except Exception:
            expired_keys.append(sector)

    if expired_keys:
        try:
            r.hdel(RK_SECTOR_MODIFIERS, *expired_keys)
        except Exception:
            pass

    return result

def _set_sector_modifier(sector: str, direction: str, strength: int,
                          reason: str, days: float = SECTOR_MODIFIER_DEFAULT_DAYS) -> None:
    """Сохраняет секторальный модификатор с истечением срока."""
    r = _get_redis()
    if not r:
        return
    try:
        mod = {
            "direction":   direction,  
            "strength":    strength,   
            "reason":      reason,     
            "set_at":      time.time(),
            "expires_at":  time.time() + days * 86400,
        }
        r.hset(RK_SECTOR_MODIFIERS, sector, json.dumps(mod, ensure_ascii=False))
        logger.info(f"Sector modifier set: {sector} -> {direction} (strength={strength}, "
                   f"days={days}): {reason}")
    except Exception as e:
        logger.warning(f"_set_sector_modifier {sector}: {e}")

async def classify_geopolitical_impact(title: str, source_context: str = "") -> dict | None:
    """
    AI-оценка влияния новости на предопределённые сектора.
    Сектора определяются через tag_sectors_by_text (ключевые слова),
    AI только оценивает направление и силу влияния.
    Возвращает None если нет подходящего сектора или новость незначима.
    """
    if not get_ai_enabled():
        return None
    if not gemini_model and not groq_client:
        return None

    # Сначала определяем сектора по ключевым словам (без AI)
    text_for_tagging = title + " " + source_context
    sectors = tag_sectors_by_text(text_for_tagging)
    if not sectors:
        return None

    sectors_str = ", ".join(sectors)
    prompt = f"""Ты аналитик российского фондового рынка (MOEX). Оцени влияние новости на указанные сектора.

НОВОСТЬ: "{title}"
{f'КОНТЕКСТ: {source_context}' if source_context else ''}

ЗАТРОНУТЫЕ СЕКТОРА (определены по ключевым словам): {sectors_str}

Определи:
1. Какое влияние на эти сектора: bullish (бычье), bearish (медвежье) или neutral (нейтрально)?
2. Сила влияния от 1 до 10.
3. На сколько дней актуально (1-7)?
4. Короткое объяснение на русском для трейдера.

Если новость незначительна или шум - верни "significant": false.

Ответь СТРОГО в формате JSON:
{{"significant": true/false, "direction": "bullish/bearish/neutral", "strength": 5, "days_relevant": 3, "reason_ru": "объяснение"}}"""

    try:
        loop = asyncio.get_event_loop()
        await _ai_rate_limiter.acquire()
        raw  = await loop.run_in_executor(None, lambda: _gemini_call(prompt))
        if not raw:
            return None
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if not m:
            return None
        obj = json.loads(m.group(0))
        if not obj.get("significant"):
            return None

        direction = obj.get("direction", "")
        if direction not in ("bullish", "bearish", "neutral"):
            return None

        strength = max(1, min(10, int(obj.get("strength", 5))))
        days     = max(1, min(7, float(obj.get("days_relevant", SECTOR_MODIFIER_DEFAULT_DAYS))))
        reason   = str(obj.get("reason_ru", ""))[:200]

        return {
            "sectors": sectors, "direction": direction,
            "strength": strength, "days": days, "reason": reason,
        }
    except Exception as e:
        logger.debug(f"classify_geopolitical_impact error: {e}")
        return None

def get_sector_modifier_for_ticker(sector: str) -> dict | None:
    """Возвращает активный модификатор для сектора тикера, если есть."""
    modifiers = _load_sector_modifiers()
    return modifiers.get(sector)

def check_signal_against_sector_modifier(sector: str, tech_signal: str) -> dict:
    """
    Сравнивает технический сигнал с активным секторальным модификатором.

    Возвращает:
        conflict: True если сигнал идёт против направления модификатора
        warning_ru: понятное предупреждение на русском для вставки в
                    filter_status/текст анализа
    """
    mod = get_sector_modifier_for_ticker(sector)
    if not mod:
        return {"conflict": False, "warning_ru": ""}

    is_long  = "LONG" in tech_signal
    is_short = "SHORT" in tech_signal or "ВЫХОД" in tech_signal
    if not (is_long or is_short):
        return {"conflict": False, "warning_ru": ""}

    direction = mod["direction"]
    conflict = (
        (is_short and direction == "bullish") or
        (is_long and direction == "bearish")
    )

    if not conflict:
        return {"conflict": False, "warning_ru": ""}

    direction_ru = "бычий" if direction == "bullish" else "медвежий"
    days_left = round((mod["expires_at"] - time.time()) / 86400, 1)
    warning = (
        f"⚠️ Сектор под {direction_ru}им геополитическим фактором "
        f"(сила {mod['strength']}/10, ещё ~{days_left} дн.): {mod['reason']}. "
        f"Технический сигнал против этого фактора - повышенный риск."
    )
    return {"conflict": True, "warning_ru": warning}

async def run_geopolitical_scan():
    """
    Фоновый цикл - раз в 30 минут проверяет свежие commodity/macro/секторные
    новости по ВСЕМ секторам и прогоняет значимые через AI-классификатор,
    обновляя секторальные модификаторы. Не привязан к конкретному тикеру -
    сканирует сектора целиком, независимо от того, какие тикеры сейчас
    анализируются.

    Три источника новостей, каждый под свою группу секторов:
    - fetch_commodity_news: нефтегаз, металлы, горнодобыча, золото, уголь,
      химия - через англоязычные commodity-издания (OilPrice, Mining.com).
    - fetch_sector_news: банки, финансы, IT, телеком, ритейл, e-commerce,
      медицина, транспорт, девелопмент - через русскоязычные источники
      (Interfax, TASS, Kommersant), так как эти сектора зависят от
      регуляторики ЦБ, санкций, потребительского рынка, а не мировых
      сырьевых цен.
    - fetch_macro_news: общерыночные факторы (ставка ЦБ, курс рубля,
      общие санкции) - могут задеть любой сектор, AI сам определяет какой.
    """
    while True:
        try:
            if not get_ai_enabled():
                await asyncio.sleep(1800)
                continue
            for sector in COMMODITY_SECTORS:
                try:
                    news = await fetch_commodity_news(sector)
                except Exception as e:
                    logger.debug(f"run_geopolitical_scan fetch {sector}: {e}")
                    continue

                for item in news[:2]:  
                    if item.get("age_min", 999) > 60:  
                        continue
                    try:
                        classification = await classify_geopolitical_impact(item["title"])
                    except Exception as e:
                        logger.debug(f"classify_geopolitical_impact: {e}")
                        continue
                    if not classification:
                        continue
                    for sec in classification["sectors"]:
                        _set_sector_modifier(
                            sec, classification["direction"],
                            classification["strength"], classification["reason"],
                            classification["days"]
                        )

            for sector in SECTOR_NEWS_SECTORS:
                try:
                    news = await fetch_sector_news(sector)
                except Exception as e:
                    logger.debug(f"run_geopolitical_scan fetch_sector_news {sector}: {e}")
                    continue

                for item in news[:2]:
                    if item.get("age_min", 999) > 60:
                        continue
                    try:
                        classification = await classify_geopolitical_impact(item["title"])
                    except Exception as e:
                        logger.debug(f"classify_geopolitical_impact: {e}")
                        continue
                    if not classification:
                        continue
                    for sec in classification["sectors"]:
                        _set_sector_modifier(
                            sec, classification["direction"],
                            classification["strength"], classification["reason"],
                            classification["days"]
                        )

            try:
                macro_news = await fetch_macro_news()
                for item in macro_news[:2]:
                    if item.get("age_min", 999) > 60:
                        continue
                    classification = await classify_geopolitical_impact(item["title"])
                    if not classification:
                        continue
                    for sec in classification["sectors"]:
                        _set_sector_modifier(
                            sec, classification["direction"],
                            classification["strength"], classification["reason"],
                            classification["days"]
                        )
            except Exception as e:
                logger.debug(f"run_geopolitical_scan macro: {e}")

        except Exception as e:
            logger.warning(f"run_geopolitical_scan error: {e}")

        await asyncio.sleep(1800)  


# === AI Response Cache ===
# Sokrashchaet kolichestvo AI-vyzovov, keshiruya otvety po hashu prompta.
_AI_CACHE: dict[int, tuple[str, float]] = {}  # hash -> (response, timestamp)
_AI_CACHE_TTL = 1800  # 30 minut

def _ai_cache_get(prompt: str) -> str | None:
    """Vozvrashaet keshirovannyy otvet ili None."""
    h = hash(prompt)
    entry = _AI_CACHE.get(h)
    if entry and (time.time() - entry[1]) < _AI_CACHE_TTL:
        return entry[0]
    return None

def _ai_cache_set(prompt: str, response: str):
    """Sohranyaet otvet v kesh."""
    h = hash(prompt)
    _AI_CACHE[h] = (response, time.time())
    # Ochistka staryh zapisey (kazhdyy 50-y set)
    if len(_AI_CACHE) > 500:
        now = time.time()
        expired = [k for k, v in _AI_CACHE.items() if (now - v[1]) > _AI_CACHE_TTL * 2]
        for k in expired:
            del _AI_CACHE[k]


def _groq_call(prompt: str, model_idx: int = 0) -> str:
    """Groq fallback - ispolzuetsya esli Gemini nedostupen ili vernul pustoy otvet."""
    if not get_ai_enabled():
        return ""
    if not groq_client:
        return _openrouter_call(prompt)
    # Proverka na ischerpanie sutochnoy kvoty
    if getattr(_groq_call, "_daily_quota_exhausted", False):
        elapsed = time.time() - _groq_call._daily_quota_exhausted
        if elapsed < 21600:
            logger.warning(f"Groq daily quota exhausted, skip for {21600 - elapsed:.0f}s")
            return _openrouter_call(prompt)
        _groq_call._daily_quota_exhausted = False

    for i in range(model_idx, len(GROQ_MODELS)):
        for attempt in range(3):
            try:
                resp = groq_client.chat.completions.create(
                    model=GROQ_MODELS[i],
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=400, temperature=0.05,
                )
                result = resp.choices[0].message.content.strip()
                _ai_cache_set(prompt, result)
                return result
            except Exception as e:
                err = str(e)
                if "rate_limit" in err.lower() or "429" in err or "too many" in err.lower():
                    jitter = __import__("random").uniform(0, 1)
                    wait = 2 ** attempt + jitter
                    logger.warning(f"Groq rate limit {GROQ_MODELS[i]}, retry {attempt+1} in {wait:.1f}s")
                    time.sleep(wait)
                elif "quota" in err.lower() or "exhausted" in err.lower() or "limit reached" in err.lower():
                    logger.warning(f"Groq daily quota exhausted, backing off 1h")
                    _groq_call._daily_quota_exhausted = time.time()
                    break
                else:
                    logger.warning(f"Groq {GROQ_MODELS[i]}: {e}")
                    break
    result = _openrouter_call(prompt)
    if result:
        return result
    return ""
def _gemini_call(prompt: str) -> str:
    """Gemini primary (google-genai SDK) -> Groq fallback pri oshibke ili pustom otvete."""
    if not get_ai_enabled():
        logger.debug("AI disabled, skipping _gemini_call")
        return ""
    # Proverka kesha
    cached = _ai_cache_get(prompt)
    if cached:
        return cached
    # Proverka na ischerpanie sutochnoy kvoty
    if getattr(_gemini_call, "_daily_quota_exhausted", False):
        elapsed = time.time() - _gemini_call._daily_quota_exhausted
        if elapsed < 21600:
            logger.warning(f"Gemini daily quota exhausted, switching to Groq")
            return _groq_call(prompt)
        _gemini_call._daily_quota_exhausted = False

    if _gemini_client and GEMINI_MODEL:
        for attempt in range(3):
            try:
                response = _gemini_client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=genai_types.GenerateContentConfig(
                        temperature=0.05,
                        max_output_tokens=400,
                    ) if genai_types else None,
                )
                result = response.text.strip() if response.text else ""
                if result:
                    _ai_cache_set(prompt, result)
                    return result
                logger.warning(f"Gemini try {attempt+1}: pustoy otvet")
            except Exception as e:
                err = str(e)
                if "429" in err.lower() or "quota" in err.lower() or "rate" in err.lower() or "too many" in err.lower():
                    jitter = __import__("random").uniform(0, 1)
                    wait = 2 ** attempt + jitter
                    logger.warning(f"Gemini rate limit, attempt {attempt+1}, retry in {wait:.1f}s")
                    time.sleep(wait)
                elif "exhausted" in err.lower() or "resource_exhausted" in err.lower():
                    logger.warning(f"Gemini quota exhausted, switching to Groq")
                    _gemini_call._daily_quota_exhausted = time.time()
                    break
                else:
                    logger.warning(f"Gemini try {attempt+1} failed: {e}")
                    break

    logger.info("AI: pereklyuchayus na Groq fallback")
    return _groq_call(prompt)

def _openrouter_call(prompt: str) -> str:
    """OpenRouter fallback - ispolzuetsya esli Gemini i Groq nedostupny."""
    if not get_ai_enabled():
        return ""
    if not openrouter_client:
        return ""
    if getattr(_openrouter_call, "_daily_quota_exhausted", False):
        elapsed = time.time() - _openrouter_call._daily_quota_exhausted
        if elapsed < 21600:
            logger.warning(f"OpenRouter daily quota exhausted, skip for {21600 - elapsed:.0f}s")
            return ""
        _openrouter_call._daily_quota_exhausted = False

    OPENROUTER_MODELS = [
        "meta-llama/llama-3.3-70b-instruct",
        "mistralai/mixtral-8x7b-instruct",
        "google/gemma-2-9b-it",
        "microsoft/phi-3-mini-4k-instruct",
    ]
    for model in OPENROUTER_MODELS:
        for attempt in range(3):
            try:
                resp = openrouter_client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    json={"model": model, "messages": [{"role": "user", "content": prompt}], "max_tokens": 400, "temperature": 0.05},
                    timeout=30,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    result = data["choices"][0]["message"]["content"].strip()
                    if result:
                        _ai_cache_set(prompt, result)
                        return result
                elif resp.status_code == 429:
                    jitter = __import__("random").uniform(0, 1)
                    wait = 2 ** attempt + jitter
                    logger.warning(f"OpenRouter rate limit {model}, retry {attempt+1} in {wait:.1f}s")
                    time.sleep(wait)
                elif "quota" in resp.text.lower() or "exhausted" in resp.text.lower():
                    logger.warning(f"OpenRouter quota exhausted, backing off")
                    _openrouter_call._daily_quota_exhausted = time.time()
                    break
                else:
                    logger.warning(f"OpenRouter {model}: HTTP {resp.status_code}")
                    break
            except Exception as e:
                logger.warning(f"OpenRouter {model}: {e}")
                break
    return ""

def _score_facts(news_items: list[dict]) -> tuple[int, list[str]]:
    """
    Считает суммарный вес фактов.
    Группирует новости по категории события (event), исключая дублирование веса 
    от публикации одной новости в нескольких СМИ.
    """
    total = 0
    events = []
    grouped_events: dict[str, dict] = {}
    
    for it in news_items:
        if it.get("is_opinion"):
            continue
        
        w = it.get("effective_weight", it.get("weight", 0))
        if w == 0:
            continue
            
        ev_type = it.get("event", "новость")
        # Берем только максимальный вес для каждого типа события
        if ev_type not in grouped_events or abs(w) > abs(grouped_events[ev_type].get("weight", 0)):
            grouped_events[ev_type] = {
                "weight": w,
                "age_min": it.get("age_min", 999),
                "item": it
            }
    
    for ev_type, data in grouped_events.items():
        w = data["weight"]
        age = data["age_min"]
        age_tag = f", {age:.0f}м назад" if age < NEWS_MAX_AGE_MINUTES else " (устарела)"
        total += w
        events.append(f"{ev_type} ({'+' if w > 0 else ''}{w}{age_tag})")
    
    return total, events

def _load_ai_memory() -> dict[str, dict]:
    raw = _rget(RK_AI_MEMORY)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return {}

def _save_ai_memory(mem: dict) -> None:
    _rset(RK_AI_MEMORY, json.dumps(mem, ensure_ascii=False))

def get_ai_category_multiplier(event_type: str) -> float:
    """Адаптивный множитель веса новости на основе точности ИИ в прошлом."""
    mem = _load_ai_memory()
    stats = mem.get(event_type)
    if not stats or stats.get("total", 0) < 5:
        return 1.0
    
    accuracy = stats.get("hits", 0) / stats["total"]
    if accuracy >= 0.75:
        return 1.25  # Повышаем доверие
    elif accuracy < 0.50:
        return 0.50  # Понижаем доверие, т.к. ИИ часто ошибался
    return 1.0

def update_ai_memory_stat(event_type: str, is_hit: bool) -> None:
    if not event_type or event_type == "нет событий":
        return
    mem = _load_ai_memory()
    stats = mem.setdefault(event_type, {"hits": 0, "total": 0})
    stats["total"] += 1
    if is_hit:
        stats["hits"] += 1
    _save_ai_memory(mem)

async def ai_check_relevance(title: str, ticker: str, company_name: str) -> bool:
    if not get_ai_enabled():
        return False
    if not gemini_model:
        return False

    prompt = (
        f'Заголовок новости: "{title}"\n'
        f'Компания: {company_name} (тикер {ticker}), Россия, фондовый рынок MOEX.\n'
        f'Эта новость напрямую касается именно этой компании или прямо влияет на неё? '
        f'Ответь одним словом: да или нет.'
    )
    try:
        loop = asyncio.get_event_loop()
        await _ai_rate_limiter.acquire()
        raw = await loop.run_in_executor(
            None, lambda: _gemini_call(prompt)
        )
        answer = raw.strip().lower()
        return answer.startswith("да")
    except Exception as e:
        logger.debug(f"ai_check_relevance error: {e}")
        return False

async def ai_evaluate_news(news_items: list[dict], ticker: str, sector: str,
                           tech_signal: str, tech_score: int,
                           price_anomaly: dict = None,
                           edisclosure_items: list = None,
                           htf_trend: dict = None) -> dict:
    """
    Новая логика: AI объясняет связь между движением цены и найденными фактами.
    Не "оцени новость" а "есть ли объяснение этому движению".

    Приоритет источников:
    1. e-disclosure (самые свежие корпоративные факты)
    2. RSS keyword-факты (если e-disclosure пустой)
    3. Если нет ни того ни другого - "чисто техническое движение"
    """
    if not get_ai_enabled():
        return {"summary": "", "ai_label": "", "ai_weight": 0}
    is_long  = "LONG" in tech_signal
    is_short = "SHORT" in tech_signal or "ВЫХОД" in tech_signal
    has_signal = is_long or is_short

    all_facts = []
    if edisclosure_items:
        all_facts.extend([it for it in edisclosure_items
                         if it.get("age_min", 999) <= NEWS_MAX_AGE_MINUTES])
    rss_facts = [it for it in (news_items or [])
                 if it.get("is_fact") and not it.get("is_opinion")
                 and it.get("age_min", 999) <= NEWS_MAX_AGE_MINUTES]
    all_facts.extend(rss_facts)
    # Сортируем строго по важности (абсолютному весу) перед срезом [:5]
    all_facts.sort(
        key=lambda x: (-abs(x.get("effective_weight", x.get("weight", 0))), x.get("age_min", 999))
    )
    fact_weight, fact_events = _score_facts(all_facts)

    blocking_found = [it["event"] for it in all_facts
                      if it.get("effective_weight", it.get("weight", 0)) <= -8]

    event_type  = fact_events[0].split(" (")[0] if fact_events else "нет событий"
    # ИСПРАВЛЕНИЕ: Автоматически применяем множитель точности из памяти ИИ!
    multiplier = get_ai_category_multiplier(event_type)
    fact_weight = int(round(fact_weight * multiplier))
    event_weight = fact_weight
    llm_summary  = ""
    movement_explained = False
    ai_skip_reason = ""

    _settings = get_bot_settings()
    ai_trigger_threshold = _settings["fact_weight_ai_trigger"]
    should_call_ai = (
        gemini_model and all_facts and (
            price_anomaly is not None or
            bool(edisclosure_items) or
            abs(fact_weight) >= ai_trigger_threshold
        )
    )

    if not should_call_ai:
        if not gemini_model:
            ai_skip_reason = "no_gemini_client"
        elif not all_facts:
            ai_skip_reason = "no_facts"
        else:
            ai_skip_reason = (f"weak_signal(fact_weight={fact_weight}, "
                              f"threshold={ai_trigger_threshold}, no_anomaly, no_edisclosure)")

    if should_call_ai:
        company_name = MOEX_STOCKS.get(ticker.upper(), ("", ticker, ""))[1]

        movement_ctx = ""
        if price_anomaly:
            movement_ctx = (
                f"\nЦЕНОВОЕ ДВИЖЕНИЕ: {price_anomaly['description']} "
                f"(объём x{price_anomaly['vol_ratio']} от нормы, срочность: {price_anomaly['urgency']})"
            )

        facts_text = "\n".join(
            f"- [{it.get('source','rss')}, {it.get('age_min',0):.0f}мин назад] {it['title']}"
            for it in all_facts[:5]
        )

        prompt = f"""Ты - квалифицированный аналитик российского фондового рынка.

Компания: {company_name} ({ticker}), сектор: {sector}{movement_ctx}

Свежие факты по компании:
{facts_text}

Ответь на три вопроса строго в формате JSON:
1. Объясняют ли эти факты ценовое движение?
2. Насколько сильное влияние на цену (вес от -10 до +10)?
3. Одно предложение объяснения для трейдера (без рекомендаций купить/продать).

Формат ответа:
{{"explains_movement": true/false, "weight": число, "summary": "одно предложение"}}"""

        loop = asyncio.get_event_loop()
        await _ai_rate_limiter.acquire()
        raw  = await loop.run_in_executor(
            None, lambda: _gemini_call(prompt)
        )
        if not raw:
            ai_skip_reason = "empty_response(gemini_and_groq_both_failed)"
        else:
            try:
                m   = re.search(r'\{.*\}', raw, re.DOTALL)
                obj = json.loads(m.group(0)) if m else {}
                movement_explained = bool(obj.get("explains_movement", False))
                llm_w        = int(obj.get("weight", fact_weight))
                event_weight = max(-10, min(10, (fact_weight + llm_w) // 2))
                llm_summary  = obj.get("summary", "")
                if not llm_summary:
                    ai_skip_reason = "parsed_but_no_summary_field"
            except Exception as parse_err:
                ai_skip_reason = f"json_parse_failed({str(parse_err)[:50]})"

    final_weight = event_weight

    htf_direction = (htf_trend or {}).get("trend", "")
    is_htf_contrarian = (
        (is_long and htf_direction == "bear") or
        (is_short and htf_direction == "bull")
    )

    min_score_required = get_bot_settings()["min_tech_score_confirmed"]
    is_tech_too_weak = tech_score < min_score_required

    if blocking_found and is_long:
        filter_status = "BLOCKED"
    elif not has_signal and abs(final_weight) >= 8:
        filter_status = "NEWS_ONLY"
    elif not has_signal:
        filter_status = "NO_SIGNAL"
    elif final_weight >= 3:
        filter_status = "WEAK" if (is_htf_contrarian or is_tech_too_weak) else "CONFIRMED"
    elif final_weight >= -2:
        filter_status = "WEAK" if (is_htf_contrarian or is_tech_too_weak) else "CONFIRMED"
    elif final_weight >= -5:
        filter_status = "WEAK"
    else:
        filter_status = "BLOCKED"

    if tech_signal == "🟩 LONG":
        status_map = {
            "CONFIRMED": "🟩 LONG CONFIRMED", "WEAK":     "🟡 LONG WEAK",
            "WATCH":     "👀 LONG WATCH",      "BLOCKED":  "🚫 LONG BLOCKED",
            "NO_SIGNAL": "🟩 LONG",            "NEWS_ONLY":"🟩 LONG (сильный фон)",
        }
    elif is_short:
        status_map = {
            "CONFIRMED": "🟥 ВЫХОД CONFIRMED", "WEAK":     "🟡 ВЫХОД WEAK",
            "WATCH":     "👀 ВЫХОД WATCH",      "BLOCKED":  "🟥 ВЫХОД (сдерживающий позитив)",
            "NO_SIGNAL": "🟥 ВЫХОД",            "NEWS_ONLY":"🟥 ВЫХОД",
        }
    else:
        status_map = {k: "НЕТ СИГНАЛА" for k in
                      ["CONFIRMED", "WEAK", "WATCH", "BLOCKED", "NO_SIGNAL", "NEWS_ONLY"]}

    confirmed = status_map.get(filter_status, tech_signal)

    return {
        "event_type":          event_type,
        "event_weight":        final_weight,
        "fact_events":         fact_events[:3],
        "filter_status":       filter_status,
        "summary":             llm_summary,
        "blocking":            blocking_found,
        "movement_explained":  movement_explained,
        "confirmed":           confirmed,
        "opinions_skipped":    len([it for it in (news_items or []) if it.get("is_opinion")]),
        "sentiment":           "позитив" if final_weight > 2 else ("негатив" if final_weight < -2 else "нейтрально"),
        "score":               min(10, max(0, abs(final_weight))),
        "has_edisclosure":     bool(edisclosure_items),
        "edisclosure_count":   len(edisclosure_items or []),
        "ai_skip_reason":      ai_skip_reason,
    }

async def ai_classify_news_impact(headline: str, ticker: str) -> dict:
    if not get_ai_enabled():
        return {"event": "", "weight": 0, "is_corporate": False}
    text = headline.lower()
    weight = 0
    event  = ""
    for pattern, event_label, w, is_corp in FACT_PATTERNS:
        if len(pattern) <= 4 and re.fullmatch(r'[a-z0-9]+', pattern):
            if re.search(r'\b' + re.escape(pattern) + r'\b', text):
                weight = w
                event  = event_label
                break
        elif pattern in text:
            weight = w
            event  = event_label
            break
    if gemini_model and weight == 0:
        company_name = MOEX_STOCKS.get(ticker.upper(), ("", ticker, ""))[1]
        prompt = (f'Новость для {ticker} ({company_name}): "{headline}"\n'
                  f'Определи влияние. Ответь строго JSON: {{"event": "описание", "weight": число от -10 до 10}}')
        loop = asyncio.get_event_loop()
        await _ai_rate_limiter.acquire()
        raw  = await loop.run_in_executor(
            None, lambda: _gemini_call(prompt)
        )
        try:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            obj = json.loads(m.group(0)) if m else {}
            weight = int(obj.get("weight", 0))
            event  = obj.get("event", event)
        except Exception:
            pass
    sentiment = "позитив" if weight > 2 else ("негатив" if weight < -2 else "нейтрально")
    return {"sentiment": sentiment, "score": min(10, abs(weight)),
            "asset": ticker, "weight": weight, "event": event}

async def ai_trade_review(
    ticker: str, direction: str, entry: float,
    sl: float, tp1: float, tp2: float, tp3: float,
    tech_score: int, filter_status: str,
    news_items: list[dict], meta: dict,
) -> str:
    """
    AI мини-ревью сделки в момент входа.
    Анализирует: индикаторы, уровни, новости, R/R - даёт короткое заключение.
    Возвращает строку для вставки в сообщение 'Сделка открыта'.
    """
    if not gemini_model and not groq_client:
        return ""

    is_long   = direction == "LONG"
    risk      = abs(entry - sl)
    rr2       = round(abs(tp2 - entry) / risk, 1) if risk > 0 else 0
    company   = MOEX_STOCKS.get(ticker.upper(), ("", ticker, ""))[1]
    direction_ru = "LONG (покупка)" if is_long else "SHORT (продажа)"

    news_lines = ""
    if news_items:
        top = news_items[:3]
        news_lines = "\n".join(f"- {it['title'][:100]}" for it in top)
    else:
        news_lines = "нет свежих новостей"

    mtf = meta.get("mtf_trends", {})
    mtf_str = ""
    if mtf:
        mtf_str = f"15m: {mtf.get('15m','?')} | 1h: {mtf.get('1h','?')} | 4h: {mtf.get('4h','?')}"

    htf_trend    = meta.get("htf_trend", "")
    imoex_regime = meta.get("imoex_regime", "")
    context_lines = []
    if mtf_str:
        context_lines.append(f"MTF тренды: {mtf_str}")
    if htf_trend:
        context_lines.append(f"Старший таймфрейм (HTF): {htf_trend}")
    if imoex_regime:
        context_lines.append(f"Режим рынка IMOEX: {imoex_regime}")
    context_str = "\n".join(context_lines) if context_lines else "нет данных о трендах"

    prompt = f"""Ты торговый ассистент. Трейдер только что открыл сделку на Московской бирже.

СДЕЛКА:
Тикер: {ticker} ({company})
Направление: {direction_ru}
Вход: {entry:.2f} ₽
SL: {sl:.2f} ₽ (риск {abs(entry-sl)/entry*100:.2f}%)
TP1: {tp1:.2f} | TP2: {tp2:.2f} | TP3: {tp3:.2f}
R/R к TP2: 1:{rr2}

ТЕХНИЧЕСКИЙ КОНТЕКСТ:
Оценка сигнала: {tech_score}/100
Фильтр: {filter_status}
{context_str}

НОВОСТИ И КОНТЕКСТ РЫНКА (компания, сектор, макро):
{news_lines}

Дай короткое заключение (3-4 предложения максимум):
1. Подтверждают ли новости и тренд направление сделки?
2. Главный риск этой сделки прямо сейчас.
3. На что обратить внимание - TP1 или выход если цена пойдёт против.

Отвечай на русском, коротко и конкретно. Без воды. Если данных по трендам или
новостям реально нет - так и скажи, не выдумывай."""

    try:
        loop = asyncio.get_event_loop()
        await _ai_rate_limiter.acquire()
        raw  = await loop.run_in_executor(None, lambda: _gemini_call(prompt))
        if raw and len(raw) > 20:
            return raw.strip()
    except Exception as e:
        logger.debug(f"ai_trade_review error: {e}")
    return ""

def _safe_vol_ratio(val, default: float = 1.0) -> float:
    """Безопасно конвертирует vol_ratio: NaN/None/0 → default.
    Обычный `x or default` НЕ работает для NaN (bool(NaN) == True)."""
    try:
        f = float(val)
        if pd.isna(f) or f <= 0:
            return default
        return f
    except (TypeError, ValueError):
        return default

def calculate_indicators(df: pd.DataFrame, tf: str = "15m") -> pd.DataFrame:
    df = df.copy()
    close = df["close"]
    is_intraday = tf in INTRADAY_TFS

    if is_intraday:
        df["ema9"] = ta.ema(close, length=9)
        df["ema20"] = ta.ema(close, length=20)
        df["ema50"] = ta.ema(close, length=50)
        df["ema200"] = ta.ema(close, length=200) if len(df) >= 200 else np.nan
    else:
        df["ema20"] = ta.ema(close, length=20)
        df["ema50"] = ta.ema(close, length=50)
        df["ema200"] = ta.ema(close, length=200)

    rsi_len = 7 if tf == "5m" else (9 if tf == "15m" else 14)
    df["rsi"] = ta.rsi(close, length=rsi_len)

    if is_intraday:
        macd = ta.macd(close, fast=8, slow=17, signal=9)
        macd_key, macd_s, macd_h = "MACD_8_17_9", "MACDs_8_17_9", "MACDh_8_17_9"
    else:
        macd = ta.macd(close, fast=12, slow=26, signal=9)
        macd_key, macd_s, macd_h = "MACD_12_26_9", "MACDs_12_26_9", "MACDh_12_26_9"
    if macd is not None:
        df["macd"] = macd.get(macd_key, np.nan)
        df["macd_signal"] = macd.get(macd_s, np.nan)
        df["macd_hist"] = macd.get(macd_h, np.nan)

    atr_len = 7 if is_intraday else 14
    df["atr"] = ta.atr(df["high"], df["low"], close, length=atr_len)

    bb = ta.bbands(close, length=20, std=2)
    if bb is not None:
        df["bb_upper"] = bb.get("BBU_20_2.0", np.nan)
        df["bb_lower"] = bb.get("BBL_20_2.0", np.nan)
        df["bb_mid"] = bb.get("BBM_20_2.0", np.nan)

    df["vol_ma20"] = ta.sma(df["volume"], length=20)
    df["vol_ratio"] = df["volume"] / df["vol_ma20"].replace(0, np.nan)

    if is_intraday and "volume" in df.columns:
        try:
            ts_col = pd.to_datetime(df["timestamp"])
            if ts_col.dt.tz is None:
                ts_col = ts_col.dt.tz_localize("UTC")
            df["timestamp_msk"] = ts_col.dt.tz_convert("Europe/Moscow")
        except Exception:
            df["timestamp_msk"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Europe/Moscow")
        df["date_msk"] = df["timestamp_msk"].dt.date
        df["day_open"] = df.groupby("date_msk")["open"].transform("first")
        tp = (df["high"] + df["low"] + df["close"]) / 3
        df["_tp_vol"] = tp * df["volume"]
        cum_vol = df.groupby("date_msk")["volume"].cumsum()
        cum_tpv = df.groupby("date_msk")["_tp_vol"].cumsum()
        df["vwap"] = cum_tpv / cum_vol.replace(0, np.nan)
        df["vwap_dev"] = (close - df["vwap"]) / df["vwap"] * 100
        df = df.drop(columns=["_tp_vol"])

    return df

def get_previous_day_levels(df: pd.DataFrame) -> dict:
    if "timestamp" not in df.columns or len(df) < 50:
        return {}
    try:
        df2 = df.copy()
        df2["ts"] = pd.to_datetime(df2["timestamp"])
        try:
            df2["date"] = df2["ts"].dt.tz_convert("Europe/Moscow").dt.date
        except Exception:
            df2["date"] = df2["ts"].dt.date

        daily = df2.groupby("date").agg(
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            open=("open", "first"),
        ).reset_index().sort_values("date")

        if len(daily) < 2:
            return {}

        prev = daily.iloc[-2]
        today_open = float(daily.iloc[-1]["open"]) if len(daily) >= 1 else None

        pdh = float(prev["high"])
        pdl = float(prev["low"])
        pdc = float(prev["close"])
        pdm = round((pdh + pdl) / 2, 4)
        gap_pct = 0.0
        if today_open:
            gap_pct = round((today_open - pdc) / pdc * 100, 2)

        return {
            "pdh": round(pdh, 4),
            "pdl": round(pdl, 4),
            "pdc": round(pdc, 4),
            "pdm": round(pdm, 4),
            "gap_pct": gap_pct,
            "today_open": round(today_open, 4) if today_open else None,
            "range": round(pdh - pdl, 4),
        }
    except Exception as e:
        logger.debug(f"PDL calc error: {e}")
        return {}

def get_pd_level_context(levels: dict, price: float) -> tuple[str, float]:
    if not levels or not price:
        return "", 0.0
    touch_zone = 0.0015
    for name, val in [("PDH", levels.get("pdh", 0)),
                       ("PDL", levels.get("pdl", 0)),
                       ("PDC", levels.get("pdc", 0)),
                       ("PDM", levels.get("pdm", 0))]:
        if not val:
            continue
        dist_pct = abs(price - val) / val
        if dist_pct <= touch_zone:
            return name, round((price - val) / val * 100, 3)
    return "", 0.0

async def get_tf_trend_state(ticker: str, figi: str, tf: str) -> str:
    """Возвращает строку-метку тренда для указанного таймфрейма через EMA20/EMA50."""
    interval, _, limit = TF_MAP.get(tf, TF_MAP["15m"])
    cache_key = f"tf_trend_{ticker}_{tf}"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 600:
        return _cache[cache_key]["val"]
    try:
        df = await fetch_candles_tinkoff(figi, interval, min(limit, 100))
        if df is None or len(df) < 30:
            return "неизвестно"
        close_s = df["close"]
        ema20 = float(ta.ema(close_s, length=20).iloc[-1] or 0)
        ema50 = float(ta.ema(close_s, length=50).iloc[-1] or 0)
        price = float(close_s.iloc[-1])
        if price > ema20 > ema50:
            label = f"📈 {tf.upper()}: бычий"
        elif price < ema20 < ema50:
            label = f"📉 {tf.upper()}: медвежий"
        else:
            label = f"↔️ {tf.upper()}: боковик"
        _cache[cache_key] = {"val": label, "ts": now}
        return label
    except Exception as e:
        logger.debug(f"tf_trend {ticker} {tf}: {e}")
        return "неизвестно"

async def get_htf_trend(ticker: str, figi: str, base_tf: str) -> dict:
    htf_map = {"5m": "15m", "15m": "1h", "1h": "1d", "4h": "1d"}
    htf = htf_map.get(base_tf, "1h")
    interval, _, limit = TF_MAP.get(htf, TF_MAP["1h"])

    cache_key = f"htf_{ticker}_{htf}"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 900:
        return _cache[cache_key]["val"]

    default = {"trend": "neutral", "label": "❓ HTF неизвестен", "htf": htf,
                "ema20": 0, "ema50": 0, "price": 0}
    try:
        df = await fetch_candles_tinkoff(figi, interval, limit)
        if df is None or len(df) < 30:
            return default

        close_s = df["close"]
        ema20 = float(ta.ema(close_s, length=20).iloc[-1] or 0)
        ema50 = float(ta.ema(close_s, length=50).iloc[-1] or 0)
        price_htf = float(close_s.iloc[-1])

        if price_htf > ema20 > ema50:
            ema50_series = ta.ema(close_s, length=50)
            ema50_vals = ema50_series.dropna()
            slope = (float(ema50_vals.iloc[-1]) - float(ema50_vals.iloc[-10])) / float(ema50_vals.iloc[-10]) * 100 if len(ema50_vals) >= 10 else 0
            trend = "bull" if slope > 0 else "neutral"
            label = f"📈 {htf.upper()}: бычий тренд"
        elif price_htf < ema20 < ema50:
            trend = "bear"
            label = f"📉 {htf.upper()}: медвежий тренд"
        else:
            trend = "neutral"
            label = f"↔️ {htf.upper()}: боковик"

        result = {"trend": trend, "label": label, "htf": htf,
                  "ema20": round(ema20, 4), "ema50": round(ema50, 4), "price": round(price_htf, 4)}
        _cache[cache_key] = {"val": result, "ts": now}
        return result
    except Exception as e:
        logger.debug(f"HTF {ticker} {htf}: {e}")
        return default

def detect_macd_divergence(df: pd.DataFrame) -> str:
    if "macd_hist" not in df.columns or len(df) < 20:
        return ""
    recent = df.tail(30).dropna(subset=["macd_hist"])
    if len(recent) < 15:
        return ""

    macd_vals  = recent["macd_hist"].values
    lows       = recent["low"].values
    highs      = recent["high"].values

    def local_extrema(arr, order=3):
        maxima, minima = [], []
        for i in range(order, len(arr) - order):
            if arr[i] == min(arr[i-order:i+order+1]): minima.append(i)
            if arr[i] == max(arr[i-order:i+order+1]): maxima.append(i)
        return maxima, minima

    _, minima = local_extrema(lows)
    maxima, _ = local_extrema(highs)

    if len(minima) >= 2:
        i1, i2 = minima[-2], minima[-1]
        price_lower  = lows[i2] < lows[i1] * 0.999
        macd_higher  = macd_vals[i2] > macd_vals[i1] + 0.0001
        if price_lower and macd_higher:
            return "🔄 Бычья дивергенция MACD - разворот вверх"

    if len(maxima) >= 2:
        i1, i2 = maxima[-2], maxima[-1]
        price_higher = highs[i2] > highs[i1] * 1.001
        macd_lower   = macd_vals[i2] < macd_vals[i1] - 0.0001
        if price_higher and macd_lower:
            return "🔄 Медвежья дивергенция MACD - разворот вниз"

    return ""

def get_session_phase() -> dict:
    now_utc  = datetime.now(timezone.utc)
    msk_hour = (now_utc.hour + 3) % 24
    msk_min  = now_utc.minute
    msk_time = msk_hour * 60 + msk_min
    weekday  = now_utc.weekday()

    if weekday >= 5:
        return {"phase": "closed", "label": "🔴 Выходной", "trade": False,
                "enter": False, "warning": "Рынок закрыт"}

    phases = [
        (0,          6*60+50,     "pre_open",    "🔴 До открытия",             False, False, "Рынок ещё не открылся"),
        (6*60+50,    7*60,        "morning_auc", "🟡 Аукцион утр. открытия",   False, False, "Аукцион открытия - заявки не исполняются"),
        (7*60,       9*60+49,     "morning_pre", "🟠 Утренняя доп. сессия",    True,  True,  "Низкая ликвидность, только топ-бумаги - вход с осторожностью"),
        (9*60+49,    9*60+50,     "tech_break1", "🔴 Тех. перерыв",            False, False, "Технический перерыв 1 сек"),
        (9*60+50,    10*60,       "opening_auc", "🟡 Аукцион осн. открытия",   False, False, "Аукцион открытия - не входим"),
        (10*60,      13*60+45,    "morning",     "🟢 Утренняя сессия",         True,  True,  ""),
        (13*60+45,   14*60+5,     "clearing1",   "🔴 Клиринг 14:00",           False, False, "Клиринговый перерыв - не входим"),
        (14*60+5,    17*60+45,    "afternoon",   "🟢 Дневная сессия",          True,  True,  ""),
        (17*60+45,   18*60+40,    "closing",     "🟡 Закрытие основной",       True,  False, "Закрывай позиции"),
        (18*60+40,   19*60,       "clearing2",   "🔴 Аукцион закрытия",        False, False, "Аукцион закрытия - не входим"),
        (19*60,      19*60+5,     "eve_open",    "🟡 Открытие вечерней",       False, False, "Аукцион открытия вечерней - не входим"),
        (19*60+5,    23*60+45,    "evening",     "🟢 Вечерняя сессия",         True,  True,  ""),
        (23*60+45,   24*60,       "eod",         "🔴 Закрытие сессии",         False, False, "Закрывай всё до 23:50"),
    ]

    for start, end, phase, label, trade, enter, warning in phases:
        if start <= msk_time < end:
            return {"phase": phase, "label": label, "trade": trade,
                    "enter": enter, "warning": warning,
                    "msk_time": f"{msk_hour:02d}:{msk_min:02d}"}

    return {"phase": "closed", "label": "🔴 Закрыто", "trade": False,
            "enter": False, "warning": ""}

def detect_candle_pattern(df: pd.DataFrame) -> str:
    if len(df) < 2:
        return "-"
    c, p = df.iloc[-1], df.iloc[-2]
    body = abs(c["close"] - c["open"])
    rng = c["high"] - c["low"]
    if rng == 0:
        return "Дожи"
    uw = c["high"] - max(c["close"], c["open"])
    lw = min(c["close"], c["open"]) - c["low"]
    if lw > body * 2 and uw < body * 0.5:
        return "📌 Пин-бар снизу"
    if uw > body * 2 and lw < body * 0.5:
        return "📌 Пин-бар сверху"
    if c["close"] > c["open"] and p["close"] < p["open"] and c["close"] > p["open"] and c["open"] < p["close"]:
        return "🟢 Бычье поглощение"
    if c["close"] < c["open"] and p["close"] > p["open"] and c["close"] < p["open"] and c["open"] > p["close"]:
        return "🔴 Медвежье поглощение"
    return "Обычная свеча"

def detect_market_regime(df: pd.DataFrame) -> dict:
    if len(df) < 40:
        return {"regime": "ranging", "label": "↔️ Боковик", "trend_mult": 1.0, "slope": 0}
    close = df["close"].values
    price = close[-1]
    ema20 = ta.ema(pd.Series(close), length=20).values
    ema50 = ta.ema(pd.Series(close), length=50).values
    ema20_v = ema20[~np.isnan(ema20)]
    ema50_v = ema50[~np.isnan(ema50)]
    if len(ema50_v) < 10:
        return {"regime": "ranging", "label": "↔️ Боковик", "trend_mult": 1.0, "slope": 0}
    slope = (ema50_v[-1] - ema50_v[-10]) / ema50_v[-10] * 100
    if price > ema20_v[-1] > ema50_v[-1] and slope > 0.1:
        return {"regime": "trending_up", "label": "📈 Восходящий тренд", "trend_mult": 1.2, "slope": slope}
    if price < ema20_v[-1] < ema50_v[-1] and slope < -0.1:
        return {"regime": "trending_down", "label": "📉 Нисходящий тренд", "trend_mult": 1.2, "slope": slope}
    return {"regime": "ranging", "label": "↔️ Боковик / Флэт", "trend_mult": 0.8, "slope": slope}

def find_support_resistance(df: pd.DataFrame, price: float, tf: str = "15m"):
    
    depth   = {"5m": 30, "15m": 30, "1h": 60, "4h": 150, "1d": 200}.get(tf, 30)
    n_side  = {"5m": 2,  "15m": 2,  "1h": 3,  "4h": 3,   "1d": 4  }.get(tf, 2)
    r = df.tail(depth).reset_index(drop=True)
    n = len(r)

    highs: dict[float, int] = {}  
    lows:  dict[float, int] = {}

    for i in range(n_side, n - n_side):
        h = float(r.iloc[i]["high"])
        l = float(r.iloc[i]["low"])

        if all(h >= float(r.iloc[i + k]["high"]) for k in range(-n_side, n_side + 1) if k != 0):
            
            touches = sum(
                1 for j in range(n)
                if abs(float(r.iloc[j]["high"]) - h) / h < 0.003
            )
            highs[h] = highs.get(h, 0) + touches

        if all(l <= float(r.iloc[i + k]["low"]) for k in range(-n_side, n_side + 1) if k != 0):
            touches = sum(
                1 for j in range(n)
                if abs(float(r.iloc[j]["low"]) - l) / l < 0.003
            )
            lows[l] = lows.get(l, 0) + touches

    def _cluster(levels: dict) -> list[float]:
        sorted_lvls = sorted(levels.keys())
        clustered = []
        skip = set()
        for i, lvl in enumerate(sorted_lvls):
            if lvl in skip:
                continue
            group = [l for l in sorted_lvls if abs(l - lvl) / max(lvl, 0.001) < 0.005]
            for g in group:
                skip.add(g)
            
            best = max(group, key=lambda x: levels.get(x, 0))
            clustered.append(best)
        return clustered

    all_highs = _cluster(highs)
    all_lows  = _cluster(lows)

    raw_supports    = sorted([l for l in all_lows  if l < price * 0.999], reverse=True)[:6]
    raw_resistances = sorted([h for h in all_highs if h > price * 1.001])[:6]

    flipped_to_resistance: list[float] = []
    flipped_to_support:    list[float] = []

    if len(r) >= 20:
        recent20    = r.tail(20)
        recent_high = float(recent20["high"].max())
        recent_low  = float(recent20["low"].min())

        for lvl in list(raw_supports):
            if recent_high > lvl > price:
                flipped_to_resistance.append(lvl)
                raw_supports.remove(lvl)

        for lvl in list(raw_resistances):
            if recent_low < lvl < price:
                flipped_to_support.append(lvl)
                raw_resistances.remove(lvl)

    supports    = sorted(flipped_to_support    + raw_supports,    key=lambda x: price - x)[:4]
    resistances = sorted(flipped_to_resistance + raw_resistances, key=lambda x: x - price)[:4]

    return supports, resistances

def calculate_sl_tp_stocks(signal: str, price: float, atr: float,
                           supports: list, resistances: list,
                           pd_levels: dict = None, tick_size: float = 0.0) -> dict:
    if signal not in ("🟩 LONG", "🟥 SHORT/ВЫХОД"):
        return {}

    is_long   = "LONG" in signal
    _settings = get_bot_settings()
    
    min_tick  = tick_size if tick_size > 0 else (
        0.0001 if price < 0.01 else
        0.001  if price < 1   else
        0.01   if price < 50  else
        0.1    if price < 500 else
        1.0
    )
    min_risk  = max(price * _settings["sl_min_risk_pct"] / 100, min_tick * 2)
    max_risk  = price * _settings["sl_max_risk_pct"] / 100
    atr_risk  = atr * 1.5

    all_supports    = list(supports or [])
    all_resistances = list(resistances or [])

    if pd_levels:
        for key in ("pdl", "pdm", "pdc", "pdh"):
            val = pd_levels.get(key, 0)
            if not val:
                continue
            if val < price * 0.999:
                all_supports.append(float(val))
            elif val > price * 1.001:
                all_resistances.append(float(val))

    all_supports    = sorted(set(round(s, 4) for s in all_supports if s > 0), reverse=True)
    all_resistances = sorted(set(round(r, 4) for r in all_resistances if r > 0))

    def _round_price(p):
        
        if tick_size and tick_size > 0:
            return round(round(p / tick_size) * tick_size, 6)
        if price > 1000:  return round(p, 1)
        if price > 100:   return round(p, 2)
        if price > 10:    return round(p, 3)
        return round(p, 4)

    if is_long:
        sl_level = None
        for sup in all_supports:
            dist = price - sup
            if min_risk <= dist <= max_risk:
                sl_level = sup
                break

        if sl_level:
            sl = _round_price(sl_level - sl_level * 0.001)
            sl_source = "за уровень поддержки"
        elif atr_risk <= max_risk:
            sl = _round_price(price - max(atr_risk, min_risk))
            sl_source = "ATR×1.5"
        else:
            sl = _round_price(price - min_risk)
            sl_source = "мин. риск"

        actual_risk = price - sl
        risk_pct    = actual_risk / price * 100

        if sl >= price or actual_risk < min_tick:
            sl          = _round_price(price - min_risk)
            actual_risk = price - sl
            risk_pct    = actual_risk / price * 100
            sl_source   = "мин. риск (скорректировано)"

        tp_candidates = [r for r in all_resistances if r > price + actual_risk * 0.5]

        if len(tp_candidates) >= 3:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(tp_candidates[1])
            tp3 = _round_price(tp_candidates[2] if len(tp_candidates) > 2
                               else price + actual_risk * 4)
        elif len(tp_candidates) == 2:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(tp_candidates[1])
            tp3 = _round_price(price + actual_risk * 4)
        elif len(tp_candidates) == 1:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(price + actual_risk * 2.5)
            tp3 = _round_price(price + actual_risk * 4)
        else:
            tp1 = _round_price(price + actual_risk * 1.5)
            tp2 = _round_price(price + actual_risk * 2.5)
            tp3 = _round_price(price + actual_risk * 4.0)

    else:
        sl_level = None
        for res in all_resistances:
            dist = res - price
            if min_risk <= dist <= max_risk:
                sl_level = res
                break

        if sl_level:
            sl = _round_price(sl_level + sl_level * 0.001)
            sl_source = "за уровень сопротивления"
        elif atr_risk <= max_risk:
            sl = _round_price(price + max(atr_risk, min_risk))
            sl_source = "ATR×1.5"
        else:
            sl = _round_price(price + min_risk)
            sl_source = "мин. риск"

        actual_risk = sl - price
        risk_pct    = actual_risk / price * 100

        if sl <= price or actual_risk < min_tick:
            sl          = _round_price(price + min_risk)
            actual_risk = sl - price
            risk_pct    = actual_risk / price * 100
            sl_source   = "мин. риск (скорректировано)"

        tp_candidates = [s for s in all_supports if s < price - actual_risk * 0.5]

        if len(tp_candidates) >= 3:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(tp_candidates[1])
            tp3 = _round_price(tp_candidates[2] if len(tp_candidates) > 2
                               else price - actual_risk * 4)
        elif len(tp_candidates) == 2:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(tp_candidates[1])
            tp3 = _round_price(price - actual_risk * 4)
        elif len(tp_candidates) == 1:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(price - actual_risk * 2.5)
            tp3 = _round_price(price - actual_risk * 4)
        else:
            tp1 = _round_price(price - actual_risk * 1.5)
            tp2 = _round_price(price - actual_risk * 2.5)
            tp3 = _round_price(price - actual_risk * 4.0)

    rr = round(abs(tp2 - price) / max(abs(price - sl), 0.001), 2)

    warnings = []
    if rr < 1.5:
        warnings.append(f"⚠️ R/R низкий ({rr:.1f}) - рассмотри пропустить")
    if risk_pct > 2.5:
        warnings.append(f"⚠️ Большой риск {risk_pct:.1f}% - уменьши лот")

    return {
        "sl":        sl,
        "tp1":       tp1,
        "tp2":       tp2,
        "tp3":       tp3,
        "risk_pct":  round(risk_pct, 2),
        "rr_ratio":  rr,
        "sl_source": sl_source,
        "warn":      " | ".join(warnings),
    }

def compute_tech_score(df: pd.DataFrame, mode_cfg: dict,
                       imoex_regime: dict = None,
                       htf_trend: dict = None, pd_levels: dict = None,
                       macd_div: str = "") -> tuple[str, int, list]:
    row = df.iloc[-1]
    rsi = float(row.get("rsi", 50) or 50)
    macd_h = float(row.get("macd_hist", 0) or 0)
    close = float(row["close"])
    vol_r = _safe_vol_ratio(row.get("vol_ratio"))
    regime = detect_market_regime(df)
    candle = detect_candle_pattern(df)

    long_gates,  short_gates  = 0, 0
    long_r,      short_r      = [], []
    vwap     = float(row.get("vwap",     0) or 0)
    vwap_dev = float(row.get("vwap_dev", 0) or 0)
    day_open = float(row.get("day_open", 0) or 0)
    ema9     = float(row.get("ema9",     0) or 0)
    ema20    = float(row.get("ema20",    0) or 0)
    ema50    = float(row.get("ema50",    0) or 0)
    has_vwap = vwap > 0

    htf_bias = "neutral"
    if htf_trend and htf_trend.get("trend") != "neutral":
        htf_bias = htf_trend["trend"]

    if has_vwap:
        if close > vwap * 1.002:
            long_gates += 1
            long_r.append(f"Выше VWAP (+{vwap_dev:.2f}%)")
        elif close < vwap * 0.998:
            short_gates += 1
            short_r.append(f"Ниже VWAP ({vwap_dev:.2f}%)")
        else:
            return "НЕТ СИГНАЛА", 0, ["Цена в зоне VWAP - нет чёткой позиции"]

    if ema9 > 0 and ema20 > 0:
        if close > ema9 > ema20:
            long_gates += 1
            long_r.append("EMA9 > EMA20 (бычий импульс)")
        elif close < ema9 < ema20:
            short_gates += 1
            short_r.append("EMA9 < EMA20 (медвежий импульс)")
    elif ema20 > 0 and ema50 > 0:
        if close > ema20 > ema50:
            long_gates += 1
            long_r.append("EMA20 > EMA50 (восходящий тренд)")
        elif close < ema20 < ema50:
            short_gates += 1
            short_r.append("EMA20 < EMA50 (нисходящий тренд)")

    min_vol = mode_cfg.get("min_vol_ratio", 1.3)
    if vol_r < min_vol:
        return "НЕТ СИГНАЛА", 0, [f"Низкий объём (x{vol_r:.1f} < x{min_vol:.1f} - нет подтверждения)"]
    max_vol = mode_cfg.get("max_vol_ratio")
    if max_vol and vol_r > max_vol:
        return "НЕТ СИГНАЛА", 0, [f"Аномальный объём (x{vol_r:.1f} > x{max_vol:.1f}) - возможный памп/сквиз, пропускаем"]

    rsi_warning = ""
    rsi_penalty = 0
    if long_gates > short_gates:
        direction = "long"
        if rsi > 78:
            rsi_penalty = 20
            rsi_warning = f"⚠️ RSI высокий ({rsi:.0f}) - возможна коррекция"
        elif rsi > 72:
            rsi_penalty = 10
            rsi_warning = f"RSI повышен ({rsi:.0f}) - следи за откатом"
    elif short_gates > long_gates:
        direction = "short"
        if rsi < 22:
            rsi_penalty = 20
            rsi_warning = f"⚠️ RSI низкий ({rsi:.0f}) - возможен отскок"
        elif rsi < 28:
            rsi_penalty = 10
            rsi_warning = f"RSI понижен ({rsi:.0f}) - следи за отскоком"
    else:
        return "НЕТ СИГНАЛА", 0, ["Противоречивые сигналы - нет чёткого направления"]

    score = 50
    if rsi_penalty:
        score -= rsi_penalty
        if direction == "long":
            long_r.append(rsi_warning)
        else:
            short_r.append(rsi_warning)

    if has_vwap:
        dev = abs(vwap_dev)
        if dev > 1.0:   score += 15
        elif dev > 0.5: score += 8
        elif dev > 0.2: score += 3

    if vol_r > 3.0:   score += 20
    elif vol_r > 2.0: score += 12
    elif vol_r > 1.5: score += 6

    if htf_bias == "bull":
        if direction == "long":
            score += 12
            long_r.append(f"✅ {htf_trend['htf'].upper()} тренд бычий - направление совпадает")
        else:
            score -= 20
            short_r.append(f"⚠️ {htf_trend['htf'].upper()} тренд бычий - SHORT против тренда")
    elif htf_bias == "bear":
        if direction == "short":
            score += 12
            short_r.append(f"✅ {htf_trend['htf'].upper()} тренд медвежий - направление совпадает")
        else:
            score -= 20
            long_r.append(f"⚠️ {htf_trend['htf'].upper()} тренд медвежий - LONG против тренда")

    if pd_levels:
        pdh = pd_levels.get("pdh", 0)
        pdl = pd_levels.get("pdl", 0)
        pdc = pd_levels.get("pdc", 0)
        pdm = pd_levels.get("pdm", 0)
        touch = 0.002
        if direction == "long":
            if pdl and abs(close - pdl) / pdl < touch:
                score += 15
                long_r.append(f"📌 Отбой от PDL ({pdl:,.2f})")
            elif pdc and abs(close - pdc) / pdc < touch and close > pdc:
                score += 10
                long_r.append(f"📌 Выше PDC ({pdc:,.2f})")
            elif pdm and abs(close - pdm) / pdm < touch:
                score += 7
                long_r.append(f"📌 Зона PDM ({pdm:,.2f})")
            if pdh and close > pdh * 1.001:
                score += 8
                long_r.append(f"🚀 Пробой PDH ({pdh:,.2f})")
        else:
            if pdh and abs(close - pdh) / pdh < touch:
                score += 15
                short_r.append(f"📌 Отбой от PDH ({pdh:,.2f})")
            elif pdc and abs(close - pdc) / pdc < touch and close < pdc:
                score += 10
                short_r.append(f"📌 Ниже PDC ({pdc:,.2f})")
            elif pdm and abs(close - pdm) / pdm < touch:
                score += 7
                short_r.append(f"📌 Зона PDM ({pdm:,.2f})")
            if pdl and close < pdl * 0.999:
                score += 8
                short_r.append(f"🔻 Пробой PDL ({pdl:,.2f})")

    if macd_div:
        if direction == "long" and "Бычья" in macd_div:
            score += 18
            long_r.append(macd_div)
        elif direction == "short" and "Медвежья" in macd_div:
            score += 18
            short_r.append(macd_div)
        elif direction == "long" and "Медвежья" in macd_div:
            score -= 10
        elif direction == "short" and "Бычья" in macd_div:
            score -= 10

    if day_open > 0:
        dev_open = (close / day_open - 1) * 100
        if direction == "long" and close > day_open * 1.003:
            score += 8
            long_r.append(f"Выше открытия дня ({dev_open:+.1f}%)")
        elif direction == "short" and close < day_open * 0.997:
            score += 8
            short_r.append(f"Ниже открытия дня ({dev_open:+.1f}%)")

    if direction == "long" and macd_h > 0:
        score += 6; long_r.append("MACD > 0")
    elif direction == "short" and macd_h < 0:
        score += 6; short_r.append("MACD < 0")

    if 40 < rsi < 60:
        score += 4
    elif direction == "long" and 60 < rsi < 70:
        score += 2
    elif direction == "short" and 30 < rsi < 40:
        score += 2

    if direction == "long" and ("Бычье" in candle or "снизу" in candle):
        score += 8; long_r.append(candle)
    elif direction == "short" and ("Медвежье" in candle or "сверху" in candle):
        score += 8; short_r.append(candle)

    if imoex_regime:
        ir = imoex_regime.get("regime", "neutral")
        if ir == "bear" and direction == "long":
            score -= 15
            long_r.append("⚠️ IMOEX медвежий")
        elif ir == "bull" and direction == "long":
            score += 5
        elif ir == "bear" and direction == "short":
            score += 5

    score = min(100, max(0, score))
    min_score = mode_cfg.get("min_score", 70)

    if direction == "long":
        signal = "🟩 LONG" if score >= min_score else "НЕТ СИГНАЛА"
        reasons = long_r
    else:
        signal = "🟥 SHORT/ВЫХОД" if score >= min_score else "НЕТ СИГНАЛА"
        reasons = short_r

    return signal, score, reasons

def calculate_daily_poc(df_daily) -> float | None:
    """
    Считает HVN (High Volume Node) - ценовой уровень с наибольшим объёмом.
    Использует high/low/volume каждой свечи: объём распределяется равномерно
    между low и high, строится гистограмма по 30 бинам.
    Честнее чем по close-ценам, но не настоящий TPO (нет тиковых данных).
    Результат отображается как 'HVN' а не 'POC'.
    """
    if df_daily is None or not isinstance(df_daily, pd.DataFrame) or len(df_daily) < 10:
        return None
    try:
        highs   = df_daily["high"].values.astype(float)
        lows    = df_daily["low"].values.astype(float)
        volumes = df_daily["volume"].values.astype(float)

        min_p = float(lows.min())
        max_p = float(highs.max())
        if min_p >= max_p:
            return float((min_p + max_p) / 2)

        n_bins  = 30
        bins    = np.linspace(min_p, max_p, n_bins + 1)
        bin_vol = np.zeros(n_bins)

        for hi, lo, vol in zip(highs, lows, volumes):
            if hi <= lo or vol <= 0:
                continue
            
            candle_range = hi - lo
            for j in range(n_bins):
                overlap = min(hi, bins[j + 1]) - max(lo, bins[j])
                if overlap > 0:
                    bin_vol[j] += vol * overlap / candle_range

        max_idx = int(np.argmax(bin_vol))
        hvn = (bins[max_idx] + bins[max_idx + 1]) / 2
        return float(hvn)
    except Exception:
        return None

async def analyze_stock(ticker: str, tf: str = DEFAULT_TF, mode_cfg: dict = None) -> dict | None:
    if mode_cfg is None:
        mode_cfg = TRADE_MODES["mid"]
    ticker = ticker.upper()
    if ticker not in MOEX_STOCKS:
        return {"error": f"Тикер {ticker} не найден в списке MOEX_STOCKS."}

    _, name, sector, *_ = MOEX_STOCKS[ticker]

    news_items = []
    imoex_regime = {"regime": "neutral", "label": "⚪ IMOEX: тренд неопределен"}
    htf_trend = {"trend": "neutral", "label": "❓ HTF неизвестен", "htf": "1h"}
    cal_check = {"block": False, "warning": "", "events": [], "score_penalty": 0}

    try:
        df_result, meta = await fetch_stock_data(ticker, tf)
    except Exception as e:
        return {"error": f"Ошибка загрузки котировок {ticker}: {e}"}

    if df_result is None or len(df_result) < 20:
        
        try:
            interval, _, limit = TF_MAP.get(tf, TF_MAP[DEFAULT_TF])
            figi_fb = MOEX_STOCKS[ticker][0]
            df_fb = await fetch_candles_tinkoff(figi_fb, interval, limit * 2)
            if df_fb is not None and len(df_fb) >= 20:
                df_result = df_fb
                logger.info(f"analyze_stock {ticker}: использован расширенный период (x2)")
            else:
                logger.warning(f"analyze_stock {ticker}: данных нет даже с x2 периодом. "
                               f"Свечей: {len(df_fb) if df_fb is not None else 0}")
                # Пробуем MOEX ISS как fallback
                try:
                    df_moex = await fetch_candles_moex(ticker, interval, limit)
                    if df_moex is not None and len(df_moex) >= 20:
                        df_result = df_moex
                        logger.info(f"analyze_stock {ticker}: использован MOEX ISS fallback ({len(df_moex)} свечей)")
                    else:
                        return {"error": f"Недостаточно данных ({ticker}, TF={tf}). "
                                         f"Попробуй /update_figi или подожди открытия сессии."}
                except Exception as moex_err:
                    return {"error": f"Недостаточно данных ({ticker}, TF={tf}): {moex_err}"}
        except Exception as fb_err:
            return {"error": f"Недостаточно данных ({ticker}, TF={tf}): {fb_err}"}

    figi = MOEX_STOCKS[ticker][0]

    trend_15m_task = get_tf_trend_state(ticker, figi, "15m")
    trend_1h_task  = get_tf_trend_state(ticker, figi, "1h")
    trend_4h_task  = get_tf_trend_state(ticker, figi, "4h")
    df_daily_task  = fetch_candles_tinkoff(figi, "CANDLE_INTERVAL_DAY", 30)

    t_15m, t_1h, t_4h, df_daily_res = await asyncio.gather(
        trend_15m_task, trend_1h_task, trend_4h_task, df_daily_task,
        return_exceptions=True
    )

    mtf_trends = {
        "15m": t_15m if not isinstance(t_15m, Exception) else "неизвестно",
        "1h":  t_1h  if not isinstance(t_1h,  Exception) else "неизвестно",
        "4h":  t_4h  if not isinstance(t_4h,  Exception) else "неизвестно",
    }
    daily_poc = calculate_daily_poc(df_daily_res) if not isinstance(df_daily_res, Exception) else None

    try:
        news_task       = fetch_russian_news(ticker, sector)
        commodity_task  = fetch_commodity_news(sector)
        sector_news_task = fetch_sector_news(sector)
        macro_task      = fetch_macro_news()
        imoex_task      = fetch_imoex_regime()
        htf_task        = get_htf_trend(ticker, figi, tf)

        news_res, commodity_res, sector_news_res, macro_res, imoex_res, htf_res = await asyncio.gather(
            news_task, commodity_task, sector_news_task, macro_task, imoex_task, htf_task,
            return_exceptions=True
        )

        if not isinstance(news_res, Exception) and news_res is not None:
            
            _settings = get_bot_settings()
            max_age = (_settings["news_max_age_liquid_min"] if ticker in LIQUID_TICKERS
                       else _settings["news_max_age_min"])
            news_items = [it for it in news_res if it.get("age_min", 0) <= max_age]
        if not isinstance(commodity_res, Exception) and commodity_res:
            
            news_items = news_items + commodity_res
        if not isinstance(sector_news_res, Exception) and sector_news_res:
            
            news_items = news_items + sector_news_res
        if not isinstance(macro_res, Exception) and macro_res:
            
            news_items = news_items + macro_res
        if not isinstance(imoex_res, Exception) and imoex_res is not None:
            imoex_regime = imoex_res
        if not isinstance(htf_res, Exception) and htf_res is not None:
            htf_trend = htf_res
    except Exception as aux_err:
        logger.warning(f"Ошибка сбора вспомогательных данных для {ticker}: {aux_err}")

    df = calculate_indicators(df_result, tf)
    df_closed = df.iloc[:-1].copy()
    price = float(df_closed["close"].iloc[-1])
    atr   = float(df_closed["atr"].dropna().iloc[-1]) if "atr" in df_closed.columns else price * 0.01
    regime  = detect_market_regime(df_closed)
    supports, resistances = find_support_resistance(df_closed, price, tf)

    liquidity_tier, liquidity_warn = get_liquidity_tier(ticker, df_closed)
    
    effective_mode = dict(mode_cfg)
    base_vol = mode_cfg.get("min_vol_ratio", 1.3)
    if liquidity_tier == "medium":
        
        effective_mode["min_vol_ratio"] = round(max(0.9, base_vol - 0.15), 2)
        
        effective_mode["max_vol_ratio"] = 4.0
    elif liquidity_tier == "low":
        
        effective_mode["min_vol_ratio"] = round(max(0.8, base_vol - 0.30), 2)
        
        effective_mode["max_vol_ratio"] = 3.0

    pd_levels  = get_previous_day_levels(df_closed)
    pd_level_name, pd_level_dist = get_pd_level_context(pd_levels, price)
    macd_div   = detect_macd_divergence(df_closed)
    session    = get_session_phase()

    tech_signal, tech_score, tech_reasons = compute_tech_score(
        df_closed, effective_mode, imoex_regime=imoex_regime,
        htf_trend=htf_trend, pd_levels=pd_levels, macd_div=macd_div)

    price_anomaly = detect_price_anomaly(df_closed, tf)

    edisclosure_items: list = []

    # AI оценка новостей — отложена: запускается после отправки сигнала
    news_ai = {"confirmed": tech_signal, "ai_skip_reason": "deferred", 
               "filter_status": "", "event_type": "", "event_weight": 0, "summary": ""}
    final_signal = tech_signal

    if imoex_regime and imoex_regime.get("regime") == "bear" and "LONG" in final_signal:
        final_signal = f"⚠️ {final_signal} (IMOEX медвежий)"

    if not session["enter"] and ("LONG" in final_signal or "SHORT" in final_signal):
        phase_warn = session.get("warning", "")
        final_signal = f"⏸ {final_signal} ({phase_warn or session['label']})"

    try:
        cal_check = check_calendar_block(ticker)
    except Exception as cal_err:
        logger.warning(f"Ошибка календаря для {ticker}: {cal_err}")

    cal_penalty = cal_check.get("score_penalty", 0)

    if cal_check["block"] and ("LONG" in final_signal or "SHORT" in final_signal):
        final_signal = f"🚫 {final_signal} - СТОП (важное событие через <30 мин)"
    elif cal_penalty >= 15 and ("LONG" in final_signal or "SHORT" in final_signal):
        if "CONFIRMED" in final_signal:
            final_signal = final_signal.replace("CONFIRMED", "WEAK ⚠️ (событие)")

    if cal_penalty > 0:
        tech_score = max(0, tech_score - cal_penalty)

    div_check = {"block": False, "already_passed": False, "dividend_info": ""}
    try:
        div_check = check_dividend_cutoff(ticker)
    except Exception as div_err:
        logger.warning(f"Ошибка проверки дивидендов для {ticker}: {div_err}")

    if div_check["block"] and "LONG" in final_signal:
        final_signal = f"🚫 {final_signal} - СТОП ({div_check['dividend_info']})"
        tech_score = max(0, tech_score - 30)
    elif div_check.get("already_passed") and "SHORT" in final_signal:
        
        days_since = abs(div_check.get("days_until", 0) or 0)
        if days_since <= 2:
            if "CONFIRMED" in final_signal:
                final_signal = final_signal.replace(
                    "CONFIRMED",
                    f"WEAK ⚠️ (дивидендный гэп {days_since:.0f} дн. назад - "
                    f"падение может быть механическим, не рыночным)"
                )

    try:
        sector_check = check_signal_against_sector_modifier(sector, final_signal)
    except Exception as sm_err:
        logger.debug(f"check_signal_against_sector_modifier {ticker}: {sm_err}")
        sector_check = {"conflict": False, "warning_ru": ""}

    if sector_check["conflict"] and "CONFIRMED" in final_signal:
        final_signal = final_signal.replace("CONFIRMED", "WEAK ⚠️ (геополитика)")

    sl_tp = calculate_sl_tp_stocks(tech_signal, price, atr, supports, resistances, pd_levels=pd_levels)

    candle_progress_pct  = 0.0
    candle_progress_note = ""
    try:
        last_open  = float(df["open"].iloc[-1])
        last_high  = float(df["high"].iloc[-1])
        last_low   = float(df["low"].iloc[-1])
        candle_range = last_high - last_low
        if candle_range > 0:
            if "LONG" in tech_signal:
                candle_progress_pct = (price - last_low)  / candle_range * 100
            elif "SHORT" in tech_signal or "ВЫХОД" in tech_signal:
                candle_progress_pct = (last_high - price) / candle_range * 100
            else:
                candle_progress_pct = (price - last_low)  / candle_range * 100
            
            candle_progress_pct = max(0.0, min(100.0, candle_progress_pct))
        if candle_progress_pct >= 80:
            candle_progress_note = (
                f"⏳ <b>Поздний вход:</b> свеча пройдена на {candle_progress_pct:.0f}% - "
                f"риск/доходность ухудшились. Жди следующей свечи или откат."
            )
        elif candle_progress_pct >= 55:
            candle_progress_note = f"⚠️ Свеча пройдена на {candle_progress_pct:.0f}% - вход с осторожностью."
        else:
            candle_progress_note = f"✅ Свеча пройдена на {candle_progress_pct:.0f}% - вход актуален."
    except Exception:
        pass

    daily_atr_progress_pct  = 0.0
    daily_atr_progress_note = ""
    try:
        
        df_daily = df_closed.copy()
        df_daily["date"] = pd.to_datetime(df_daily["timestamp"]).dt.date if "timestamp" in df_daily.columns else None
        if df_daily["date"] is not None and df_daily["date"].notna().any():
            
            day_group = df_daily.groupby("date").agg(
                open=("open", "first"), high=("high", "max"),
                low=("low", "min"),   close=("close", "last")
            ).tail(20)
            if len(day_group) >= 5:
                
                daily_ranges = (day_group["high"] - day_group["low"]).tail(14)
                avg_daily_atr = float(daily_ranges.mean())
                
                today_high = float(day_group["high"].iloc[-1])
                today_low  = float(day_group["low"].iloc[-1])
                today_range = today_high - today_low
                if avg_daily_atr > 0:
                    daily_atr_progress_pct = today_range / avg_daily_atr * 100
                    pct = round(daily_atr_progress_pct)
                    if daily_atr_progress_pct >= 90:
                        daily_atr_progress_note = (
                            f"🔴 <b>Дневной ATR исчерпан на {pct}%</b> - день выдохся. "
                            f"Новые входы очень рискованны."
                        )
                    elif daily_atr_progress_pct >= 70:
                        daily_atr_progress_note = (
                            f"🟡 Дневной ATR пройден на {pct}% - "
                            f"потенциал движения ограничен."
                        )
                    else:
                        daily_atr_progress_note = (
                            f"🟢 Дневной ATR пройден на {pct}% - "
                            f"пространство для движения есть."
                        )
    except Exception:
        pass

    try:
        
        has_direction = "LONG" in tech_signal or "SHORT" in tech_signal
        logged_tech_score = tech_score if has_direction else 0

        _rpush_log(RK_SIGNAL_LOG, {
            "ts":           datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "ticker":       ticker,
            "tf":           tf,
            "price":        price,
            "tech_signal":  tech_signal,
            "tech_score":   logged_tech_score,
            "final_signal": final_signal,
            "filter_status": news_ai.get("filter_status", ""),
            "event_type":   news_ai.get("event_type", ""),
            "event_weight": news_ai.get("event_weight", 0),
            "ai_summary":   news_ai.get("summary", ""),
            "ai_skip_reason": news_ai.get("ai_skip_reason", ""),
            "macd_div":     macd_div,
            "htf_trend":    htf_trend.get("trend", "") if htf_trend else "",
            "imoex_regime": imoex_regime.get("regime", "") if imoex_regime else "",
            "candle_progress_pct":    round(candle_progress_pct, 1),
            "daily_atr_progress_pct": round(daily_atr_progress_pct, 1),
            "mtf_trends":    json.dumps(mtf_trends, ensure_ascii=False),
            "daily_poc":     daily_poc,
        })
    except Exception as log_err:
        logger.debug(f"Signal log write failed for {ticker}: {log_err}")

    try:
        # Регистрируем ВСЕ технические сигналы с целями в базе отслеживания исходов
        if has_direction and sl_tp:
            signal_id = f"{ticker}_{tf}_{int(time.time())}"
            _pending_outcomes_add(signal_id, {
                "ticker":        ticker,
                "tf":            tf,
                "direction":     "LONG" if "LONG" in tech_signal else "SHORT",
                "signal_ts":     datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "entry_price":   price,
                "sl":            sl_tp.get("sl", 0),
                "tp1":           sl_tp.get("tp1", 0),
                "tp2":           sl_tp.get("tp2", 0),
                "tp3":           sl_tp.get("tp3", 0),
                "tech_score":    logged_tech_score,
                "filter_status": news_ai.get("filter_status", "CONFIRMED"),
                "event_type":    news_ai.get("event_type", ""),
                "figi":          MOEX_STOCKS.get(ticker, ("",))[0],
                "mfe_pct":       0.0,
                "mae_pct":       0.0,
            })
    except Exception as reg_err:
        logger.debug(f"Retro outcome registration failed for {ticker}: {reg_err}")

    return {
        "ticker": ticker, "name": name, "sector": sector, "tf": tf,
        "price": price, "atr": round(atr, 2), "atr_pct": round(atr / price * 100, 2),
        "tech_signal": tech_signal, "tech_score": tech_score, "tech_reasons": tech_reasons,
        "regime": regime, "imoex_regime": imoex_regime,
        "htf_trend": htf_trend, "pd_levels": pd_levels,
        "pd_level_name": pd_level_name, "pd_level_dist": pd_level_dist,
        "macd_div": macd_div, "session": session,
        "news_items": news_items[:5], "news_ai": news_ai, "final_signal": final_signal,
        "sl_tp": sl_tp, "supports": supports, "resistances": resistances,
        "rsi_div": "",
        "candle": detect_candle_pattern(df_closed),
        "vol_ratio": round(_safe_vol_ratio(df_closed["vol_ratio"].iloc[-1]), 2),
        "time_warning": session.get("warning", ""),
        "calendar": cal_check,
        "dividend": div_check,
        "sector_modifier": sector_check,
        "liquidity_tier": liquidity_tier,
        "liquidity_warn": liquidity_warn,
        "candle_progress_pct": round(candle_progress_pct, 1),
        "candle_progress_note": candle_progress_note,
        "daily_atr_progress_pct": round(daily_atr_progress_pct, 1),
        "daily_atr_progress_note": daily_atr_progress_note,
        "price_anomaly":      price_anomaly,
        "edisclosure_items":  edisclosure_items[:3],
        "mtf_trends":         mtf_trends,
        "daily_poc":          daily_poc,
    }

def format_analysis(result: dict) -> str:
    if "error" in result:
        return f"❌ {esc(result['error'])}"

    ticker = result["ticker"]; name = result["name"]; sector = result["sector"]
    tf = result["tf"]; price = result["price"]; atr_pct = result["atr_pct"]
    ts = result["tech_signal"]; tscore = result["tech_score"]; treasons = result["tech_reasons"]
    regime = result["regime"]; imoex = result.get("imoex_regime"); final = result["final_signal"]
    candle = result["candle"]; vol_r = result["vol_ratio"]
    news = result["news_items"]; time_warn = result.get("time_warning", "")

    bars = "█" * (tscore // 10) + "░" * (10 - tscore // 10)

    htf    = result.get("htf_trend", {})
    pd_lev = result.get("pd_levels", {})
    pd_nm  = result.get("pd_level_name", "")
    sess   = result.get("session", {})
    cal    = result.get("calendar", {})

    lines = [
        f"📊 <b>{esc(ticker)} - {esc(name)}</b> | {esc(sector.upper())}",
        f"⏱ <b>{tf}</b>  |  💰 <b>{price:,.2f} ₽</b>  |  ATR {atr_pct:.2f}%  |  Объём x{vol_r:.1f}",
    ]

    liq_warn = result.get("liquidity_warn", "")
    if liq_warn:
        lines.append(liq_warn)

    if sess:
        warn = f" - {esc(sess['warning'])}" if sess.get("warning") else ""
        lines.append(f"🕐 {esc(sess['label'])}{warn}")

    if cal and cal.get("warning"):
        cal_block = cal.get("block", False)
        lines.append(f"{'🚫' if cal_block else '⚠️'} <b>КАЛЕНДАРЬ:</b> {esc(cal['warning'])}")

    div_info = result.get("dividend", {})
    if div_info.get("dividend_info"):
        div_e = "🚫" if div_info.get("block") else "ℹ️"
        lines.append(f"{div_e} <b>ДИВИДЕНДЫ:</b> {esc(div_info['dividend_info'])}")

    sec_mod = result.get("sector_modifier", {})
    if sec_mod.get("warning_ru"):
        lines.append(f"🌍 <b>СЕКТОР:</b> {esc(sec_mod['warning_ru'])}")

    lines.append("")

    if htf and htf.get("label"):
        lines.append(f"<b>{esc(htf['label'])}</b>")

    if imoex:
        slope_arrow = "↑" if imoex.get("slope_10d", 0) > 0 else "↓"
        imoex_price = imoex.get("price", 0)  
        tk_used = imoex.get("ticker_used", "MOEX")
        price_part = f"{tk_used}: {imoex_price:,.2f} ₽  " if imoex_price else ""
        lines.append(
            f"<b>🏛 {esc(imoex['label'])}</b>  "
            f"{price_part}{slope_arrow}{imoex.get('slope_10d',0):+.2f}%"
        )

    mtf = result.get("mtf_trends", {})
    if mtf:
        lines.append("<b>📊 КОНТЕКСТ ТРЕНДОВ (MTF):</b>")
        lines.append(f"• на 15m: {esc(mtf.get('15m', 'неизвестно'))}")
        lines.append(f"• на 1h:  {esc(mtf.get('1h',  'неизвестно'))}")
        lines.append(f"• на 4h:  {esc(mtf.get('4h',  'неизвестно'))}")
        lines.append("")

    if pd_lev:
        pdh = pd_lev.get("pdh", 0); pdl = pd_lev.get("pdl", 0)
        pdc = pd_lev.get("pdc", 0); gap = pd_lev.get("gap_pct", 0)
        pd_line = f"📅 PDH: {pdh:,.2f}  PDL: {pdl:,.2f}  PDC: {pdc:,.2f}"
        if abs(gap) > 0.3:
            pd_line += f"  Гэп: {gap:+.2f}%"
        if pd_nm:
            pd_line += f"  ⚡ Цена у <b>{pd_nm}</b>"
        lines.append(pd_line)

    poc = result.get("daily_poc")
    if poc:
        lines.append(f"📊 <b>HVN (зона макс. объёма):</b> {poc:,.2f} ₽")
    lines.append("")

    lines += [
        f"<b>🔧 ТЕХНИЧЕСКИЙ АНАЛИЗ</b>",
        f"{ts} (Скор: {tscore}/100)",
        f"<code>{bars}</code>",
        f"Факторы: {esc(', '.join(treasons[:3]))}",
        f"Состояние: {esc(regime['label'])}",
    ]
    if candle and candle != "Обычная свеча":
        lines.append(f"Свеча: {esc(candle)}")

    lines += ["", "<b>📰 НОВОСТНОЙ БЛОК</b>"]
    news_ai = result["news_ai"]
    fs      = news_ai.get("filter_status", "CONFIRMED")
    ew      = news_ai.get("event_weight", 0)
    ev      = news_ai.get("event_type", "нет событий")
    summ    = news_ai.get("summary", "")
    movement_explained = news_ai.get("movement_explained", False)
    fs_emoji = {"CONFIRMED": "✅", "WEAK": "🟡", "WATCH": "👀",
                "BLOCKED": "🚫", "NEWS_ONLY": "📢"}.get(fs, "⚪")
    ew_sign = f"+{ew}" if ew > 0 else str(ew)

    anomaly = result.get("price_anomaly")
    if anomaly:
        urgency_e = "🔴" if anomaly["urgency"] == "высокая" else ("🟡" if anomaly["urgency"] == "средняя" else "🟢")
        lines.append(f"{urgency_e} <b>Аномалия:</b> {esc(anomaly['description'])}")

    lines.append(f"{fs_emoji} <b>{fs}</b>  |  Вес: {ew_sign}/10")
    if summ:
        explained_tag = " ✔ движение объяснено" if movement_explained else ""
        lines.append(f"<i>{esc(summ)}{explained_tag}</i>")
    elif ev and ev != "нет событий":
        lines.append(f"Событие: {esc(ev)}")
    elif not anomaly:
        lines.append("<i>Новостного фона нет - чисто техническое движение</i>")

    lines += ["", f"<b>🎯 СИГНАЛ: {esc(final)}</b>"]
    if time_warn:
        lines.append(f"<i>{esc(time_warn)}</i>")

    candle_note = result.get("candle_progress_note", "")
    if candle_note:
        lines.append(candle_note)

    daily_atr_note = result.get("daily_atr_progress_note", "")
    if daily_atr_note:
        lines.append(daily_atr_note)

    sl_tp = result.get("sl_tp", {})
    if sl_tp:
        atr = result.get("atr", 0)
        price = result.get("price", 0)
        if atr and price:
            dist_to_tp1 = abs(sl_tp.get("tp1", price) - price)
            if dist_to_tp1 > atr * 1.5:
                lines.append(f"⚠️ <i>TP1 далеко: {dist_to_tp1/atr:.1f}×ATR - вход с повышенным риском</i>")
            dist_to_sl = abs(price - sl_tp.get("sl", price))
            if dist_to_sl > atr:
                lines.append(f"⚠️ <i>SL широкий: {dist_to_sl/atr:.1f}×ATR</i>")
    if sl_tp:
        lines += [
            "", "<b>📐 ЦЕЛИ (ИНТРАДЕЙ)</b>",
            f"STOP: {sl_tp['sl']:,.2f} ₽  ({sl_tp['risk_pct']:.2f}% риск)",
            f"TP1:  {sl_tp['tp1']:,.2f} ₽",
            f"TP2:  {sl_tp['tp2']:,.2f} ₽  (R/R {sl_tp['rr_ratio']:.1f})",
        ]
        if sl_tp.get("tp3"):
            lines.append(f"TP3:  {sl_tp['tp3']:,.2f} ₽")
        if sl_tp.get("warn"):
            lines.append(sl_tp["warn"])

    fact_news       = [it for it in news if it.get("is_fact") and not it.get("is_corporate")]
    commodity_news  = [it for it in news if it.get("is_commodity")]
    sector_news_list = [it for it in news if it.get("is_sector_news")]
    macro_news      = [it for it in news if it.get("is_macro")]

    if fact_news:
        lines += ["", "📌 <b>RSS факты:</b>"]
        for it in fact_news[:2]:
            w   = it.get("weight", 0)
            w_e = "🟢" if w > 2 else ("🔴" if w < -2 else "⚪")
            lines.append(f"{w_e} {esc(it['title'][:90])}")

    if commodity_news:
        lines += ["", "🌍 <b>Мировой рынок:</b>"]
        for it in commodity_news[:3]:
            age = int(it.get("age_min", 0))
            age_str = f"{age}м" if age < 60 else f"{age//60}ч"
            lines.append(f"  • {esc(it['title'][:90])}  <i>({age_str})</i>")

    if sector_news_list:
        lines += ["", "🏢 <b>Сектор (регуляторика/рынок):</b>"]
        for it in sector_news_list[:3]:
            age = int(it.get("age_min", 0))
            age_str = f"{age}м" if age < 60 else f"{age//60}ч"
            lines.append(f"  • {esc(it['title'][:90])}  <i>({age_str})</i>")

    if macro_news:
        lines += ["", "🏛 <b>Макро (влияет на весь рынок):</b>"]
        for it in macro_news[:2]:
            age = int(it.get("age_min", 0))
            age_str = f"{age}м" if age < 60 else f"{age//60}ч"
            lines.append(f"  ⚠️ {esc(it['title'][:90])}  <i>({age_str})</i>")

    lines += [
        "",
        f"<i>⏰ {msk_now().strftime('%d.%m.%Y %H:%M')} МСК</i>",
        "<i>⚠️ Фьючерсы закрываются в 23:50. Акции в минусе - тоже. Прибыльные позиции могут переноситься.</i>",
    ]
    return "\n".join(lines)

_user_state: dict = {}

def get_user_state(chat_id: int) -> dict:
    return _user_state.get(chat_id, {"mode": "mid", "tf": DEFAULT_TF})

def set_user_state(chat_id: int, **kwargs):
    s = get_user_state(chat_id)
    s.update(kwargs)
    _user_state[chat_id] = s

def _load_figi_from_file():
    try:
        data = _load_json(RK_FIGI, FIGI_FILE, {})
        if not data:
            return 0
        updated = 0
        for ticker, info in data.items():
            if ticker in MOEX_STOCKS and info.get("figi"):
                old_entry = MOEX_STOCKS[ticker]
                MOEX_STOCKS[ticker] = (info["figi"], old_entry[1], old_entry[2])
                if info.get("currency") and info["currency"].lower() not in ("rub", ""):
                    _instrument_currency[info["figi"]] = info["currency"].lower()
                updated += 1
        if updated:
            logger.info(f"FIGI loaded from Redis/file: {updated} tickers updated")
        return updated
    except Exception as e:
        logger.warning(f"Failed to load FIGI: {e}")
        return 0

def _futures_base_code(ticker: str) -> str:
    """
    Извлекает базовый код фьючерса из его тикера универсально для любой
    длины префикса. Раньше был ticker[:2] - работает для текущего набора
    (Si, Eu, Ri, Br...), но молча ломается для любого будущего тикера с
    3+ буквенным префиксом. Ищем букву месяца по стандарту фьючерсов
    (F,G,H,J,K,M,N,Q,U,V,X,Z) перед цифрами года - всё что до неё
    и есть базовый код, независимо от длины.
    """
    m = re.match(r'^([A-Za-z]+?)([FGHJKMNQUVXZ])(\d+)$', ticker.upper())
    if m:
        return m.group(1)
    return ticker[:2].upper()  

async def fetch_nearest_futures() -> dict:
    headers = {
        "Authorization": f"Bearer {TINKOFF_TOKEN}",
        "Content-Type": "application/json",
    }
    result = {}
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/Futures",
                headers=headers,
                json={"instrumentStatus": "INSTRUMENT_STATUS_BASE"},
            ) as r:
                if r.status != 200:
                    logger.warning(f"GetFutures: HTTP {r.status}")
                    return {}
                data = await r.json()

        now = datetime.now(timezone.utc)
        by_base: dict[str, list] = {}
        for inst in data.get("instruments", []):
            ticker   = inst.get("ticker", "")
            figi     = inst.get("figi", "") or inst.get("uid", "")
            exp_str  = inst.get("expirationDate", "")
            exchange = inst.get("exchange", "")
            if not figi or not ticker:
                continue
            # Если exchange пустой или неизвестный — пропускаем (не биржевой инструмент)
            if exchange and exchange not in ("FORTS", "MOEX", "FORTS_EVENING", 
                                              "MOEX_FORTS", "FORTS_EVENING", "",
                                              "TINKOFF", "TINKOFF_IIS"):
                logger.debug(f"fetch_nearest_futures: skip {ticker} exchange={exchange}")
            base = _futures_base_code(ticker)
            try:
                exp_dt = datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
            except Exception:
                continue
            if exp_dt < now:
                continue
            by_base.setdefault(base, []).append({
                "ticker": ticker,
                "figi":   figi,
                "expiry": exp_dt,
            })

        for base, contracts in by_base.items():
            nearest = min(contracts, key=lambda x: x["expiry"])
            result[base] = nearest

        for code, info in list(FUTURES.items()):
            base = _futures_base_code(code)
            if base in result:
                near = result[base]
                old  = FUTURES[code]
                FUTURES[code] = (near["figi"],) + old[1:]
                logger.debug(f"Futures {code}: FIGI → {near['figi']} (exp {near['expiry'].date()})")

        save_data = {k: {"ticker": v["ticker"], "figi": v["figi"],
                         "expiry": v["expiry"].isoformat()}
                     for k, v in result.items()}
        _save_json(RK_FUT_EXP, FUTURES_EXPIRY_FILE, save_data)
        logger.info(f"Futures FIGI updated: {len(result)} base assets: {list(result.keys())}")
        if not result:
            logger.warning("fetch_nearest_futures: NO futures found! Exchange filter may be too strict.")

    except Exception as e:
        logger.error(f"fetch_nearest_futures: {e}")

    return result

async def update_figi_data() -> tuple:
    """Обновляет FIGI из Tinkoff API. Возвращает (count, err, updated_list, not_found_list, fut_count)."""
    headers = {
        "Authorization": f"Bearer {TINKOFF_TOKEN}",
        "Content-Type": "application/json",
    }
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        api_index = {}

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/Shares",
                headers=headers, json={"instrumentStatus": "INSTRUMENT_STATUS_BASE"},
            ) as r1:
                if r1.status == 200:
                    shares_data = await r1.json()
                    for inst in shares_data.get("instruments", []):
                        ticker = inst.get("ticker", "")
                        if ticker and inst.get("figi"):
                            api_index[ticker] = {
                                "figi":     inst["figi"],
                                "name":     inst.get("name", ""),
                                "uid":      inst.get("uid", ""),
                                "currency": inst.get("currency", "rub"),
                            }

            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/DepositoryReceipts",
                headers=headers, json={"instrumentStatus": "INSTRUMENT_STATUS_BASE"},
            ) as r2:
                if r2.status == 200:
                    dr_data = await r2.json()
                    for inst in dr_data.get("instruments", []):
                        ticker = inst.get("ticker", "")
                        if ticker and inst.get("figi"):
                            api_index[ticker] = {
                                "figi":     inst["figi"],
                                "name":     inst.get("name", ""),
                                "uid":      inst.get("uid", ""),
                                "currency": inst.get("currency", "usd"),  
                            }

        updated, not_found = [], []

        TICKER_ALIASES = {
            "CIAN":  ["CIAN", "CNRU"],
            "FIXP":  ["FIXP", "FIXRU", "FIXR"],
            "VKCO":  ["VKCO", "VKRU"],
            "OZON":  ["OZON", "OZONRU"],
            "YDEX":  ["YDEX", "YDEXRU"],
            "TCSG":  ["TCSG", "TCS", "T"],
            "T":     ["T", "TCSG", "TCS"],
            "X5":    ["X5", "FIVE"],
            "FIXR":  ["FIXR", "FIXP", "FIXRU"],
        }

        for ticker in list(MOEX_STOCKS.keys()):
            
            candidates = TICKER_ALIASES.get(ticker, [ticker])
            found = None
            for candidate in candidates:
                if candidate in api_index:
                    found = api_index[candidate]
                    break
            if found:
                old_entry = MOEX_STOCKS[ticker]
                MOEX_STOCKS[ticker] = (found["figi"], old_entry[1], old_entry[2])
                
                curr = found.get("currency", "rub").lower()
                _instrument_currency[found["figi"]] = curr
                if curr not in ("rub", ""):
                    logger.info(f"Currency flag: {ticker} figi={found['figi']} currency={curr}")
                updated.append(ticker)
                if candidates[0] != ticker or (len(candidates) > 1 and candidates[0] != ticker):
                    logger.info(f"FIGI alias match: {ticker} → found as {candidates}")
            else:
                # Fallback: поиск через FindInstrument
                try:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as fallback_session:
                        async with fallback_session.post(
                            f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument",
                            headers=headers,
                            json={"query": ticker, "instrumentKind": "INSTRUMENT_TYPE_SHARE",
                                  "apiTradeAvailableFlag": False},
                        ) as fr:
                            if fr.status == 200:
                                fdata = await fr.json()
                                finsts = fdata.get("instruments", [])
                                if finsts:
                                    found_figi = finsts[0].get("figi", "")
                                    if found_figi and found_figi.startswith("BBG"):
                                        old_entry = MOEX_STOCKS[ticker]
                                        MOEX_STOCKS[ticker] = (found_figi, old_entry[1], old_entry[2])
                                        _instrument_currency[found_figi] = finsts[0].get("currency", "rub").lower()
                                        updated.append(ticker)
                                        logger.info(f"FIGI fallback FindInstrument: {ticker} -> {found_figi}")
                                        continue
                except Exception as fe:
                    logger.debug(f"FindInstrument fallback for {ticker}: {fe}")
                not_found.append(ticker)

        fut_updated = 0
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/Futures",
                    headers=headers, json={"instrumentStatus": "INSTRUMENT_STATUS_BASE"},
                ) as r3:
                    if r3.status == 200:
                        fdata = await r3.json()
                        fut_index = {
                            i.get("ticker", ""): (i.get("figi", "") or i.get("uid", ""))
                            for i in fdata.get("instruments", [])
                            if i.get("figi") or i.get("uid")
                        }
                        for code in list(FUTURES.keys()):
                            if code in fut_index:
                                old = FUTURES[code]
                                FUTURES[code] = (fut_index[code],) + old[1:]
                                fut_updated += 1
                        api_index.update({k: {"figi": v} for k, v in fut_index.items()})
        except Exception as fe:
            logger.warning(f"Futures FIGI update error: {fe}")

        _save_json(RK_FIGI, FIGI_FILE, api_index)

        logger.info(f"FIGI updated successfully: stocks & GDR {len(updated)} OK, futures {fut_updated} OK, not_found {len(not_found)}")
        return len(updated) + fut_updated, None, updated, not_found, fut_updated

    except Exception as e:
        logger.error(f"FIGI update failed: {e}")
        return None, str(e)


async def cmd_ai_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /ai — переключить ИИ вкл/выкл."""
    new_state = not get_ai_enabled()
    set_ai_enabled(new_state)
    global MAIN_MENU_KEYBOARD, MENU_BUTTON_LABELS
    MAIN_MENU_KEYBOARD = _build_main_menu_keyboard()
    MENU_BUTTON_LABELS = {
        btn.text for row in MAIN_MENU_KEYBOARD.keyboard for btn in row
    }
    icon = "🟢" if new_state else "🔴"
    status = "включены" if new_state else "выключены"
    await update.message.reply_text(
        f"{icon} <b>Все ИИ-функции {status}</b>\n"
        f"{'Бот будет использовать Gemini + Groq + OpenRouter для анализа новостей и сигналов.' if new_state else 'Сканер работает по технике без ИИ. Новости не классифицируются.'}",
        parse_mode="HTML",
        reply_markup=MAIN_MENU_KEYBOARD
    )


async def cmd_ai_memory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mem = _load_ai_memory()
    if not mem:
        await update.message.reply_text("🧠 Память ИИ пока пуста. Накапливается статистика...")
        return
    lines = ["🧠 <b>AI Memory — Точность ИИ по категориям новостей</b>\n"]
    for ev_type, stats in sorted(mem.items(), key=lambda x: -x[1].get("total", 0)):
        total = stats.get("total", 0)
        hits = stats.get("hits", 0)
        acc = (hits / total * 100) if total > 0 else 0
        mult = get_ai_category_multiplier(ev_type)
        status_e = "🟢" if acc >= 70 else ("🟡" if acc >= 50 else "🔴")
        lines.append(f"{status_e} <b>{esc(ev_type)}</b>: точность <b>{acc:.1f}%</b> ({hits}/{total}) [x{mult:.2f}]")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


def _build_main_menu_keyboard() -> ReplyKeyboardMarkup:
    ai_label = "🟢 ИИ: ВКЛ" if get_ai_enabled() else "🔴 ИИ: ВЫКЛ"
    kb = ReplyKeyboardMarkup(
        [
            [KeyboardButton("📊 Анализ"), KeyboardButton("🔍 Скан")],
            [KeyboardButton("🗂 Ватчлист"), KeyboardButton("📂 Сделки")],
            [KeyboardButton("📈 Фьючерсы"), KeyboardButton("🚀 Памп/Дамп")],
            [KeyboardButton("📅 Календарь"), KeyboardButton("⚙️ Режим")],
            [KeyboardButton("📰 Новости"), KeyboardButton("🔧 Настройки бота")],
            [KeyboardButton(ai_label)],
            [KeyboardButton("🔍 Диагностика")],
            [KeyboardButton("📈 Стата сигналов"), KeyboardButton("ℹ️ Помощь")],
            [KeyboardButton("❌ Скрыть меню")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )
    return kb

MAIN_MENU_KEYBOARD = _build_main_menu_keyboard()

AI_MENU_LABELS = {"🟢 ИИ: ВКЛ", "🔴 ИИ: ВЫКЛ"}

MENU_BUTTON_LABELS: set[str] = {
    btn.text for row in MAIN_MENU_KEYBOARD.keyboard for btn in row
}

async def _set_bot_commands(app):
    """Регистрирует команды в кнопке 'Меню' Telegram (выпадающий список)."""
    commands = [
        BotCommand("start",       "🏛 Главное меню"),
        BotCommand("menu",        "📋 Показать кнопочное меню снизу"),
        BotCommand("analyze",     "📊 Анализ тикера: /analyze SBER"),
        BotCommand("scan",        "🔍 Сканер сигналов сейчас"),
        BotCommand("scan_start",  "▶️ Автосигналы каждые 30 мин"),
        BotCommand("scan_stop",   "⏹ Выключить автосигналы"),
        BotCommand("watchlist",   "🗂 Мой ватчлист"),
        BotCommand("add",         "➕ Добавить тикер: /add GAZP"),
        BotCommand("remove",      "➖ Удалить тикер"),
        BotCommand("add_all",     "📋 Добавить все акции"),
        BotCommand("all_tickers", "📜 Список всех инструментов"),
        BotCommand("trades",      "📂 Открытые сделки"),
        BotCommand("open_trade",  "✅ Открыть сделку вручную"),
        BotCommand("close_trade", "❌ Закрыть сделку"),
        BotCommand("export_log",  "📤 Выгрузить лог сигналов CSV"),
        BotCommand("futures",     "📈 Анализ фьючерса: /futures SBM6"),
        BotCommand("futures_list","📃 Список фьючерсов FORTS"),
        BotCommand("scan_futures","🔍 Сканер фьючерсов"),
        BotCommand("fadd",        "➕ Добавить фьючерс"),
        BotCommand("fadd_all",    "📋 Добавить все фьючерсы"),
        BotCommand("fremove",     "➖ Удалить фьючерс"),
        BotCommand("pd",          "🚀 Памп/Дамп детектор"),
        BotCommand("news",        "📰 Новости по тикеру: /news SBER"),
        BotCommand("market",      "🏛 Обзор рынка IMOEX"),
        BotCommand("calendar",    "📅 Календарь событий"),
        BotCommand("calendar_add","➕ Добавить событие в календарь"),
        BotCommand("mode",        "⚙️ Режим риска LOW/MID/HARD"),
        BotCommand("tf",          "⏱ Таймфрейм анализа"),
        BotCommand("update_figi", "🔄 Обновить базу FIGI"),
        BotCommand("settings",    "🔧 Настройки бота - пороги сигналов"),
        BotCommand("stats",       "📊 Статистика по закрытым сделкам"),
        BotCommand("outcome_stats","📈 Статистика по всем сигналам (не только сделкам)"),
        BotCommand("sectors",      "🌍 Активные секторальные геополитические факторы"),
        BotCommand("diagnostics",    "🔍 Полная диагностика всех систем"),
        BotCommand("ai",             "🤖 Вкл/выкл ИИ одной кнопкой"),
    ]
    try:
        await app.bot.set_my_commands(commands)
        logger.info(f"Bot commands registered: {len(commands)}")
    except Exception as e:
        logger.warning(f"set_my_commands failed: {e}")

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🏛 <b>MOEX Intraday Bot</b>\n"
        "Интрадей-сигналы 1-2 эшелона МосБиржи.\n"
        "Работает во время утренней, основной и вечерней сессии (07:00 - 23:50 МСК).\n\n"
        "<b>📊 Функции:</b>\n"
        "/analyze SBER - технический + vwap анализ\n"
        "/watchlist - управление ватчлистом\n"
        "/scan - ручной запуск сканера\n"
        "/scan_start - автосигналы каждые 30 минут\n"
        "/scan_stop - выключить автосигналы\n"
        "/mode - настройки риска (LOW / MID / HARD)\n"
        "/tf - таймфрейм (15m по умолчанию)\n"
        "/trades - открытые позиции\n"
        "/export_log [N] - выгрузить лог сигналов в CSV (по умолч. 1000)\n"
        "/open_trade - открыть сделку вручную\n"
        "/close_trade - закрыть сделку\n"
        "/update_figi - обновить базу FIGI\n\n"
        "<b>📈 Фьючерсы FORTS:</b>\n"
        "/futures CODE - анализ фьючерса (SBM6, BRM6, RIM6...)\n"
        "/scan_futures - сканер фьючерсов\n"
        "/futures_list - все доступные\n"
        "/fadd_all - добавить все сразу\n"
        "/fadd CODE - добавить один\n"
        "/fremove CODE - убрать\n\n"
        "<b>🚀 Памп/Дамп детектор:</b>\n"
        "/pd - найти памп/дамп прямо сейчас\n"
        "/pd futures - только фьючерсы\n"
        "/pd stocks  - только акции\n"
        "Авто-алерты: включаются вместе с /scan_start\n\n"
        "<b>📅 Календарь событий:</b>\n"
        "/calendar - заседания ЦБ, отсечки, события\n"
        "/calendar update - загрузить с MOEX и ЦБ\n"
        "/calendar_add - добавить событие вручную\n\n"
        "<i>⚠️ Фьючерсы закрываются в 23:50. Акции в плюсе - переносятся. SL всегда активен.</i>\n\n"
        "👇 Используй меню снизу или кнопку 'Меню' рядом с полем ввода для быстрого доступа к командам."
    )
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=MAIN_MENU_KEYBOARD)

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возвращает скрытую кнопочную клавиатуру обратно."""
    await update.message.reply_text(
        "📋 Меню вернул на место.",
        reply_markup=MAIN_MENU_KEYBOARD,
    )

async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wl = load_watchlist()
    lines = [f"🗂 <b>Мой ватчлист ({len(wl)}/100):</b>\n"]
    for t in wl:
        info = MOEX_STOCKS.get(t)
        if info:
            _, name, sector, *_ = info
            lines.append(f"<code>{t}</code> - {name} <i>({sector})</i>")
        else:
            lines.append(f"<code>{t}</code>")
    lines += ["", "/add TICKER - добавить инструмент",
              "/remove TICKER - удалить инструмент",
              "/clear_watchlist - очистить весь ватчлист"]
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_all_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sector_map: dict[str, list] = {}
    for ticker, (_, name, sector, *_) in MOEX_STOCKS.items():
        sector_map.setdefault(sector, []).append(f"<code>{ticker}</code> {name}")
    lines = ["📋 <b>Инструменты МосБиржи (1-2 эшелон)</b>\n"]
    for sector, items in sorted(sector_map.items()):
        lines.append(f"\n<b>{sector.upper()}</b>")
        lines.extend(items)
    text = "\n".join(lines)
    
    chunk_size = 4000
    if len(text) <= chunk_size:
        await update.message.reply_text(text, parse_mode="HTML")
    else:
        chunks, current = [], []
        current_len = 0
        for line in lines:
            if current_len + len(line) + 1 > chunk_size and current:
                await update.message.reply_text("\n".join(current), parse_mode="HTML")
                current, current_len = [], 0
            current.append(line)
            current_len += len(line) + 1
        if current:
            await update.message.reply_text("\n".join(current), parse_mode="HTML")

async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /add TICKER")
        return
    ticker = context.args[0].upper().strip()
    ok, msg = add_to_watchlist(ticker)
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /remove TICKER")
        return
    ticker = context.args[0].upper().strip()
    ok, msg = remove_from_watchlist(ticker)
    await update.message.reply_text(msg, parse_mode="HTML")

async def cmd_add_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    all_tickers = list(MOEX_STOCKS.keys())
    added, skipped = [], []
    for t in all_tickers:
        ok, _ = add_to_watchlist(t)
        (added if ok else skipped).append(t)
    await update.message.reply_text(f"Добавлено: {len(added)}, уже в списке: {len(skipped)}")

async def cmd_clear_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or args[0].lower() != "confirm":
        await update.message.reply_text("Для очистки всего списка введите: /clear_watchlist confirm", parse_mode="HTML")
        return
    save_watchlist([])
    await update.message.reply_text("🗑 Ватчлист успешно очищен.")

async def cmd_add_sector(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        sectors = sorted(set(v[2] for v in MOEX_STOCKS.values()))
        lines = ["📂 <b>Доступные секторы рынка:</b>\n"]
        for s in sectors:
            lines.append(f"<code>/add_sector {s}</code>")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        return
    sector_query = " ".join(context.args).lower().strip()
    to_add = [t for t, v in MOEX_STOCKS.items() if v[2].lower() == sector_query]
    added, skipped = [], []
    for t in to_add:
        ok, _ = add_to_watchlist(t)
        (added if ok else skipped).append(t)
    await update.message.reply_text(f"Добавлено из сектора {sector_query}: {len(added)}")

async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Формат: /analyze TICKER [таймфрейм]")
        return
    ticker = args[0].upper()
    tf = args[1].lower() if len(args) > 1 else get_user_state(update.effective_chat.id)["tf"]
    if tf not in TF_MAP:
        tf = DEFAULT_TF

    msg = await update.message.reply_text(
        f"⏳ Рассчитываю индикаторы для <b>{ticker}</b> [{tf}]...", parse_mode="HTML")
    mode_cfg = TRADE_MODES[get_user_state(update.effective_chat.id)["mode"]]

    try:
        result = await analyze_stock(ticker, tf, mode_cfg)
        text   = format_analysis(result)
    except Exception as e:
        logger.error(f"analyze_stock {ticker}: {e}", exc_info=True)
        await msg.edit_text(
            f"❌ Ошибка при анализе <b>{esc(ticker)}</b>:\n"
            f"<code>{esc(str(e)[:200])}</code>\n\n"
            f"Попробуй ещё раз или проверь /update_figi",
            parse_mode="HTML")
        return

    kb = []
    if "error" not in result:
        kb.append([
            InlineKeyboardButton("📰 Новости",  callback_data=f"news_{ticker}"),
            InlineKeyboardButton("5м",          callback_data=f"analyze_{ticker}_5m"),
            InlineKeyboardButton("15м",         callback_data=f"analyze_{ticker}_15m"),
            InlineKeyboardButton("1ч",          callback_data=f"analyze_{ticker}_1h"),
        ])
        kb.append([
            InlineKeyboardButton("1д контекст",  callback_data=f"analyze_{ticker}_1d"),
            InlineKeyboardButton("➕ Ватчлист",   callback_data=f"wl_add_{ticker}"),
        ])
        sl_tp = result.get("sl_tp", {})
        if sl_tp and result.get("tech_signal") in ("🟩 LONG", "🟥 SHORT/ВЫХОД"):
            direction = "LONG" if "LONG" in result["tech_signal"] else "SHORT"
            price = result["price"]
            sl  = sl_tp.get("sl",  0)
            tp1 = sl_tp.get("tp1", 0)
            tp2 = sl_tp.get("tp2", 0)
            tp3 = sl_tp.get("tp3", 0)
            key = f"{ticker}_{direction}_{int(time.time())}"[-20:]
            _cleanup_pending_trades()
            _pending_trades[key] = {
                "ticker": ticker, "direction": direction,
                "entry": price, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                "ts": time.time()
            }
            kb.append([InlineKeyboardButton(
                f"✅ Войти ({direction})",
                callback_data=f"enter2_{key}"
            )])

    context.chat_data[f"last_analysis_{ticker}"] = {
        "tech_score":    result.get("tech_score", 0),
        "filter_status": result.get("news_ai", {}).get("filter_status", ""),
        "anomaly":       str(result.get("price_anomaly", "")),
        "ai_summary":    result.get("news_ai", {}).get("summary", ""),
        "mtf_trends":    result.get("mtf_trends", {}),
        "htf_trend":     (result.get("htf_trend") or {}).get("trend", ""),
        "imoex_regime":  (result.get("imoex_regime") or {}).get("regime", ""),
    }

    markup = InlineKeyboardMarkup(kb) if kb else None
    try:
        await msg.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        logger.warning(f"HTML parse error {ticker}: {e}")
        plain = text.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>","").replace("<code>","").replace("</code>","")
        await msg.edit_text(plain[:4000], reply_markup=markup)

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    ticker = args[0].upper() if args else ""
    sector = ""
    if ticker in MOEX_STOCKS:
        _, _, sector, *_ = MOEX_STOCKS[ticker]
    msg = await update.message.reply_text(f"⏳ Поиск событий по {ticker or 'рынку'}...", parse_mode="HTML")
    news = await fetch_russian_news(ticker, sector)
    if not news:
        await msg.edit_text("📭 Новостных фактов за последнее время не обнаружено.")
        return
    lines = [f"📰 <b>События рынка - {ticker or 'MOEX'}</b>\n"]
    for it in news[:6]:
        sp = "🔵" if it.get("is_specific") else "⚪"
        lines.append(f"{sp} <b>{it['title'][:120]}</b>")
        lines.append(f"  └ {it['source']} | {it['pub'][:16]}")
        ai = await ai_classify_news_impact(it["title"], ticker or "IMOEX")
        s_e = {"позитив": "🟢", "негатив": "🔴", "нейтрально": "⚪"}.get(ai["sentiment"], "⚪")
        lines.append(f"  └ Влияние: {s_e} {ai['sentiment']} ({ai['score']}/10)")
        lines.append("")
    await msg.edit_text("\n".join(lines), parse_mode="HTML")

async def cmd_market(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Загружаю текущую картину рынка...", parse_mode="HTML")
    tasks = [
        fetch_market_news(),
        analyze_stock("SBER", "1h", TRADE_MODES["mid"]),
        analyze_stock("GAZP", "1h", TRADE_MODES["mid"]),
        analyze_stock("LKOH", "1h", TRADE_MODES["mid"]),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    news = results[0] if not isinstance(results[0], Exception) else []
    stocks = [r for r in results[1:] if not isinstance(r, Exception) and r and "error" not in r]

    lines = ["🏛 <b>MOEX ТЕКУЩИЙ СТАТУС</b>\n"]
    if stocks:
        lines.append("<b>Ведущие инструменты (1h):</b>")
        for s in stocks:
            sig_e = {"🟩 LONG": "🟢", "🟥 SHORT/ВЫХОД": "🔴"}.get(s["tech_signal"], "⚪")
            lines.append(
                f"{sig_e} <b>{s['ticker']}</b> - {s['price']:,.2f} ₽  "
                f"Скор: {s['tech_score']} ({s['regime']['label']})"
            )
        lines.append("")
    if news:
        lines.append("<b>Лента новостей:</b>")
        for it in news[:5]:
            lines.append(f"⚪ {esc(it['title'][:100])}")
    await msg.edit_text("\n".join(lines), parse_mode="HTML")

async def _find_futures_figi(ticker: str) -> str | None:
    cache_key = f"fut_figi_{ticker}"
    if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < 86400:
        return _cache[cache_key]["figi"]
    try:
        headers = {"Authorization": f"Bearer {TINKOFF_TOKEN}", "Content-Type": "application/json"}
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument",
                headers=headers,
                json={"query": ticker, "instrumentKind": "INSTRUMENT_TYPE_FUTURES",
                      "apiTradeAvailableFlag": False},
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                instruments = data.get("instruments", [])
                # Ищем точное совпадение по тикеру
                for inst in instruments:
                    if inst.get("ticker", "").upper() == ticker.upper():
                        figi = inst.get("figi", "") or inst.get("uid", "")
                        if figi:
                            _cache[cache_key] = {"figi": figi, "ts": time.time()}
                            if ticker in FUTURES:
                                old = FUTURES[ticker]
                                FUTURES[ticker] = (figi,) + old[1:]
                            return figi
                # Берём первый результат как fallback
                if instruments:
                    inst = instruments[0]
                    figi = inst.get("figi", "") or inst.get("uid", "")
                    if figi:
                        _cache[cache_key] = {"figi": figi, "ts": time.time()}
                        if ticker in FUTURES:
                            old = FUTURES[ticker]
                            FUTURES[ticker] = (figi,) + old[1:]
                        return figi
    except Exception as e:
        logger.warning(f"FindInstrument {ticker}: {e}")
    return None

async def analyze_futures(code: str, tf: str = "15m", mode_cfg: dict = None) -> dict:
    if mode_cfg is None:
        mode_cfg = TRADE_MODES["mid"]
    code = code.upper()
    
    # ИСПРАВЛЕНИЕ: Поддержка как полного тикера (SiM6), так и базового (SiU6 или Si)
    info = FUTURES.get(code)
    if not info:
        # Пробуем найти по базовому коду (без месяца)
        base = ''.join(c for c in code if c.isalpha())
        matching = [k for k in FUTURES if k.startswith(base)]
        if matching:
            info = FUTURES[matching[0]]
            logger.info(f"analyze_futures: {code} не найден, использован {matching[0]}")
        else:
            return {"error": f"Фьючерс {code} не найден. /futures_list - список доступных"}

    figi, name, category, tick, lot = info

    if not figi or (figi.startswith("FUT") and len(figi) <= 15):
        found_figi = await _find_futures_figi(code)
        if found_figi:
            figi = found_figi
        else:
            return {
                "error": f"FIGI для {code} не найден.\nЗапусти /update_figi для обновления базы."
            }

    interval, _, limit = TF_MAP.get(tf, TF_MAP["15m"])
    df = await fetch_candles_tinkoff(figi, interval, limit)

    if df is None or len(df) < 20:
        return {"error": f"Недостаточно данных для {code} [{tf}].\nВозможно неверный FIGI - запусти /update_figi"}

    df = calculate_indicators(df)
    df_c = df.iloc[:-1].copy()
    price = float(df_c["close"].iloc[-1])
    atr   = float(df_c["atr"].dropna().iloc[-1]) if "atr" in df_c.columns else price * 0.01
    regime = detect_market_regime(df_c)

    htf_trend = {}
    base_ticker = FUTURES_BASE_ASSET.get(code)
    if base_ticker and base_ticker in MOEX_STOCKS:
        base_figi = MOEX_STOCKS[base_ticker][0]
        try:
            htf_trend = await get_htf_trend(base_ticker, base_figi, tf)
        except Exception:
            htf_trend = {}

    tech_signal, tech_score, tech_reasons = compute_tech_score(
        df_c, mode_cfg, htf_trend=htf_trend)
    supports, resistances = find_support_resistance(df_c, price, tf)
    sl_tp = calculate_sl_tp_stocks(tech_signal, price, atr, supports, resistances, tick_size=tick)

    return {
        "ticker":        code,
        "name":          name,
        "sector":        category,
        "tf":            tf,
        "price":         price,
        "atr":           round(atr, 4),
        "atr_pct":       round(atr / price * 100, 2) if price else 0,
        "tech_signal":   tech_signal,
        "tech_score":    tech_score,
        "tech_reasons":  tech_reasons,
        "regime":        regime,
        "sl_tp":         sl_tp,
        "supports":      supports,
        "resistances":   resistances,
        "candle":        detect_candle_pattern(df_c),
        "rsi_div":       "",
        "vol_ratio":     round(_safe_vol_ratio(df_c["vol_ratio"].iloc[-1]), 2),
        
        "news_items":    [],
        "imoex_regime":  None,
        "htf_trend":     htf_trend,
        "pd_levels":     {},
        "pd_level_name": "",
        "pd_level_dist": 0.0,
        "macd_div":      "",
        "session":       get_session_phase(),
        "time_warning":  "",
        "calendar":      {"block": False, "warning": "", "events": [], "score_penalty": 0},
        "candle_progress_pct":  0.0,
        "candle_progress_note": "",
        "daily_atr_progress_pct":  0.0,
        "daily_atr_progress_note": "",
        "news_ai":       {"filter_status": "NO_SIGNAL", "event_weight": 0,
                          "confirmed": tech_signal, "summary": "",
                          "blocking": [], "underreaction": False,
                          "sentiment": "нейтрально", "score": 0,
                          "fact_events": [], "opinions_skipped": 0},
        "final_signal":  tech_signal,
        "is_futures":    True,
        "tick_size":     tick,
        "lot_value":     lot,
    }

async def cmd_futures(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await cmd_futures_list(update, context)
        return

    chat_id = update.effective_chat.id
    code = args[0].upper()
    tf   = args[1].lower() if len(args) > 1 else get_user_state(chat_id)["tf"]
    if tf not in TF_MAP:
        tf = "15m"

    msg = await update.message.reply_text(
        f"⏳ Анализирую фьючерс <b>{esc(code)}</b> [{tf}]...", parse_mode="HTML")
    mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
    result = await analyze_futures(code, tf, mode_cfg)
    text   = format_analysis(result)

    kb = []
    if "error" not in result:
        kb.append([
            InlineKeyboardButton("5м",  callback_data=f"fut_{code}_5m"),
            InlineKeyboardButton("15м", callback_data=f"fut_{code}_15m"),
            InlineKeyboardButton("1ч",  callback_data=f"fut_{code}_1h"),
        ])
        sl_tp = result.get("sl_tp", {})
        if sl_tp and result.get("tech_signal") in ("🟩 LONG", "🟥 SHORT/ВЫХОД"):
            direction = "LONG" if "LONG" in result["tech_signal"] else "SHORT"
            p  = result["price"]
            sl = sl_tp.get("sl", 0)
            t1 = sl_tp.get("tp1", 0)
            t2 = sl_tp.get("tp2", 0)
            t3 = sl_tp.get("tp3", 0)
            
            key = f"{code}_{direction}"[:20]
            _cleanup_pending_trades()
            _pending_trades[key] = {"ticker": code, "direction": direction,
                                    "entry": p, "sl": sl, "tp1": t1, "tp2": t2, "tp3": t3,
                                    "ts": time.time()}
            kb.append([InlineKeyboardButton(
                f"✅ Войти ({direction})",
                callback_data=f"enter2_{key}"
            )])

    await msg.edit_text(text, parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(kb) if kb else None)

async def cmd_futures_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cats: dict[str, list] = {}
    for code, (_, name, cat, tick, _) in FUTURES.items():
        cats.setdefault(cat, []).append(f"<code>{code}</code> - {name}")
    lines = ["📋 <b>Доступные фьючерсы FORTS</b>\n"]
    cat_labels = {"валюта": "💵 Валюта", "индекс": "📈 Индексы",
                  "товар": "🛢 Товары", "акция": "📊 Акционные"}
    for cat, items in cats.items():
        lines.append(f"\n<b>{cat_labels.get(cat, cat)}</b>")
        lines.extend(items)
    lines += [
        "",
        "Анализ: /futures CODE [tf]",
        "Пример: /futures SiM6 15m",
        "Сканер: /scan_futures",
        "Добавить в сканер: /fadd CODE",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_scan_futures(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id  = update.effective_chat.id
    mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
    tf       = get_user_state(chat_id)["tf"]
    wl       = load_futures_watchlist()

    if not wl:
        await update.message.reply_text(
            "📭 Фьючерсный ватчлист пуст.\n/fadd CODE - добавить\n/futures_list - список")
        return

    msg = await update.message.reply_text(
        f"🔍 Сканирую {len(wl)} фьючерсов [{tf}]...", parse_mode="HTML")

    long_sigs, short_sigs, watch_sigs = [], [], []
    for i in range(0, len(wl), 4):
        batch = wl[i:i+4]
        tasks = [analyze_futures(c, tf, mode_cfg) for c in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception) or not r or "error" in r:
                continue
            sig = r["tech_signal"]
            if sig == "🟩 LONG":      long_sigs.append(r)
            elif sig == "🟥 SHORT/ВЫХОД": short_sigs.append(r)
            elif r["tech_score"] >= 52:   watch_sigs.append(r)
        await asyncio.sleep(0.3)

    long_sigs.sort(key=lambda x: -x["tech_score"])
    short_sigs.sort(key=lambda x: -x["tech_score"])
    ts = msk_now().strftime("%d.%m.%Y %H:%M")

    if not long_sigs and not short_sigs:
        await msg.edit_text(
            f"😶 Сигналов по фьючерсам нет [{tf}]\n<i>{ts}</i>", parse_mode="HTML")
        return

    lines = [f"🔍 <b>СКАНЕР ФЬЮЧЕРСОВ FORTS</b> | {tf}", f"<i>{ts}</i>", ""]
    for sig_list, label in [(long_sigs, "🟩 ПОКУПКА"), (short_sigs, "🟥 ПРОДАЖА")]:
        if sig_list:
            lines.append(f"<b>{label}:</b>")
            for s in sig_list[:5]:
                lines.append(
                    f"<b>{esc(s['ticker'])}</b> - {esc(s['name'])}\n"
                    f" 💰 {s['price']:,.2f} | Скор: {s['tech_score']}/100\n"
                    f" ⚙️ {esc(s['tech_reasons'][0] if s['tech_reasons'] else '-')}"
                )
                lines.append("")
    if watch_sigs:
        lines.append("👀 <b>На радаре:</b>")
        for s in watch_sigs[:3]:
            lines.append(f" <b>{esc(s['ticker'])}</b> {s['price']:,.2f} Скор:{s['tech_score']}")

    kb  = []
    for s in (long_sigs + short_sigs):  
        sl_tp     = s.get("sl_tp", {})
        direction = "LONG" if "LONG" in s["tech_signal"] else "SHORT"
        row = [InlineKeyboardButton(f"📊 {s['ticker']}", callback_data=f"fut_{s['ticker']}_{tf}")]
        if sl_tp and sl_tp.get("sl"):
            p  = s["price"]; sl = sl_tp.get("sl",0)
            t1 = sl_tp.get("tp1",0); t2 = sl_tp.get("tp2",0); t3 = sl_tp.get("tp3",0)
            
            key = f"{s['ticker']}_{direction}"[:20]
            _cleanup_pending_trades()
            _pending_trades[key] = {"ticker": s["ticker"], "direction": direction,
                                    "entry": p, "sl": sl, "tp1": t1, "tp2": t2, "tp3": t3,
                                    "ts": time.time()}
            row.append(InlineKeyboardButton(
                f"✅ Войти {direction}",
                callback_data=f"enter2_{key}"
            ))
        kb.append(row)
    await msg.edit_text("\n".join(lines), parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(kb) if kb else None)

async def cmd_fadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /fadd CODE\nПример: /fadd SiM6")
        return
    code = context.args[0].upper()
    if code not in FUTURES:
        similar = [c for c in FUTURES if c[:2] == code[:2]]
        hint = f" Похожие: {', '.join(similar[:4])}" if similar else ""
        await update.message.reply_text(f"❌ {esc(code)} не найден.{hint}\n/futures_list")
        return
    wl = load_futures_watchlist()
    if code in wl:
        await update.message.reply_text(f"ℹ️ {code} уже в ватчлисте.")
        return
    wl.append(code)
    save_futures_watchlist(wl)
    _, name, cat, _, _ = FUTURES[code]
    await update.message.reply_text(
        f"✅ <b>{code}</b> ({name}) добавлен в фьючерсный ватчлист.\nВсего: {len(wl)}",
        parse_mode="HTML")

async def cmd_fremove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /fremove CODE")
        return
    code = context.args[0].upper()
    wl = load_futures_watchlist()
    if code not in wl:
        await update.message.reply_text(f"ℹ️ {code} не найден в ватчлисте.")
        return
    wl.remove(code)
    save_futures_watchlist(wl)
    await update.message.reply_text(f"🗑 {code} удалён. Осталось: {len(wl)}")

PUMP_DUMP_CONFIG = {
    "15m": {"threshold_pct": 3.0,  "lookback": 3, "vol_mult": 2.5,
            "sl_pct": 1.2, "tp1_pct": 2.5, "tp2_pct": 4.0, "tp3_pct": 6.0},
    "5m":  {"threshold_pct": 3.5,  "lookback": 4, "vol_mult": 3.0,
            "sl_pct": 0.8, "tp1_pct": 1.8, "tp2_pct": 3.0, "tp3_pct": 4.5},
    "1h":  {"threshold_pct": 7.0,  "lookback": 2, "vol_mult": 2.0,
            "sl_pct": 2.0, "tp1_pct": 3.5, "tp2_pct": 6.0, "tp3_pct": 9.0},
}

def detect_price_anomaly(df: pd.DataFrame, tf: str = "15m") -> dict | None:
    """
    Ищет аномальное движение цены за последние 2-3 свечи.
    В отличие от detect_pump_dump (порог 3%), ловит движения от 0.8%.
    Используется как триггер для поиска объясняющей новости в e-disclosure.

    Возвращает dict с описанием аномалии или None если всё в норме.
    """
    if len(df) < 20:
        return None

    min_pct = {"5m": 0.6, "15m": 0.8, "1h": 1.2, "4h": 1.8, "1d": 2.5}.get(tf, 0.8)
    min_vol = 2.0   

    recent   = df.tail(3)       
    baseline = df.tail(30).head(27)  

    price_now   = float(recent["close"].iloc[-1])
    price_start = float(recent["close"].iloc[0])
    pct_change  = (price_now - price_start) / price_start * 100

    avg_vol = float(baseline["volume"].mean()) if len(baseline) > 0 else 0
    cur_vol = float(recent["volume"].sum())
    vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0

    if abs(pct_change) < min_pct:
        return None
    if vol_ratio < min_vol:
        return None

    direction = "вверх" if pct_change > 0 else "вниз"
    urgency   = "высокая" if vol_ratio >= 3.5 else ("средняя" if vol_ratio >= 2.5 else "низкая")

    return {
        "pct_change": round(pct_change, 2),
        "vol_ratio":  round(vol_ratio, 1),
        "direction":  direction,
        "urgency":    urgency,
        "is_up":      pct_change > 0,
        "candles":    3,
        "description": (
            f"Цена {'выросла' if pct_change > 0 else 'упала'} на "
            f"{abs(pct_change):.1f}% за 3 свечи при объёме x{vol_ratio:.1f} от нормы"
        ),
    }

def detect_pump_dump(df: pd.DataFrame, tf: str = "15m") -> dict | None:
    cfg = PUMP_DUMP_CONFIG.get(tf, PUMP_DUMP_CONFIG["15m"])
    threshold = cfg["threshold_pct"]
    lookback  = cfg["lookback"]
    
    vol_ratio_setting = get_bot_settings()["pump_dump_vol_ratio"]
    vol_mult = cfg["vol_mult"] * (vol_ratio_setting / 3.0)

    if len(df) < lookback + 10:
        return None

    recent   = df.tail(lookback + 1)
    baseline = df.tail(30).head(30 - lookback)

    price_now   = float(recent["close"].iloc[-1])
    price_start = float(recent["close"].iloc[0])
    pct_change  = (price_now - price_start) / price_start * 100

    avg_vol = float(baseline["volume"].mean()) if len(baseline) > 0 else 0
    cur_vol = float(recent["volume"].sum())
    vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0

    if abs(pct_change) < threshold or vol_ratio < vol_mult:
        return None

    is_pump = pct_change > 0
    signal_type = "PUMP" if is_pump else "DUMP"
    trade_direction = "SHORT" if is_pump else "LONG"

    entry = price_now
    if is_pump:
        sl  = round(entry * (1 + cfg["sl_pct"] / 100), 4)
        tp1 = round(entry * (1 - cfg["tp1_pct"] / 100), 4)
        tp2 = round(entry * (1 - cfg["tp2_pct"] / 100), 4)
        tp3 = round(entry * (1 - cfg["tp3_pct"] / 100), 4)
    else:
        sl  = round(entry * (1 - cfg["sl_pct"] / 100), 4)
        tp1 = round(entry * (1 + cfg["tp1_pct"] / 100), 4)
        tp2 = round(entry * (1 + cfg["tp2_pct"] / 100), 4)
        tp3 = round(entry * (1 + cfg["tp3_pct"] / 100), 4)

    strength = min(100, int(
        (abs(pct_change) / threshold * 40) +
        (min(vol_ratio, 5) / 5 * 40) +
        (20 if abs(pct_change) > threshold * 1.5 else 0)
    ))

    return {
        "type":       signal_type,
        "direction":  trade_direction,
        "pct_change": round(pct_change, 2),
        "vol_ratio":  round(vol_ratio, 1),
        "strength":   strength,
        "entry":      entry,
        "sl":         sl,
        "tp1":        tp1,
        "tp2":        tp2,
        "tp3":        tp3,
        "candles":    lookback,
    }

_pd_sent: dict[str, float] = {}

async def _scan_pump_dump(tickers: list[str], tf: str,
                          is_futures: bool = False) -> list[dict]:
    results = []
    now = time.time()

    for i in range(0, len(tickers), 6):
        batch = tickers[i:i+6]
        tasks = []
        for code in batch:
            if is_futures:
                info = FUTURES.get(code)
                if info:
                    figi = info[0]
                    # Пропускаем placeholder-FIGI (не заменились при старте)
                    if not figi or figi.startswith("FUT") and len(figi) < 15:
                        logger.debug(f"_scan_pump_dump: skip {code} placeholder FIGI {figi}")
                        continue
                    tasks.append((code, info[1], fetch_candles_tinkoff(
                        figi, TF_MAP[tf][0], 50)))
            else:
                info = MOEX_STOCKS.get(code)
                if info:
                    figi = info[0]
                    tasks.append((code, info[1], fetch_candles_tinkoff(
                        figi, TF_MAP[tf][0], 50)))

        fetched = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)

        for (code, name, _), df in zip(tasks, fetched):
            if isinstance(df, Exception) or df is None or len(df) < 20:
                continue

            pd_signal = detect_pump_dump(df, tf)
            if not pd_signal:
                continue

            last_sent = _pd_sent.get(f"pd_{code}", 0)
            if now - last_sent < 1800:
                continue

            _pd_sent[f"pd_{code}"] = now
            results.append({
                **pd_signal,
                "ticker":      code,
                "name":        name,
                "tf":          tf,
                "is_futures":  is_futures,
            })

        await asyncio.sleep(0.3)

    return results

def format_pump_dump_alert(s: dict) -> str:
    t         = s["ticker"]
    name      = s["name"]
    sig_type  = s["type"]
    direction = s["direction"]
    pct       = s["pct_change"]
    vol_r     = s["vol_ratio"]
    strength  = s["strength"]
    entry     = s["entry"]
    sl        = s["sl"]
    tp1, tp2, tp3 = s["tp1"], s["tp2"], s["tp3"]
    tf        = s["tf"]

    type_e  = "🚀" if sig_type == "PUMP" else "💥"
    dir_e   = "🟥 SHORT" if direction == "SHORT" else "🟩 LONG"
    fut_tag = " [фьюч]" if s.get("is_futures") else ""

    sl_pct  = abs(entry - sl) / entry * 100
    rr      = abs(entry - tp2) / abs(entry - sl) if abs(entry - sl) > 0 else 0

    return (
        f"{type_e} <b>{sig_type} ДЕТЕКТОР - {esc(t)}{fut_tag}</b>\n"
        f"{esc(name)} | {tf}\n\n"
        f"📊 Движение: <b>{pct:+.1f}%</b> за {s['candles']} свечи | Объём: x{vol_r:.1f}\n"
        f"💪 Сила сигнала: {strength}/100\n\n"
        f"<b>Сигнал: {dir_e}</b> (контртренд)\n"
        f"Вход: {entry:,.2f}\n"
        f"SL: {sl:,.2f}  ({sl_pct:.1f}%)\n"
        f"TP1: {tp1:,.2f}  ({abs(entry-tp1)/entry*100:.1f}%)\n"
        f"TP2: {tp2:,.2f}  ({abs(entry-tp2)/entry*100:.1f}%)  R/R {rr:.1f}\n"
        f"TP3: {tp3:,.2f}  ({abs(entry-tp3)/entry*100:.1f}%)\n\n"
        f"<i>⚠️ Контртренд - высокий риск. Используй строгий SL.</i>"
    )

async def cmd_fadd_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wl = load_futures_watchlist()
    added, skipped = [], []
    for code in FUTURES:
        if code not in wl:
            wl.append(code)
            added.append(code)
        else:
            skipped.append(code)
    save_futures_watchlist(wl)
    await update.message.reply_text(
        f"✅ Добавлено: {len(added)}\n"
        f"ℹ️ Уже были: {len(skipped)}\n"
        f"📋 Итого в ватчлисте: {len(wl)} фьючерсов\n\n"
        f"Запустить сканер: /scan_futures",
        parse_mode="HTML")

async def cmd_pump_dump(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    mode = args[0].lower() if args else "all"
    tf   = get_user_state(chat_id)["tf"]

    msg = await update.message.reply_text(
        f"🔍 Ищу памп/дамп [{tf}]...", parse_mode="HTML")

    results = []
    if mode in ("all", "stocks"):
        wl = load_watchlist()
        results += await _scan_pump_dump(wl, tf, is_futures=False)
    if mode in ("all", "futures"):
        fwl = load_futures_watchlist()
        results += await _scan_pump_dump(fwl, tf, is_futures=True)

    if not results:
        await msg.edit_text(
            f"😶 Памп/дамп сигналов не найдено [{tf}]\n"
            f"Порог: {PUMP_DUMP_CONFIG.get(tf, PUMP_DUMP_CONFIG['15m'])['threshold_pct']}% "
            f"за {PUMP_DUMP_CONFIG.get(tf, PUMP_DUMP_CONFIG['15m'])['lookback']} свечи")
        return

    results.sort(key=lambda x: -x["strength"])
    lines = [f"{'🚀' if r['type']=='PUMP' else '💥'} <b>ПАМП/ДАМП [{tf}]</b>  "
             f"{msk_now().strftime('%H:%M')}", ""]
    for r in results[:5]:
        lines.append(format_pump_dump_alert(r))
        lines.append("")

    kb = []
    for r in results:  
        
        key = f"{r['ticker']}_{r['direction']}"[:20]
        _cleanup_pending_trades()
        _pending_trades[key] = {"ticker": r["ticker"], "direction": r["direction"],
                                "entry": r["entry"], "sl": r["sl"],
                                "tp1": r["tp1"], "tp2": r["tp2"], "tp3": r["tp3"],
                                "ts": time.time()}
        kb.append([InlineKeyboardButton(
            f"✅ Войти {r['ticker']} {r['direction']}",
            callback_data=f"enter2_{key}"
        )])

    await msg.edit_text("\n".join(lines), parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(kb) if kb else None)

async def run_pump_dump_broadcast(app, tf: str = "15m"):
    wl  = load_watchlist()
    fwl = load_futures_watchlist()
    all_results = (
        await _scan_pump_dump(wl,  tf, is_futures=False) +
        await _scan_pump_dump(fwl, tf, is_futures=True)
    )
    if not all_results:
        return
    all_results.sort(key=lambda x: -x["strength"])
    for r in all_results[:3]:
        text = format_pump_dump_alert(r)
        for chat_id in SCANNER_CHAT_IDS:
            try:
                await app.bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception as e:
                logger.warning(f"PD broadcast {chat_id}: {e}")

async def cmd_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if args and args[0].lower() == "update":
        msg = await update.message.reply_text("⏳ Обновляю календарь...", parse_mode="HTML")
        try:
            moex_ev = await fetch_moex_dividends()
            cbr_ev  = get_cbr_dates_2026()
            existing = [e for e in load_calendar_events() if e.get("source") == "manual"]
            save_calendar_events(existing + moex_ev + cbr_ev)
            await msg.edit_text(
                f"✅ Календарь обновлён\n"
                f"📅 Отсечки MOEX: {len(moex_ev)}\n"
                f"🏦 Заседания ЦБ: {len(cbr_ev)}\n\n"
                f"Смотреть: /calendar",
                parse_mode="HTML")
        except Exception as e:
            await msg.edit_text(f"❌ Ошибка: {esc(str(e))}")
        return

    upcoming = get_upcoming_events(hours_ahead=72)
    all_ev   = load_calendar_events()

    if not all_ev:
        await update.message.reply_text(
            "📭 Календарь пуст.\n\n"
            "/calendar update - загрузить события с MOEX и ЦБ\n"
            "/calendar_add - добавить вручную",
            parse_mode="HTML")
        return

    lines = ["📅 <b>ЭКОНОМИЧЕСКИЙ КАЛЕНДАРЬ</b>\n"]

    if upcoming:
        lines.append("<b>🔔 Ближайшие (до 72ч):</b>")
        for ev in upcoming[:8]:
            h      = ev["hours_ahead"]
            imp_e  = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}.get(ev.get("impact", "low"), "⚪")
            t_str  = (f"{abs(h)*60:.0f}м назад" if h < 0 else
                      f"через {h*60:.0f} мин ⚡" if h < 1 else
                      f"через {h:.1f} ч" if h < 24 else f"через {h/24:.1f} дн")
            tk_str = f" [{', '.join(ev['tickers'][:3])}]" if ev.get("tickers") else ""
            lines.append(f"{imp_e} {esc(ev['name'])}{tk_str} - {t_str}")

    upcoming_keys = {(e.get("name"), e.get("datetime_utc")) for e in upcoming}
    future = [e for e in all_ev
              if (e.get("name"), e.get("datetime_utc")) not in upcoming_keys
              and e.get("datetime_utc", "") > datetime.now(timezone.utc).isoformat()]
    if future:
        lines.append("\n<b>📋 Далее:</b>")
        for ev in future[:5]:
            try:
                dt = datetime.fromisoformat(ev["datetime_utc"])
                dt_msk = dt.astimezone(timezone(timedelta(hours=3))).strftime("%d.%m %H:%M МСК")
            except Exception:
                dt_msk = ""
            imp_e = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}.get(ev.get("impact","low"), "⚪")
            lines.append(f"{imp_e} {esc(ev['name'])} - {dt_msk}")

    lines.append("\n<i>Обновить: /calendar update | Добавить: /calendar_add</i>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_calendar_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Формат:\n<code>/calendar_add ГГГГ-ММ-ДД ЧЧ:ММ Название [ТИКЕР] [high/medium/low]</code>\n\n"
            "Пример:\n<code>/calendar_add 2026-06-10 11:00 Отчёт_SBER_МСФО SBER high</code>",
            parse_mode="HTML")
        return
    try:
        name   = args[2].replace("_", " ")
        ticker = args[3].upper() if len(args) > 3 else ""
        impact = args[4].lower() if len(args) > 4 and args[4] in ("critical","high","medium","low") else "medium"
        dt_msk = datetime.strptime(f"{args[0]} {args[1]}", "%Y-%m-%d %H:%M")
        dt_utc = dt_msk.replace(tzinfo=timezone(timedelta(hours=3)))
        event  = {
            "name": name, "datetime_utc": dt_utc.isoformat(),
            "impact": impact, "tickers": [ticker] if ticker else [],
            "type": "manual", "source": "manual",
        }
        evs = load_calendar_events()
        evs.append(event)
        save_calendar_events(evs)
        imp_e = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}.get(impact, "⚪")
        await update.message.reply_text(
            f"✅ Добавлено:\n{imp_e} <b>{esc(name)}</b>\n"
            f"📅 {args[0]} {args[1]} МСК\n"
            f"Тикер: {esc(ticker) if ticker else 'весь рынок'}  Импакт: {impact}",
            parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ {esc(str(e))}")

async def cmd_update_figi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Обновление базы FIGI...")
    count, err, updated_list, not_found_list, fut_count = await update_figi_data()
    if err:
        await msg.edit_text(f"❌ Ошибка обновления: {err}")
    else:
        lines = [f"✅ База FIGI обновлена."]
        lines.append(f"📈 Акции: обновлено {len(updated_list)}, фьючерсы: {fut_count}")
        if not_found_list:
            lines.append(f"❌ Не найдены ({len(not_found_list)}): {', '.join(not_found_list)}")
            lines.append("💡 Попробуй добавить алиас в TICKER_ALIASES в коде бота")
        if updated_list:
            lines.append(f"✅ Обновлены: {', '.join(updated_list[:20])}{'...' if len(updated_list) > 20 else ''}")
        # Очистка ватчлиста от мёртвых тикеров
        wl = load_watchlist()
        dead = [t for t in wl if t not in MOEX_STOCKS and t not in FUTURES]
        if dead:
            wl = [t for t in wl if t in MOEX_STOCKS or t in FUTURES]
            save_watchlist(wl)
            lines.append(f"\n🧹 Из ватчлиста удалены мёртвые тикеры: {', '.join(dead)}")
            lines.append("↔️ Они больше не будут мешать сканированию.")
        await msg.edit_text("\n".join(lines))

async def cmd_open_trade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    if len(args) < 7:
        await update.message.reply_text(
            "📝 <b>Открыть сделку:</b>\n"
            "<code>/open_trade TICKER LONG/SHORT ENTRY SL TP1 TP2 TP3</code>\n\n"
            "Пример:\n<code>/open_trade SBER LONG 320.5 316.0 325.0 330.0 337.0</code>\n\n"
            "Или нажми кнопку <b>✅ Войти в сделку</b> в анализе.",
            parse_mode="HTML")
        return
    try:
        ticker = args[0].upper()
        direction = args[1].upper()
        entry = float(args[2])
        sl = float(args[3])
        tp1 = float(args[4])
        tp2 = float(args[5])
        tp3 = float(args[6])
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Неверный формат. Пример: /open_trade SBER LONG 320.5 316.0 325.0 330.0 337.0")
        return
    if ticker not in MOEX_STOCKS:
        await update.message.reply_text(f"❌ Тикер {esc(ticker)} не найден.")
        return
    if direction not in ("LONG", "SHORT"):
        await update.message.reply_text("❌ Направление должно быть LONG или SHORT.")
        return
    if entry <= 0 or sl <= 0:
        await update.message.reply_text("❌ Цена входа и SL должны быть больше нуля.")
        return

    tf = get_user_state(chat_id)["tf"]
    try:
        trade_id = open_trade(ticker, direction, entry, sl, tp1, tp2, tp3, chat_id, tf)
    except ValueError as e:
        await update.message.reply_text(f"❌ Не удалось открыть сделку: {e}")
        return
    direction_e = "🟩 LONG" if direction == "LONG" else "🟥 SHORT"
    risk_pct = abs(entry - sl) / entry * 100

    await update.message.reply_text(
        f"✅ <b>Сделка открыта!</b>\n\n"
        f"<b>{esc(ticker)}</b> {direction_e}\n"
        f"Вход: {entry:,.2f} ₽  |  Риск: {risk_pct:.1f}%\n"
        f"SL: {sl:,.2f} ₽\n"
        f"TP1: {tp1:,.2f} ₽\n"
        f"TP2: {tp2:,.2f} ₽\n"
        f"TP3: {tp3:,.2f} ₽\n\n"
        f"Мониторинг активен. Алерты при достижении уровней или тайм-ауте.\n/trades - все позиции",
        parse_mode="HTML"
    )

async def cmd_close_trade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    if not args:
        await update.message.reply_text(
            "Использование:\n/close_trade SBER - закрыть по тикеру\n/close_trade ALL - закрыть все")
        return

    trades = load_trades()
    target = args[0].upper()

    if target == "ALL":
        open_ids = [k for k, v in trades.items() if v["status"] in ("open", "tp1_hit", "tp2_hit")]
        if not open_ids:
            await update.message.reply_text("📭 Нет открытых сделок.")
            return
        for tid in open_ids:
            figi = MOEX_STOCKS.get(trades[tid]["ticker"], ("",))[0]
            price = await fetch_last_price_tinkoff(figi) if figi else trades[tid]["entry"]
            price = price or trades[tid]["entry"]
            close_trade(tid, "manual", price)
        await update.message.reply_text(f"✅ Закрыто сделок: {len(open_ids)}")
        return

    found = [(k, v) for k, v in trades.items()
             if v["ticker"] == target and v["status"] in ("open", "tp1_hit", "tp2_hit")]
    if not found:
        await update.message.reply_text(f"ℹ️ Нет открытых сделок по {esc(target)}.")
        return

    trade_id, t = found[0]
    figi = MOEX_STOCKS.get(target, ("",))[0]
    price = await fetch_last_price_tinkoff(figi) if figi else t["entry"]
    price = price or t["entry"]
    closed = close_trade(trade_id, "manual", price)
    pnl = closed.get("pnl_pct", 0) if closed else 0
    pnl_e = "📈" if pnl >= 0 else "📉"

    await update.message.reply_text(
        f"🔒 <b>Сделка {esc(target)} закрыта вручную</b>\n"
        f"Цена: {price:,.2f} ₽\n{pnl_e} Результат: {pnl:+.2f}%",
        parse_mode="HTML")

async def _show_trades_history(update: Update, trades: dict):
    """Полный архив закрытых сделок с постраничным выводом."""
    closed = sorted(
        [v for v in trades.values() if v.get("status") == "closed"],
        key=lambda x: x.get("closed_at", ""), reverse=True
    )
    if not closed:
        await update.message.reply_text("📭 Закрытых сделок нет.")
        return

    reason_map = {
        "SL": "🛑 SL", "TP3": "🎯 TP3", "TP2": "🎯 TP2", "TP1": "🎯 TP1",
        "manual": "🔒 ручное", "EOD": "🕐 конец сессии", "Time Exit": "🕐 тайм-аут",
    }
    total_pnl = sum(t.get("pnl_pct", 0) for t in closed)
    wins  = sum(1 for t in closed if t.get("pnl_pct", 0) > 0)
    total = len(closed)

    lines = [
        f"📋 <b>Полный архив сделок</b> ({total} шт.)",
        f"Итого P&L: <b>{total_pnl:+.2f}%</b>  Винрейт: <b>{wins/total*100:.0f}%</b>",
        "",
    ]
    for t in closed[:20]:
        pnl    = t.get("pnl_pct", 0)
        pnl_e  = "📈" if pnl >= 0 else "📉"
        reason = reason_map.get(t.get("close_reason", ""), t.get("close_reason", ""))
        ts_score  = t.get("tech_score", "")
        fs_status = t.get("filter_status", "")
        ai_sum    = t.get("ai_summary", "")
        lines.append(
            f"{pnl_e} <b>{esc(t['ticker'])}</b> {t['direction']}  "
            f"{reason}  <b>{pnl:+.2f}%</b>  "
            f"<i>{t.get('closed_at', '')[:16].replace('T', ' ')}</i>"
        )
        if ts_score:
            lines.append(f"   ТА: {ts_score}/100  ФС: {esc(fs_status)}")
        if ai_sum:
            lines.append(f"   🤖 {esc(ai_sum[:60])}")
        lines.append("")
    if total > 20:
        lines.append(f"<i>... и ещё {total - 20} сделок. Используй /stats для полной статистики.</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает текущие настройки бота с кнопками для изменения каждой."""
    settings = get_bot_settings()
    lines = ["🔧 <b>Настройки бота</b>", "<i>Пороги, влияющие на сигналы</i>", ""]
    kb = []
    for key, value in settings.items():
        label = BOT_SETTINGS_LABELS.get(key, key)
        default = BOT_SETTINGS_DEFAULTS.get(key)
        changed_mark = " ✏️" if value != default else ""
        lines.append(f"• {label}: <b>{value}</b>{changed_mark}")
        kb.append([InlineKeyboardButton(f"✏️ {label}", callback_data=f"setcfg_{key}")])
    kb.append([InlineKeyboardButton("↩️ Сбросить всё к дефолтам", callback_data="setcfg_reset")])
    lines.append("\n<i>✏️ - значение изменено от дефолта</i>")
    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb)
    )

async def cmd_outcome_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика по исходам ВСЕХ сигналов (не только взятых в реальную сделку)."""
    entries = _lrange_log(RK_OUTCOME_STATS, 500)
    if not entries:
        await update.message.reply_text(
            "📭 Пока нет разрешённых исходов сигналов.\n"
            "Проверка идёт автоматически каждые 15 минут после появления сигнала."
        )
        return

    total = len(entries)
    tp_hits  = [e for e in entries if "tp" in e.get("outcome", "")]
    sl_hits  = [e for e in entries if e.get("outcome") == "sl_hit"]
    expired  = [e for e in entries if e.get("outcome") == "expired_no_move"]

    win_rate = len(tp_hits) / total * 100 if total else 0
    avg_pnl  = sum(e.get("pnl_pct", 0) for e in entries) / total if total else 0

    lines = [
        f"📊 <b>Статистика по ВСЕМ сигналам</b> ({total} шт.)",
        f"<i>Включает сигналы которые не были взяты в реальную сделку</i>",
        "",
        f"🎯 Достигли TP: {len(tp_hits)} ({len(tp_hits)/total*100:.0f}%)",
        f"🛑 Выбили SL: {len(sl_hits)} ({len(sl_hits)/total*100:.0f}%)",
        f"⏱ Протухли без движения: {len(expired)} ({len(expired)/total*100:.0f}%)",
        "",
        f"Средний P&L по всем сигналам: <b>{avg_pnl:+.2f}%</b>",
        "",
    ]

    by_status: dict[str, list] = {}
    for e in entries:
        by_status.setdefault(e.get("filter_status", "?"), []).append(e)

    lines.append("📋 <b>По статусу фильтра:</b>")
    for status, items in sorted(by_status.items(), key=lambda x: -len(x[1])):
        s_tp = len([i for i in items if "tp" in i.get("outcome", "")])
        s_avg = sum(i.get("pnl_pct", 0) for i in items) / len(items)
        lines.append(f"  {status}: {len(items)} сигн., винрейт {s_tp/len(items)*100:.0f}%, avg {s_avg:+.2f}%")

    has_mfe_mae = any("mfe_pct" in e for e in entries)
    if has_mfe_mae:
        avg_mfe = sum(e.get("mfe_pct", 0) for e in entries) / total
        avg_mae = sum(e.get("mae_pct", 0) for e in entries) / total

        lines.append("")
        lines.append("📈 <b>MFE/MAE (макс. движение по пути):</b>")
        lines.append(f"  Средний MFE (лучшая точка): <b>{avg_mfe:+.2f}%</b>")
        lines.append(f"  Средний MAE (худшая точка): <b>{avg_mae:+.2f}%</b>")

        losers = [e for e in entries if e.get("outcome") == "sl_hit"]
        if losers:
            avg_mfe_losers = sum(e.get("mfe_pct", 0) for e in losers) / len(losers)
            if avg_mfe_losers > 0.3:
                lines.append(
                    f"  ⚠️ На сделках с SL цена в среднем заходила в плюс "
                    f"на {avg_mfe_losers:+.2f}% перед разворотом - "
                    f"возможно стоит фиксировать частичную прибыль раньше."
                )

        winners = [e for e in entries if "tp" in e.get("outcome", "")]
        if winners:
            avg_mae_winners = sum(e.get("mae_pct", 0) for e in winners) / len(winners)
            if avg_mae_winners < -0.5:
                lines.append(
                    f"  ℹ️ На прибыльных сделках цена в среднем уходила в минус "
                    f"на {avg_mae_winners:.2f}% перед разворотом к TP - "
                    f"текущий SL даёт этому пространство."
                )

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_sectors(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает все активные секторальные геополитические/событийные модификаторы."""
    modifiers = _load_sector_modifiers()
    if not modifiers:
        await update.message.reply_text(
            "🌍 <b>Секторальные модификаторы</b>\n\n"
            "Сейчас нет активных геополитических/событийных факторов "
            "по секторам. Технические сигналы не корректируются.",
            parse_mode="HTML"
        )
        return

    lines = ["🌍 <b>Активные секторальные модификаторы</b>", ""]
    now = time.time()
    for sector, mod in sorted(modifiers.items(), key=lambda x: -x[1]["strength"]):
        direction_e  = "📈" if mod["direction"] == "bullish" else "📉"
        direction_ru = "бычий" if mod["direction"] == "bullish" else "медвежий"
        days_left = round((mod["expires_at"] - now) / 86400, 1)
        lines.append(
            f"{direction_e} <b>{esc(sector)}</b> - {direction_ru} "
            f"(сила {mod['strength']}/10, ещё ~{days_left} дн.)"
        )
        lines.append(f"   {esc(mod['reason'])}")
        lines.append("")

    lines.append("<i>Сигналы против этих факторов автоматически понижаются "
                "до WEAK с предупреждением в тексте анализа.</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика по закрытым сделкам - винрейт, средний P&L, лучший/худший тикер."""
    trades = load_trades()
    closed = [v for v in trades.values() if v.get("status") == "closed"]

    if not closed:
        await update.message.reply_text("📭 Закрытых сделок пока нет.")
        return

    period_days = None
    if context.args:
        arg = context.args[0].lower().replace("d", "").replace("д", "")
        if arg.isdigit():
            period_days = int(arg)

    if period_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=period_days)
        closed = [t for t in closed
                  if t.get("closed_at") and datetime.fromisoformat(t["closed_at"]).replace(tzinfo=timezone.utc) >= cutoff]
        if not closed:
            await update.message.reply_text(f"📭 Нет закрытых сделок за последние {period_days} дн.")
            return

    total      = len(closed)
    wins       = [t for t in closed if t.get("pnl_pct", 0) > 0]
    losses     = [t for t in closed if t.get("pnl_pct", 0) <= 0]
    win_rate   = len(wins) / total * 100 if total else 0
    avg_pnl    = sum(t.get("pnl_pct", 0) for t in closed) / total if total else 0
    avg_win    = sum(t.get("pnl_pct", 0) for t in wins) / len(wins) if wins else 0
    avg_loss   = sum(t.get("pnl_pct", 0) for t in losses) / len(losses) if losses else 0
    total_pnl  = sum(t.get("pnl_pct", 0) for t in closed)

    by_ticker: dict[str, list] = {}
    for t in closed:
        by_ticker.setdefault(t["ticker"], []).append(t.get("pnl_pct", 0))
    ticker_avg = {tk: sum(v)/len(v) for tk, v in by_ticker.items()}
    best_ticker  = max(ticker_avg.items(), key=lambda x: x[1]) if ticker_avg else None
    worst_ticker = min(ticker_avg.items(), key=lambda x: x[1]) if ticker_avg else None

    by_reason: dict[str, int] = {}
    for t in closed:
        reason = t.get("close_reason", "-")
        by_reason[reason] = by_reason.get(reason, 0) + 1

    longs  = [t for t in closed if t.get("direction") == "LONG"]
    shorts = [t for t in closed if t.get("direction") == "SHORT"]
    long_wr  = (sum(1 for t in longs  if t.get("pnl_pct",0) > 0) / len(longs)  * 100) if longs  else 0
    short_wr = (sum(1 for t in shorts if t.get("pnl_pct",0) > 0) / len(shorts) * 100) if shorts else 0

    period_label = f"за {period_days} дн." if period_days else "за всё время"

    lines = [
        f"📊 <b>СТАТИСТИКА СДЕЛОК</b> ({period_label})",
        "",
        f"Всего закрыто: <b>{total}</b>",
        f"Винрейт: <b>{win_rate:.1f}%</b> ({len(wins)}/{total})",
        f"Средний P&L: <b>{avg_pnl:+.2f}%</b>",
        f"Суммарный P&L: <b>{total_pnl:+.2f}%</b>",
        "",
        f"Средний выигрыш: <span>{avg_win:+.2f}%</span>",
        f"Средний убыток: <span>{avg_loss:+.2f}%</span>",
    ]

    if longs or shorts:
        lines += ["", "<b>По направлению:</b>"]
        if longs:
            lines.append(f" LONG: {len(longs)} сделок, винрейт {long_wr:.0f}%")
        if shorts:
            lines.append(f" SHORT: {len(shorts)} сделок, винрейт {short_wr:.0f}%")

    if best_ticker and worst_ticker and len(by_ticker) > 1:
        lines += ["", "<b>По тикерам:</b>"]
        lines.append(f" Лучший: {esc(best_ticker[0])} ({best_ticker[1]:+.2f}% сред.)")
        lines.append(f" Худший: {esc(worst_ticker[0])} ({worst_ticker[1]:+.2f}% сред.)")

    if by_reason:
        lines += ["", "<b>Причины закрытия:</b>"]
        for reason, cnt in sorted(by_reason.items(), key=lambda x: -x[1]):
            lines.append(f" {esc(reason)}: {cnt}")

    lines += ["", "<i>/stats 7d - за неделю, /stats 30d - за месяц</i>"]

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подробный журнал сделок с разбором по каждой позиции."""
    trades = load_trades()
    args = context.args

    if args and args[0].lower() == "history":
        await _show_trades_history(update, trades)
        return

    open_t = {k: v for k, v in trades.items() if v["status"] in ("open", "tp1_hit", "tp2_hit")}
    closed_t = sorted(
        [v for v in trades.values() if v["status"] == "closed"],
        key=lambda x: x.get("closed_at", ""), reverse=True
    )[:5]

    if not open_t and not closed_t:
        await update.message.reply_text(
            "📭 Нет сделок.\n\nОткрыть: /open_trade TICKER LONG/SHORT ENTRY SL TP1 TP2 TP3")
        return

    msg = await update.message.reply_text(
        "⏳ Загружаю текущие цены...", parse_mode="HTML")

    lines = []
    total_pnl = 0.0

    if open_t:
        lines.append(f"📂 <b>Открытые позиции ({len(open_t)}):</b>")
        for k, t in open_t.items():
            ticker    = t["ticker"]
            entry     = t["entry"]
            direction = t["direction"]
            sl  = t.get("sl",  0)
            tp1 = t.get("tp1", 0)
            tp2 = t.get("tp2", 0)
            tp3 = t.get("tp3", 0)

            cur_price = None
            src = MOEX_STOCKS.get(ticker) or FUTURES.get(ticker)
            if src:
                figi = src[0]
                cur_price = await fetch_last_price_tinkoff(figi)
            if cur_price and entry:
                if direction == "LONG":
                    upnl_pct = (cur_price - entry) / entry * 100
                else:
                    upnl_pct = (entry - cur_price) / entry * 100
                total_pnl += upnl_pct
                pnl_e = "📈" if upnl_pct >= 0 else "📉"

                dist_to_tp1 = abs(cur_price - tp1) / abs(entry - tp1) * 100 if tp1 and entry != tp1 else 0
                progress = f" → TP1 {100-dist_to_tp1:.0f}%" if dist_to_tp1 < 100 else " ✅TP1"

                status_e = {"open": "🔵", "tp1_hit": "🟡", "tp2_hit": "🟠"}.get(t["status"], "⚪")
                sl_note  = " (б/у)" if t.get("sl_moved_to_be") else (" (на TP1)" if t.get("sl_moved_to_tp1") else "")

                fl_tp1 = "✅" if t["status"] in ("tp1_hit", "tp2_hit") else ("🎯" if tp1 else "")
                fl_tp2 = "✅" if t["status"] == "tp2_hit" else ("🎯" if tp2 else "")
                fl_tp3 = "🎯" if tp3 else ""

                ts_score = t.get("tech_score", "")
                fs_status = t.get("filter_status", "CONFIRMED")
                ai_sum   = t.get("ai_summary", "")

                lines += [
                    f"{status_e} <b>{esc(ticker)}</b> {'🟩 LONG' if direction == 'LONG' else '🟥 SHORT'}",
                    f"  Вход: {entry:,.2f} → Сейчас: <b>{cur_price:,.2f}</b>  {pnl_e} <b>{upnl_pct:+.2f}%</b>{progress}",
                    f"  SL: {sl:,.2f}{sl_note}",
                    f"  {fl_tp1} TP1: {tp1:,.2f}  {fl_tp2} TP2: {tp2:,.2f}  {fl_tp3} TP3: {tp3:,.2f}",
                ]
                if ts_score:
                    lines.append(f"  📊 ТА: {ts_score}/100  ФС: {esc(fs_status)}")
                if ai_sum:
                    lines.append(f"  🤖 {esc(ai_sum[:80])}")
                lines.append(f"  <code>/close_trade {ticker}</code>")
                lines.append("")

            else:
                lines += [
                    format_trade_status(t),
                    f"  <code>/close_trade {ticker}</code>",
                    "",
                ]

        if len(open_t) > 1:
            total_e = "📈" if total_pnl >= 0 else "📉"
            lines.append(f"<b>Итого нереализованный PnL: {total_e} {total_pnl:+.2f}%</b>")
            lines.append("")

    _reason_map = {
        "SL":        "🛑 SL",
        "TP3":       "🎯 TP3",
        "TP2":       "🎯 TP2",
        "TP1":       "🎯 TP1",
        "manual":    "🔒 ручное",
        "EOD":       "🕐 конец сессии",
        "Time Exit": "🕐 тайм-аут",
    }

    if closed_t:
        lines.append("📋 <b>Последние закрытые:</b>")
        for t in closed_t:
            pnl   = t.get("pnl_pct", 0)
            pnl_e = "📈" if pnl >= 0 else "📉"
            reason = _reason_map.get(t.get("close_reason", ""), t.get("close_reason", ""))
            lines.append(
                f"{pnl_e} <b>{esc(t['ticker'])}</b> {t['direction']}  "
                f"{reason}  {pnl:+.2f}%  "
                f"<i>{t.get('closed_at', '')[:16].replace('T', ' ')}</i>"
            )

    if closed_t:
        lines.append("")
        lines.append("📊 <i>Полный архив: /trades history</i>")

    await msg.edit_text("\n".join(lines), parse_mode="HTML")

async def cmd_export_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выгружает лог сигналов в CSV для анализа эффективности AI/скоринга."""
    args  = context.args
    limit = 1000
    if args and args[0].isdigit():
        limit = min(5000, int(args[0]))

    entries = _lrange_log(RK_SIGNAL_LOG, limit)
    if not entries:
        await update.message.reply_text(
            "📭 Лог сигналов пуст.\n\n"
            "Записи появляются автоматически при каждом /analyze и при работе сканера. "
            "Подожди немного и попробуй снова."
        )
        return

    msg = await update.message.reply_text(
        f"⏳ Выгружаю {len(entries)} записей в CSV...")

    entries = list(reversed(entries))

    fieldnames = [
        "ts", "ticker", "tf", "price", "tech_signal", "tech_score",
        "final_signal", "filter_status", "event_type", "event_weight",
        "ai_summary", "ai_skip_reason", "macd_div", "htf_trend", "imoex_regime",
        "candle_progress_pct", "daily_atr_progress_pct",
    ]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for e in entries:
        writer.writerow(e)

    csv_bytes = output.getvalue().encode("utf-8-sig")  
    csv_file  = io.BytesIO(csv_bytes)
    ts_str    = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    csv_file.name = f"signal_log_{ts_str}.csv"

    await msg.delete()
    await update.message.reply_document(
        document=csv_file,
        filename=csv_file.name,
        caption=(
            f"📊 <b>Лог сигналов</b> - {len(entries)} записей\n\n"
            f"Колонки: тикер, скор, итоговый сигнал, статус AI-фильтра "
            f"(CONFIRMED/WEAK/BLOCKED), событие и вес от AI, цена на момент сигнала.\n\n"
            f"Можно открыть в Google Sheets для анализа: считай средний "
            f"исход по группам filter_status или event_weight."
        ),
        parse_mode="HTML",
    )

def _format_scan_row(s: dict) -> str:
    ticker = s["ticker"]
    name   = s["name"]
    price  = s["price"]
    score  = s["tech_score"]
    sector = s["sector"]
    regime = s["regime"]["label"]
    reason = s["tech_reasons"][0] if s["tech_reasons"] else "-"
    sig    = s["tech_signal"]
    na     = s["news_ai"]
    fs     = na.get("filter_status", "NO_SIGNAL")
    ew     = na.get("event_weight", 0)
    fs_e   = {"CONFIRMED": "✅", "WEAK": "🟡", "WATCH": "👀",
              "BLOCKED": "🚫", "NEWS_ONLY": "📢"}.get(fs, "⚪")
    ew_s   = f"+{ew}" if ew > 0 else str(ew)

    sl_tp  = s.get("sl_tp", {})
    sl     = sl_tp.get("sl", 0)
    tp1    = sl_tp.get("tp1", 0)
    tp2    = sl_tp.get("tp2", 0)
    tp3    = sl_tp.get("tp3", 0)
    rr     = sl_tp.get("rr_ratio", 0)
    risk   = sl_tp.get("risk_pct", 0)

    lines = [
        f"<b>{esc(ticker)}</b> - {esc(name)} <i>({esc(sector)})</i>",
        f" 💰 {price:,.2f} ₽ | Скор: {score}/100 | {esc(regime)}",
        f" ⚙️ {esc(reason)}",
        f" {fs_e} Новость: {fs} [{ew_s}]",
    ]

    if sl_tp and sl and tp1:
        dir_e = "🟩" if "LONG" in sig else "🟥"
        lines.append(
            f" {dir_e} SL: {sl:,.2f} ({risk:.1f}%) | "
            f"TP1: {tp1:,.2f} | TP2: {tp2:,.2f} | TP3: {tp3:,.2f} | R/R {rr:.1f}"
        )
    else:
        lines.append(" ⚠️ Уровни недоступны")

    daily_atr_note = s.get("daily_atr_progress_note", "")
    if daily_atr_note:
        lines.append(f" {daily_atr_note}")

    return "\n".join(lines)

_API_SEMAPHORE: asyncio.Semaphore | None = None

def _get_semaphore() -> asyncio.Semaphore:
    global _API_SEMAPHORE
    if _API_SEMAPHORE is None:
        _API_SEMAPHORE = asyncio.Semaphore(3)
    return _API_SEMAPHORE

async def _analyze_with_semaphore(ticker: str, tf: str, mode_cfg: dict) -> dict | None:
    async with _get_semaphore():
        for attempt in range(3):
            try:
                result = await analyze_stock(ticker, tf, mode_cfg)
                return result
            except Exception as e:
                err_str = str(e)
                if "429" in err_str or "Too Many Requests" in err_str:
                    jitter = __import__("random").uniform(0, 1)
                    wait = 2 ** attempt + jitter
                    logger.warning(f"Rate limit {ticker}, retry {attempt+1} in {wait:.1f}s")
                    await asyncio.sleep(wait)
                else:
                    logger.debug(f"analyze {ticker}: {e}")
                    return None
        return None
async def _run_scan(tickers: list[str], tf: str, mode_cfg: dict,
                    progress_cb=None, exclude_low_liquidity: bool = False) -> tuple[list, list, list, dict]:
    long_sigs, short_sigs, watch_sigs = [], [], []
    # Фильтруем только известные боту тикеры (отсеиваем мёртвые/невалидные)
    valid_tickers = [t for t in tickers if t in MOEX_STOCKS or t in FUTURES]
    skipped = len(tickers) - len(valid_tickers)
    if skipped:
        logger.warning(f"_run_scan: {skipped} тикеров пропущено (нет в MOEX_STOCKS/FUTURES)")
    tickers = valid_tickers
    total = len(tickers)
    failed_count = 0

    tasks = [_analyze_with_semaphore(t, tf, mode_cfg) for t in tickers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, r in enumerate(results):
        if isinstance(r, Exception) or not r or "error" in r:
            failed_count += 1
            continue
        sig = r["tech_signal"]
        if "Время входа вышло" in r.get("final_signal", ""):
            continue
        
        if exclude_low_liquidity and r.get("liquidity_tier") == "low":
            continue
        if sig == "🟩 LONG":
            long_sigs.append(r)
        elif sig == "🟥 SHORT/ВЫХОД":
            short_sigs.append(r)
        elif r["tech_score"] >= 52:
            watch_sigs.append(r)
        if progress_cb and i > 0 and i % 10 == 0:
            await progress_cb(i, total)

    long_sigs.sort(key=lambda x: -x["tech_score"])
    short_sigs.sort(key=lambda x: -x["tech_score"])
    scan_stats = {
        "total": total,
        "failed": failed_count,
        "failed_pct": (failed_count / total * 100) if total else 0,
    }
    return long_sigs, short_sigs, watch_sigs, scan_stats

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
    tf = get_user_state(chat_id)["tf"]
    wl = load_watchlist()
    if not wl:
        await update.message.reply_text("Ватчлист пуст. Добавьте тикеры через /add или /add_all")
        return

    msg = await update.message.reply_text(f"🔍 Сканирую рынок [{tf}]... Найдено: {len(wl)} активов.", parse_mode="HTML")

    async def progress(done, total):
        try:
            await msg.edit_text(f"🔍 Сканирование рынка... {done}/{total} инструментов [{tf}]", parse_mode="HTML")
        except Exception:
            pass

    long_sigs, short_sigs, watch_sigs, scan_stats = await _run_scan(wl, tf, mode_cfg, progress)
    ts = msk_now().strftime("%d.%m.%Y %H:%M")

    if scan_stats["failed_pct"] >= 50:
        await msg.edit_text(
            f"⚠️ <b>Не удалось получить данные по {scan_stats['failed']} из "
            f"{scan_stats['total']} тикеров.</b>\n"
            f"Похоже на сбой сети или API Tinkoff - попробуй ещё раз через минуту.\n"
            f"<i>{ts}</i>",
            parse_mode="HTML"
        )
        return

    if not long_sigs and not short_sigs:
        await msg.edit_text(f"😶 <b>Интрадей-сигналов на таймфрейме {tf} не обнаружено.</b>\n<i>{ts}</i>", parse_mode="HTML")
        return

    lines = [
        f"🔍 <b>СКАНЕР РЫНКА MOEX</b> | {esc(mode_cfg['label'])} | {tf}",
        f"<i>Интрадей-сессия | {ts}</i>",
        "",
    ]
    if long_sigs:
        lines.append("🟩 <b>СИГНАЛЫ НА ПОКУПКУ:</b>")
        for s in long_sigs[:5]:
            lines.append(_format_scan_row(s))
            lines.append("")
    if short_sigs:
        lines.append("🟥 <b>СИГНАЛЫ НА ВЫХОД:</b>")
        for s in short_sigs[:5]:
            lines.append(_format_scan_row(s))
            lines.append("")

    kb = []
    for s in (long_sigs + short_sigs):  
        sl_tp     = s.get("sl_tp", {})
        direction = "LONG" if "LONG" in s["tech_signal"] else "SHORT"
        row = [InlineKeyboardButton(
            f"📊 {s['ticker']}", callback_data=f"analyze_{s['ticker']}_{tf}")]
        if sl_tp and sl_tp.get("sl"):
            p  = s["price"]; sl = sl_tp.get("sl",0)
            t1 = sl_tp.get("tp1",0); t2 = sl_tp.get("tp2",0); t3 = sl_tp.get("tp3",0)
            key = f"{s['ticker']}_{direction}_{int(time.time())}"[-20:]
            _cleanup_pending_trades()
            _pending_trades[key] = {"ticker": s["ticker"], "direction": direction,
                                    "entry": p, "sl": sl, "tp1": t1, "tp2": t2, "tp3": t3,
                                    "ts": time.time()}
            row.append(InlineKeyboardButton(
                f"✅ {direction}",
                callback_data=f"enter2_{key}"
            ))
        kb.append(row)

    await msg.edit_text("\n".join(lines), parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(kb) if kb else None)

async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cur_mode = get_user_state(chat_id)["mode"]
    kb = [[
        InlineKeyboardButton(
            f"{'✅ ' if m == cur_mode else ''}{TRADE_MODES[m]['label']}",
            callback_data=f"mode_{m}"
        ) for m in TRADE_MODES
    ]]
    await update.message.reply_text(
        f"⚙️ Режим чувствительности: <b>{TRADE_MODES[cur_mode]['label']}</b>\n\n"
        "LOW - жесткие условия фильтрации\n"
        "MID - сбалансированные параметры (рекомендуется)\n"
        "HARD - импульсный вход для интрадея",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb),
    )

async def cmd_tf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cur_tf = get_user_state(chat_id)["tf"]
    tfs = ["5m", "15m", "1h", "4h", "1d"]
    kb = [[
        InlineKeyboardButton(
            f"{'✅ ' if t == cur_tf else ''}{t}",
            callback_data=f"tf_{t}"
        ) for t in tfs
    ]]
    await update.message.reply_text(
        f"⏱ Рабочий таймфрейм: <b>{cur_tf}</b>\n"
        "Фьючерсы не переносятся. Акции в плюсе могут переноситься на следующую сессию.",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb),
    )

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id

    if data.startswith("mode_"):
        await query.answer()
        mode = data.split("_")[1]
        set_user_state(chat_id, mode=mode)
        await query.edit_message_text(f"✅ Режим изменен на: <b>{TRADE_MODES[mode]['label']}</b>", parse_mode="HTML")

    elif data == "setcfg_reset":
        await query.answer()
        reset_bot_settings()
        await query.edit_message_text(
            "↩️ Все настройки сброшены к дефолтным значениям.\n"
            "Открой /settings чтобы посмотреть текущие."
        )

    elif data.startswith("setcfg_"):
        await query.answer()
        key = data[len("setcfg_"):]
        if key not in BOT_SETTINGS_DEFAULTS:
            await query.edit_message_text("❌ Неизвестная настройка.")
        else:
            label   = BOT_SETTINGS_LABELS.get(key, key)
            current = get_bot_settings()[key]
            default = BOT_SETTINGS_DEFAULTS[key]
            context.chat_data["awaiting_setting"]    = key
            context.chat_data["awaiting_setting_ts"] = time.time()
            await query.edit_message_text(
                f"✏️ <b>{label}</b>\n"
                f"Текущее значение: <b>{current}</b> (дефолт: {default})\n\n"
                f"Отправь новое числовое значение в чат в течение 5 минут.",
                parse_mode="HTML"
            )

    elif data.startswith("tf_"):
        await query.answer()
        tf = data.split("_")[1]
        set_user_state(chat_id, tf=tf)
        await query.edit_message_text(f"✅ Таймфрейм: <b>{tf}</b>", parse_mode="HTML")

    elif data.startswith("news_"):
        await query.answer()
        ticker = data.split("_")[1]
        await query.edit_message_text(f"⏳ Загружаю новости для {ticker}...", parse_mode="HTML")
        sector = MOEX_STOCKS.get(ticker, ("", "", ""))[2]
        news = await fetch_russian_news(ticker, sector)
        lines = [f"📰 <b>События - {ticker}</b>\n"]
        for it in news[:5]:
            lines.append(f"⚪ {it['title'][:120]}\n")
        await query.edit_message_text("\n".join(lines), parse_mode="HTML")

    elif data.startswith("analyze_"):
        await query.answer()
        parts = data.split("_")
        ticker = parts[1]
        tf = parts[2] if len(parts) > 2 else DEFAULT_TF
        await query.edit_message_text(f"⏳ Перерасчет {ticker} [{tf}]...", parse_mode="HTML")
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
        result = await analyze_stock(ticker, tf, mode_cfg)
        await query.edit_message_text(format_analysis(result), parse_mode="HTML")

    elif data.startswith("wl_add_"):
        ticker = data.split("_", 2)[2]
        ok, msg = add_to_watchlist(ticker)
        await query.answer(msg.replace("<b>", "").replace("</b>", "")[:200], show_alert=True)

    elif data.startswith("fut_"):
        await query.answer()
        parts = data.split("_")
        code = parts[1]
        tf_  = parts[2] if len(parts) > 2 else DEFAULT_TF
        await query.edit_message_text(
            f"⏳ Анализирую фьючерс {esc(code)} [{tf_}]...", parse_mode="HTML")
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
        result = await analyze_futures(code, tf_, mode_cfg)
        await query.edit_message_text(format_analysis(result), parse_mode="HTML")

    elif data.startswith("enter2_"):
        key = data[7:]  
        td = _pending_trades.get(key)
        if not td:
            await query.answer("Данные устарели, запроси сигнал заново", show_alert=True)
            return
        ticker    = td["ticker"]
        direction = td["direction"]
        entry     = td["entry"]
        sl        = td["sl"]
        tp1       = td["tp1"]
        tp2       = td["tp2"]
        tp3       = td["tp3"]
        tf = get_user_state(chat_id)["tf"]
        try:
            trade_id = open_trade(ticker, direction, entry, sl, tp1, tp2, tp3, chat_id, tf)
        except ValueError as e:
            await query.answer(f"❌ Ошибка: {e}", show_alert=True)
            return
        risk_pct = abs(entry - sl) / entry * 100 if entry else 0
        direction_e = "🟩 LONG" if direction == "LONG" else "🟥 SHORT"
        await query.answer("✅ Сделка открыта!", show_alert=True)
        await query.message.reply_text(
            f"✅ <b>Сделка открыта!</b>\n\n"
            f"<b>{esc(ticker)}</b> {direction_e}\n"
            f"Вход: {entry:,.2f}  |  Риск: {risk_pct:.1f}%\n"
            f"SL: {sl:,.2f}\nTP1: {tp1:,.2f} → SL в безубыток\n"
            f"TP2: {tp2:,.2f} → SL на TP1\nTP3: {tp3:,.2f} → Закрытие\n\n"
            f"Алерты придут автоматически.\n/trades - все позиции",
            parse_mode="HTML"
        )

    elif data.startswith("recalc_"):
        
        parts = data.split("_")
        try:
            ticker    = parts[1]
            direction = parts[2]
        except IndexError:
            await query.answer("Ошибка данных", show_alert=True)
            return

        await query.answer("🔄 Пересчитываю...")
        tf = get_user_state(chat_id)["tf"]
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]

        try:
            fresh = await analyze_stock(ticker, tf, mode_cfg)
        except Exception as e:
            logger.warning(f"recalc_ analyze_stock error for {ticker}: {e}")
            fresh = None

        if not fresh or "error" in fresh:
            await query.message.reply_text(
                f"❌ Не удалось пересчитать {ticker} - {fresh.get('error', 'нет данных') if fresh else 'нет данных'}.\n"
                f"Попробуй /analyze {ticker} вручную."
            )
            return

        fresh_sl_tp = fresh.get("sl_tp", {})
        fresh_signal = fresh.get("tech_signal", "")
        fresh_direction = "LONG" if "LONG" in fresh_signal else ("SHORT" if "SHORT" in fresh_signal else "")

        if not fresh_sl_tp or fresh_direction != direction:
            await query.message.reply_text(
                f"ℹ️ <b>{ticker}</b>: свежий анализ больше не даёт сигнал {direction}.\n"
                f"Текущий сигнал: {esc(fresh_signal)}\n\n"
                f"Открой /analyze {ticker} чтобы увидеть полную картину.",
                parse_mode="HTML"
            )
            return

        new_price = fresh.get("price", 0)
        new_sl  = fresh_sl_tp.get("sl", 0)
        new_tp1 = fresh_sl_tp.get("tp1", 0)
        new_tp2 = fresh_sl_tp.get("tp2", 0)
        new_tp3 = fresh_sl_tp.get("tp3", 0)
        new_risk = abs(new_price - new_sl) / new_price * 100 if new_price else 0
        new_rr   = abs(new_tp1 - new_price) / abs(new_price - new_sl) if new_price != new_sl else 0

        await query.message.reply_text(
            f"🔄 <b>Пересчитано от текущей цены - {ticker}</b>\n\n"
            f"Цена сейчас: <b>{new_price:,.2f} ₽</b>\n"
            f"Скор: {fresh.get('tech_score', 0)}/100\n\n"
            f"Новый SL: {new_sl:,.2f} ₽ (риск {new_risk:.1f}%)\n"
            f"TP1: {new_tp1:,.2f} ₽\n"
            f"TP2: {new_tp2:,.2f} ₽\n"
            f"TP3: {new_tp3:,.2f} ₽ (R/R {new_rr:.1f})\n\n"
            f"<i>Уровни найдены заново от реальной цены рынка, "
            f"не сдвинуты арифметически от старого сигнала.</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                f"✅ Войти по {new_price:,.2f} ₽",
                callback_data=f"enterforce_{ticker}_{direction}_{new_price:.2f}_{new_sl:.2f}_{new_tp1:.2f}_{new_tp2:.2f}_{new_tp3:.2f}"
            )]])
        )

    elif data.startswith("enterforce_"):
        
        parts = data.split("_")
        try:
            ticker    = parts[1]
            direction = parts[2]
            entry     = float(parts[3])
            sl        = float(parts[4])
            tp1       = float(parts[5])
            tp2       = float(parts[6])
            tp3       = float(parts[7])
        except (IndexError, ValueError):
            await query.answer("Ошибка данных", show_alert=True)
            return

        tf = get_user_state(chat_id)["tf"]
        meta          = context.chat_data.get(f"last_analysis_{ticker}", {})
        tech_score    = meta.get("tech_score", 0)
        filter_status = meta.get("filter_status", "")
        anomaly       = meta.get("anomaly", "")
        ai_summary    = meta.get("ai_summary", "MANUAL (цена устарела, вход подтверждён вручную)")

        try:
            open_trade(
                ticker, direction, entry, sl, tp1, tp2, tp3, chat_id, tf,
                tech_score=tech_score, filter_status=filter_status,
                anomaly=anomaly, ai_summary=ai_summary,
            )
        except ValueError as e:
            await query.answer(f"❌ Ошибка: {e}", show_alert=True)
            return

        risk_pct    = abs(entry - sl) / entry * 100 if entry else 0
        direction_e = "🟩 LONG" if direction == "LONG" else "🟥 SHORT"

        await query.answer("✅ Сделка открыта по старой цене", show_alert=True)
        await query.edit_message_text(
            f"✅ <b>Сделка открыта (вход по цене сигнала)!</b>\n\n"
            f"<b>{esc(ticker)}</b> {direction_e}\n"
            f"Вход: {entry:,.2f} ₽  |  Риск: {risk_pct:.1f}%\n"
            f"SL: {sl:,.2f} ₽\n"
            f"TP1: {tp1:,.2f} ₽ → SL в безубыток\n"
            f"TP2: {tp2:,.2f} ₽ → SL на TP1\n"
            f"TP3: {tp3:,.2f} ₽ → Закрытие\n\n"
            f"<i>⚠️ Вход подтверждён по устаревшей цене вручную.</i>\n\n"
            f"Алерты придут автоматически.\n/trades - все позиции",
            parse_mode="HTML"
        )

    elif data.startswith("enter_"):
        parts = data.split("_")
        try:
            ticker    = parts[1]
            direction = parts[2]
            entry     = float(parts[3])  
            sl        = float(parts[4])
            tp1       = float(parts[5])
            tp2       = float(parts[6])
            tp3       = float(parts[7])
        except (IndexError, ValueError):
            await query.answer("Ошибка данных", show_alert=True)
            return

        signal_age_min = _get_signal_age_minutes(ticker, direction)
        if signal_age_min is not None and signal_age_min > SIGNAL_STALE_AFTER_MIN:
            await query.answer(
                f"⚠️ Сигналу {signal_age_min:.0f} мин - устарел!",
                show_alert=True
            )
            await query.message.reply_text(
                f"⏰ <b>Сигнал устарел - {ticker}</b>\n\n"
                f"Этому сигналу уже {signal_age_min:.0f} минут "
                f"(актуальны ~{SIGNAL_STALE_AFTER_MIN - 10} мин).\n"
                f"Технический сетап (VWAP/RSI/объём) мог полностью развалиться "
                f"за это время, даже если цена почти не изменилась.\n\n"
                f"<i>Открой заново /analyze {ticker} чтобы получить свежий сигнал, "
                f"или используй кнопку ниже чтобы войти по старому сигналу на свой риск.</i>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                    f"⚠️ Всё равно войти по {entry:,.2f} ₽",
                    callback_data=f"enterforce_{ticker}_{direction}_{entry:.2f}_{sl:.2f}_{tp1:.2f}_{tp2:.2f}_{tp3:.2f}"
                )]])
            )
            return

        figi_check = MOEX_STOCKS.get(ticker.upper(), ("",))[0] or FUTURES.get(ticker.upper(), ("",))[0]
        real_price = await fetch_last_price_tinkoff(figi_check) if figi_check else None

        price_drift_pct = 0.0
        price_warning   = ""
        if real_price and entry:
            price_drift_pct = (real_price - entry) / entry * 100
            drift_abs = abs(price_drift_pct)

            is_worse = ((direction == "LONG" and real_price > entry) or
                       (direction == "SHORT" and real_price < entry))

            if drift_abs >= 1.0:
                
                await query.answer(
                    f"⚠️ Цена ушла на {price_drift_pct:+.2f}% от сигнала!",
                    show_alert=True
                )
                risk_now = abs(real_price - sl) / real_price * 100 if real_price else 0
                rr_tp1_now = abs(tp1 - real_price) / abs(real_price - sl) if (real_price and real_price != sl) else 0
                if is_worse:
                    direction_note = "📉 Заходишь по значительно худшей цене чем сигнал."
                else:
                    direction_note = "📈 Цена ушла в твою пользу - риск ниже расчётного."

                await query.message.reply_text(
                    f"⚠️ <b>Цена сигнала устарела - {ticker}</b>\n\n"
                    f"Цена в сигнале: {entry:,.2f} ₽\n"
                    f"Цена сейчас: <b>{real_price:,.2f} ₽</b> ({price_drift_pct:+.2f}%)\n\n"
                    f"{direction_note}\n"
                    f"Новый риск до SL: {risk_now:.2f}%\n"
                    f"Новый R/R до TP1: 1:{rr_tp1_now:.1f}\n\n"
                    f"<i>Нажми «Пересчитать» чтобы получить свежие уровни от "
                    f"текущей цены (заново найдёт support/resistance, а не "
                    f"просто сдвинет старые), или войди по цене сигнала "
                    f"на свой риск.</i>",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(
                            "🔄 Пересчитать от текущей цены",
                            callback_data=f"recalc_{ticker}_{direction}"
                        )],
                        [InlineKeyboardButton(
                            f"⚠️ Всё равно войти по {entry:,.2f} ₽",
                            callback_data=f"enterforce_{ticker}_{direction}_{entry:.2f}_{sl:.2f}_{tp1:.2f}_{tp2:.2f}_{tp3:.2f}"
                        )],
                    ])
                )
                return
            elif drift_abs >= 0.3:
                price_warning = (f"\n⚠️ <i>Цена сдвинулась на {price_drift_pct:+.2f}% "
                                f"с момента сигнала (сейчас {real_price:,.2f} ₽)</i>")

        tf = get_user_state(chat_id)["tf"]
        meta          = context.chat_data.get(f"last_analysis_{ticker}", {})
        tech_score    = meta.get("tech_score", 0)
        filter_status = meta.get("filter_status", "")
        anomaly       = meta.get("anomaly", "")
        ai_summary    = meta.get("ai_summary", "MANUAL")

        try:
            trade_id = open_trade(
                ticker, direction, entry, sl, tp1, tp2, tp3, chat_id, tf,
                tech_score=tech_score, filter_status=filter_status,
                anomaly=anomaly, ai_summary=ai_summary,
            )
        except ValueError as e:
            await query.answer(f"❌ Ошибка: {e}", show_alert=True)
            return

        risk_pct    = abs(entry - sl) / entry * 100 if entry else 0
        direction_e = "🟩 LONG" if direction == "LONG" else "🟥 SHORT"

        await query.answer("✅ Сделка открыта!", show_alert=True)

        base_msg = (
            f"✅ <b>Сделка открыта!</b>\n\n"
            f"<b>{esc(ticker)}</b> {direction_e}\n"
            f"Вход: {entry:,.2f} ₽  |  Риск: {risk_pct:.1f}%\n"
            f"SL: {sl:,.2f} ₽\n"
            f"TP1: {tp1:,.2f} ₽ → SL в безубыток\n"
            f"TP2: {tp2:,.2f} ₽ → SL на TP1\n"
            f"TP3: {tp3:,.2f} ₽ → Закрытие"
            f"{price_warning}\n\n"
            f"⏳ <i>AI анализирует сделку...</i>"
        )
        sent = await query.message.reply_text(base_msg, parse_mode="HTML")

        try:
            sector = ""
            if ticker.upper() in MOEX_STOCKS:
                _, _, sector, *_ = MOEX_STOCKS[ticker.upper()]

            news_task      = fetch_russian_news(ticker, sector)
            commodity_task = fetch_commodity_news(sector)
            macro_task     = fetch_macro_news()
            news_res, commodity_res, macro_res = await asyncio.gather(
                news_task, commodity_task, macro_task, return_exceptions=True
            )

            news_items = []
            if not isinstance(news_res, Exception) and news_res:
                news_items += news_res
            if not isinstance(commodity_res, Exception) and commodity_res:
                news_items += commodity_res
            if not isinstance(macro_res, Exception) and macro_res:
                news_items += macro_res

            review = await ai_trade_review(
                ticker=ticker, direction=direction,
                entry=entry, sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                tech_score=tech_score, filter_status=filter_status,
                news_items=news_items, meta=meta,
            )
        except Exception as e:
            logger.warning(f"ai_trade_review fetch error: {e}")
            review = ""

        ai_block = (
            f"\n\n🤖 <b>AI ревью:</b>\n{esc(review)}"
            if review else
            "\n\n<i>AI ревью недоступно.</i>"
        )
        await sent.edit_text(
            f"✅ <b>Сделка открыта!</b>\n\n"
            f"<b>{esc(ticker)}</b> {direction_e}\n"
            f"Вход: {entry:,.2f} ₽  |  Риск: {risk_pct:.1f}%\n"
            f"SL: {sl:,.2f} ₽\n"
            f"TP1: {tp1:,.2f} ₽ → SL в безубыток\n"
            f"TP2: {tp2:,.2f} ₽ → SL на TP1\n"
            f"TP3: {tp3:,.2f} ₽ → Закрытие"
            f"{price_warning}"
            f"{ai_block}\n\n"
            f"Алерты придут автоматически.\n/trades - все позиции",
            parse_mode="HTML"
        )

def _load_scanner_chats() -> list[int]:
    data = _load_json(RK_SCANNER_CHATS, None, [])
    return [int(x) for x in data if x]

def _save_scanner_chats(chats: list[int]) -> None:
    _save_json(RK_SCANNER_CHATS, None, chats)

SCANNER_CHAT_IDS: list[int] = _load_scanner_chats()

async def cmd_scan_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in SCANNER_CHAT_IDS:
        SCANNER_CHAT_IDS.append(chat_id)
        _save_scanner_chats(SCANNER_CHAT_IDS)
        await update.message.reply_text(
            "✅ <b>Интрадей авто-сканер активирован.</b>\n\n"
            "Бот будет сканировать рынок каждые 30 минут во время торговых сессий:\n"
            "• Утренняя доп. сессия: 07:00 - 09:49 МСК (только ликвидные бумаги)\n"
            "• Дневная сессия: 10:00 - 18:40 МСК\n"
            "• Вечерняя сессия: 19:05 - 23:50 МСК\n\n"
            "<i>🔔 Перед закрытием сессии в 23:35 придет уведомление о закрытии сделок.</i>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text("ℹ️ Авто-сканер уже запущен.")

async def cmd_scan_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in SCANNER_CHAT_IDS:
        SCANNER_CHAT_IDS.remove(chat_id)
        _save_scanner_chats(SCANNER_CHAT_IDS)
        await update.message.reply_text("🔕 Авто-сканер отключен.")
    else:
        await update.message.reply_text("ℹ️ Авто-сканер не был запущен.")

_last_broadcast_signals: set = set()
_last_futures_signals: set = set()

_signal_generated_at: dict[str, float] = {}
SIGNAL_STALE_AFTER_MIN = 40  

def _register_signal_time(ticker: str, direction: str) -> None:
    _signal_generated_at[f"{ticker}_{direction}"] = time.time()

def _get_signal_age_minutes(ticker: str, direction: str) -> float | None:
    ts = _signal_generated_at.get(f"{ticker}_{direction}")
    if ts is None:
        return None
    return (time.time() - ts) / 60

_scanner_failure_state: dict = {
    "consecutive_failures": 0,
    "consecutive_data_failures": 0,  
    "last_failure_notify_ts": 0.0,
    "was_failing": False,  
}
SCANNER_FAILURE_NOTIFY_COOLDOWN = 3600  

async def _notify_scanner_issue(app, message: str):
    """Шлёт уведомление о проблеме сканера во все чаты, где включён авто-скан."""
    for chat_id in SCANNER_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id, message, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"_notify_scanner_issue: не удалось отправить в {chat_id}: {e}")

async def _background_ai_evaluation(app, ticker: str, sector: str, 
                              news_items: list, tech_signal: str, 
                              tech_score: int, price: float) -> None:
    """Фоновая AI дооценка сигнала. Вызывается после отправки тех-сигнала в чат."""
    try:
        if not news_items:
            return
        news_ai = await ai_evaluate_news(
            list(news_items), ticker, sector, tech_signal, tech_score
        )
        ai_skip = news_ai.get("ai_skip_reason", "")
        if ai_skip and ai_skip != "deferred":
            return
        filter_status = news_ai.get("filter_status", "")
        summary = news_ai.get("summary", "")
        if not filter_status and not summary:
            return
        fs_label = {"CONFIRMED": "✅ CONFIRMED", "WEAK": "⚠️ WEAK", "NEWS_ONLY": "📰 NEWS"}.get(filter_status, filter_status)
        msg = f"🤖 <b>AI дооценка [{ticker}]</b>\nСтатус: {fs_label}\n"
        if summary:
            msg += f"📌 <i>{summary[:200]}</i>\n"
        msg += f"💰 Цена: {price:.2f} ₽ | TechScore: {tech_score}"
        for chat_id in SCANNER_CHAT_IDS:
            try:
                await app.bot.send_message(chat_id, msg, parse_mode="HTML")
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"_background_ai_evaluation {ticker}: {e}")


async def run_scanner_broadcast(app):
    global _last_broadcast_signals
    tf = "15m"
    mode_cfg = TRADE_MODES["mid"]
    wl = load_watchlist()
    if not wl:
        return
    long_sigs, short_sigs, _, scan_stats = await _run_scan(wl, tf, mode_cfg, exclude_low_liquidity=True)

    if scan_stats["failed_pct"] >= 50:
        _scanner_failure_state["consecutive_data_failures"] += 1
        logger.warning(f"run_scanner_broadcast: {scan_stats['failed']}/{scan_stats['total']} "
                       f"тикеров не удалось получить ({scan_stats['failed_pct']:.0f}%), "
                       f"цикл провала #{_scanner_failure_state['consecutive_data_failures']}")
        if _scanner_failure_state["consecutive_data_failures"] >= 2:
            now_ts = time.time()
            since_last = now_ts - _scanner_failure_state["last_failure_notify_ts"]
            if since_last >= SCANNER_FAILURE_NOTIFY_COOLDOWN:
                await _notify_scanner_issue(
                    app,
                    f"⚠️ <b>Проблема с получением данных</b>\n"
                    f"Не удалось получить котировки по {scan_stats['failed']} из "
                    f"{scan_stats['total']} тикеров ({scan_stats['failed_pct']:.0f}%) "
                    f"на протяжении нескольких циклов.\n"
                    f"Похоже на сбой Tinkoff API - сигналы могут не приходить "
                    f"до восстановления."
                )
                _scanner_failure_state["last_failure_notify_ts"] = now_ts
                _scanner_failure_state["was_failing"] = True
        return

    _scanner_failure_state["consecutive_data_failures"] = 0

    all_sigs = long_sigs + short_sigs
    if not all_sigs:
        return
    new_sigs = [s for s in all_sigs if f"{s['ticker']}_{s['tech_signal']}" not in _last_broadcast_signals]
    if not new_sigs:
        return
    for s in new_sigs:
        _last_broadcast_signals.add(f"{s['ticker']}_{s['tech_signal']}")
        direction = "LONG" if "LONG" in s["tech_signal"] else "SHORT"
        _register_signal_time(s["ticker"], direction)
    ts_scan = msk_now().strftime("%H:%M")
    
    lines = [
        f"🔔 <b>НОВЫЕ ИНТРАДЕЙ СИГНАЛЫ [{tf}] | {ts_scan} МСК</b>",
        f"<i>Сигналы актуальны ~30 мин. Сделки закройте до 23:50 МСК.</i>",
        "",
    ]
    for s in new_sigs[:5]:
        lines.append(_format_scan_row(s))
        lines.append("")
    text = "\n".join(lines)

    kb = []
    for s in new_sigs[:5]:
        sl_tp = s.get("sl_tp", {})
        if sl_tp and sl_tp.get("sl"):
            direction = "LONG" if "LONG" in s["tech_signal"] else "SHORT"
            p  = s["price"]; sl = sl_tp.get("sl", 0)
            t1 = sl_tp.get("tp1", 0); t2 = sl_tp.get("tp2", 0); t3 = sl_tp.get("tp3", 0)
            key = f"{s['ticker']}_{direction}_{int(time.time())}"[-20:]
            _cleanup_pending_trades()
            _pending_trades[key] = {"ticker": s["ticker"], "direction": direction,
                                    "entry": p, "sl": sl, "tp1": t1, "tp2": t2, "tp3": t3,
                                    "ts": time.time()}
            kb.append([InlineKeyboardButton(
                f"✅ Войти {s['ticker']} ({direction})",
                callback_data=f"enter2_{key}"
            )])
    markup = InlineKeyboardMarkup(kb) if kb else None

    for chat_id in SCANNER_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            logger.warning(f"Broadcast failed for {chat_id}: {e}")

    # Фоновая AI дооценка сигналов (если AI был отложен)
    for s in new_sigs[:5]:
        if s.get("news_ai", {}).get("ai_skip_reason") == "deferred":
            ticker_s = s["ticker"]
            asyncio.create_task(_background_ai_evaluation(
                app, ticker_s, s.get("sector", ""),
                s.get("news_items", []),
                s.get("tech_signal", ""),
                s.get("tech_score", 0),
                s.get("price", 0),
            ))

_last_news_signals: set = set()

async def _analyze_post_news_action(
    ticker: str, figi: str, news_weight: int, news_title: str
) -> dict:
    """
    Смотрит что СДЕЛАЛА цена после выхода новости.
    Берёт последние 6 свечей 5m - это ~30 минут, окно отыгрывания.

    Возвращает:
        action:    "long" | "short" | "wait" | "ignore"
        reason:    строка с объяснением
        price:     текущая цена
        move_pct:  движение за последние 6 свечей %
        pumped:    True если памп уже был (>1.5%)
        dumped:    True если дамп уже был (>1.5%)
    """
    try:
        df = await fetch_candles_tinkoff(figi, "CANDLE_INTERVAL_5_MIN", 12)
        if df is None or len(df) < 4:
            return {"action": "wait", "reason": "нет данных свечей", "price": 0,
                    "move_pct": 0, "pumped": False, "dumped": False}

        last   = df.iloc[-1]
        first  = df.iloc[-6] if len(df) >= 6 else df.iloc[0]
        price  = float(last["close"])
        open6  = float(first["open"])

        move_pct = (price - open6) / open6 * 100
        high6    = float(df.tail(6)["high"].max())
        low6     = float(df.tail(6)["low"].min())
        pumped   = (high6 - open6) / open6 * 100 > 1.5
        dumped   = (open6 - low6)  / open6 * 100 > 1.5

        avg_vol  = float(df["volume"].iloc[:-1].mean()) if len(df) > 1 else 1
        last_vol = float(last["volume"])
        vol_ratio = last_vol / avg_vol if avg_vol > 0 else 1.0

        if news_weight > 0:
            if pumped and price < high6 * 0.99 and vol_ratio < 0.7:
                
                return {"action": "short", "price": price, "move_pct": move_pct,
                        "pumped": pumped, "dumped": dumped,
                        "reason": f"памп +{(high6-open6)/open6*100:.1f}% уже был, "
                                  f"цена откатила до {price:.2f}, объём ↓{vol_ratio:.1f}x - sell the news"}
            elif move_pct > 0.3 and vol_ratio >= 1.0:
                
                return {"action": "long", "price": price, "move_pct": move_pct,
                        "pumped": pumped, "dumped": dumped,
                        "reason": f"рост +{move_pct:.1f}% с объёмом {vol_ratio:.1f}x - импульс продолжается"}
            elif abs(move_pct) < 0.3:
                
                return {"action": "wait", "price": price, "move_pct": move_pct,
                        "pumped": pumped, "dumped": dumped,
                        "reason": f"цена не отреагировала ({move_pct:+.1f}%) - новость уже в цене или слабая"}
            else:
                return {"action": "long", "price": price, "move_pct": move_pct,
                        "pumped": pumped, "dumped": dumped,
                        "reason": f"умеренный рост {move_pct:+.1f}%, подтверждение позитива"}

        else:
            if dumped and price > low6 * 1.01 and vol_ratio < 0.7:
                
                return {"action": "long", "price": price, "move_pct": move_pct,
                        "pumped": pumped, "dumped": dumped,
                        "reason": f"дамп -{(open6-low6)/open6*100:.1f}% уже был, "
                                  f"отскок до {price:.2f}, объём ↓ - возможен buy the dip"}
            elif move_pct < -0.3 and vol_ratio >= 1.0:
                return {"action": "short", "price": price, "move_pct": move_pct,
                        "pumped": pumped, "dumped": dumped,
                        "reason": f"падение {move_pct:.1f}% с объёмом {vol_ratio:.1f}x - импульс вниз"}
            elif abs(move_pct) < 0.3:
                return {"action": "wait", "price": price, "move_pct": move_pct,
                        "pumped": pumped, "dumped": dumped,
                        "reason": f"цена не отреагировала ({move_pct:+.1f}%) - новость нейтральна для рынка"}
            else:
                return {"action": "short", "price": price, "move_pct": move_pct,
                        "pumped": pumped, "dumped": dumped,
                        "reason": f"умеренное падение {move_pct:+.1f}%, подтверждение негатива"}

    except Exception as e:
        logger.debug(f"post_news_action {ticker}: {e}")
        return {"action": "wait", "reason": f"ошибка анализа: {e}",
                "price": 0, "move_pct": 0, "pumped": False, "dumped": False}

async def run_news_driven_scan(app):
    """
    Новостной сканер - смотрит на РЕАКЦИЮ ЦЕНЫ после новости.

    Логика:
    1. Нашли свежую (<30 мин) сильную новость по тикеру из watchlist
    2. Смотрим что сделала цена за последние 6 свечей 5m (~30 мин)
    3. AI + price action решают: лонг, шорт или ждать
    4. Шлём алерт с конкретным действием и причиной
    """
    global _last_news_signals
    wl = load_watchlist()
    if not wl:
        return

    async def _fetch_one(ticker: str):
        try:
            sector = MOEX_STOCKS.get(ticker, ("", "", ""))[2]
            news_items = await fetch_russian_news(ticker, sector)
            return ticker, news_items
        except Exception as e:
            logger.debug(f"news scan {ticker}: {e}")
            return ticker, []

    try:
        
        results = await asyncio.wait_for(
            asyncio.gather(*[_fetch_one(t) for t in wl], return_exceptions=True),
            timeout=60.0
        )
    except asyncio.TimeoutError:
        logger.warning("run_news_driven_scan: таймаут загрузки новостей (60с), пропускаю цикл")
        return

    triggered = []
    for res in results:
        if isinstance(res, Exception):
            continue
        ticker, news_items = res
        if not news_items:
            continue

        strong_fresh = [
            it for it in news_items
            if it.get("is_fact") and not it.get("is_opinion")
            and it.get("is_specific")
            and it.get("age_min", 999) <= 30
            and abs(it.get("weight", 0)) >= 6
        ]
        if not strong_fresh:
            continue

        best_news = max(strong_fresh, key=lambda x: abs(x.get("weight", 0)))
        sig_key   = f"news_{ticker}_{best_news['title'][:40]}"
        if sig_key in _last_news_signals:
            continue

        triggered.append({"ticker": ticker, "news": best_news, "sig_key": sig_key})

    if not triggered:
        return

    for t in triggered:
        _last_news_signals.add(t["sig_key"])

    ts_scan = msk_now().strftime("%H:%M")
    lines = [
        f"📰 <b>НОВОСТНОЙ ТРИГГЕР | {ts_scan} МСК</b>",
        "<i>Анализ реакции цены после новости</i>",
        "",
    ]
    kb = []

    for t in triggered[:5]:
        ticker  = t["ticker"]
        news    = t["news"]
        weight  = news.get("weight", 0)
        name    = MOEX_STOCKS.get(ticker, ("", ticker, ""))[1]
        age     = news.get("age_min", 0)
        figi    = MOEX_STOCKS.get(ticker, ("", "", ""))[0]

        pa = await _analyze_post_news_action(ticker, figi, weight, news["title"])
        action    = pa["action"]
        price     = pa["price"]
        move_pct  = pa["move_pct"]
        reason    = pa["reason"]

        if price == 0:
            logger.warning(f"news_trigger: {ticker} (figi: {figi}) - Tinkoff API не вернул свечи (price=0). Пропускаем.")
            continue

        action_emoji = {
            "long":   "🟢 ЛОНГ",
            "short":  "🔴 ШОРТ",
            "wait":   "⏳ ЖДАТЬ",
            "ignore": "⚪ ИГНОР",
        }.get(action, "⚪")

        news_emoji = "📈" if weight > 0 else "📉"

        lines += [
            f"{news_emoji} <b>{esc(ticker)}</b> - {esc(name)}",
            f"   <i>{esc(news['title'][:110])}</i>",
            f"   Новость: {weight:+d}/10 | {age:.0f} мин назад",
            f"   Цена: {price:,.2f} ₽  {move_pct:+.1f}% за 30м",
            f"   {action_emoji} - {esc(reason)}",
            "",
        ]

        if action in ("long", "short") and price > 0:
            try:
                direction = action.upper()
                quick = await analyze_stock(ticker, "5m", TRADE_MODES["mid"])
                if quick and "error" not in quick and quick.get("sl_tp"):
                    sl_tp = quick["sl_tp"]
                    sl = sl_tp.get("sl", 0); t1 = sl_tp.get("tp1", 0)
                    t2 = sl_tp.get("tp2", 0); t3 = sl_tp.get("tp3", 0)
                    if sl and t1:
                        key = f"{ticker}_{direction}_{int(time.time())}"[-20:]
                        _cleanup_pending_trades()
                        _pending_trades[key] = {"ticker": ticker, "direction": direction,
                                                            "entry": price, "sl": sl, "tp1": t1, "tp2": t2, "tp3": t3,
                                                            "ts": time.time()}
                        kb.append([InlineKeyboardButton(
                            f"✅ {action_emoji} {ticker} по новости",
                            callback_data=f"enter2_{key}"
                        )])
            except Exception as e:
                logger.debug(f"quick sl_tp for news {ticker}: {e}")

    text   = "\n".join(lines)
    markup = InlineKeyboardMarkup(kb) if kb else None

    for chat_id in SCANNER_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            logger.warning(f"News-driven broadcast failed for {chat_id}: {e}")

async def run_futures_broadcast(app):
    """Сканирует фьючерсы через analyze_futures и рассылает сигналы."""
    global _last_futures_signals
    tf       = "15m"
    mode_cfg = TRADE_MODES["mid"]
    fwl      = load_futures_watchlist()
    if not fwl:
        return

    results = []
    for code in fwl:
        try:
            r = await analyze_futures(code, tf, mode_cfg)
            if r and "error" not in r:
                results.append(r)
        except Exception as e:
            logger.debug(f"futures scan {code}: {e}")

    long_sigs  = [r for r in results if r["tech_signal"] == "🟩 LONG"]
    short_sigs = [r for r in results if r["tech_signal"] == "🟥 SHORT/ВЫХОД"]
    all_sigs   = sorted(long_sigs + short_sigs, key=lambda x: -x["tech_score"])

    if not all_sigs:
        return

    best_sigs, also_sigs = filter_correlated_futures(all_sigs)

    new_sigs = [s for s in best_sigs
                if f"fut_{s['ticker']}_{s['tech_signal']}" not in _last_futures_signals]
    if not new_sigs:
        return

    for s in new_sigs:
        _last_futures_signals.add(f"fut_{s['ticker']}_{s['tech_signal']}")
        direction = "LONG" if "LONG" in s["tech_signal"] else "SHORT"
        _register_signal_time(s["ticker"], direction)

    new_also = [s for s in also_sigs
                if f"fut_{s['ticker']}_{s['tech_signal']}" not in _last_futures_signals]

    ts_scan = msk_now().strftime("%H:%M")
    lines = [
        f"📊 <b>СИГНАЛЫ ПО ФЬЮЧЕРСАМ [{tf}] | {ts_scan} МСК</b>",
        "<i>Срочный рынок FORTS</i>", "",
    ]
    for s in new_sigs[:4]:
        sig_e  = "🟩 LONG" if "LONG" in s["tech_signal"] else "🟥 SHORT"
        sl_tp  = s.get("sl_tp", {})
        sl     = sl_tp.get("sl", 0)
        tp1    = sl_tp.get("tp1", 0)
        tp2    = sl_tp.get("tp2", 0)
        rr     = sl_tp.get("rr_ratio", 0)
        reason = s["tech_reasons"][0] if s["tech_reasons"] else "-"
        lines += [
            f"<b>{esc(s['ticker'])}</b> - {esc(s['name'])}",
            f" 💰 {s['price']:,.2f} | Скор: {s['tech_score']}/100 | {sig_e}",
            f" ⚙️ {esc(reason)}",
        ]
        if sl and tp1:
            risk = abs(s["price"] - sl) / s["price"] * 100 if s["price"] else 0
            lines.append(
                f" SL: {sl:,.2f} ({risk:.1f}%) | TP1: {tp1:,.2f} | TP2: {tp2:,.2f} | R/R {rr:.1f}"
            )
        lines.append("")

    if new_also:
        also_tickers = ", ".join(s["ticker"] for s in new_also[:5])
        lines.append(f"<i>📎 Коррелирует: {also_tickers} - аналогичный сигнал</i>")
        lines.append("")

    text = "\n".join(lines)
    kb   = []
    for s in new_sigs[:4]:
        sl_tp = s.get("sl_tp", {})
        if sl_tp and sl_tp.get("sl"):
            direction = "LONG" if "LONG" in s["tech_signal"] else "SHORT"
            p  = s["price"]; sl = sl_tp.get("sl", 0)
            t1 = sl_tp.get("tp1", 0); t2 = sl_tp.get("tp2", 0); t3 = sl_tp.get("tp3", 0)
            
            key = f"{s['ticker']}_{direction}"[:20]
            _cleanup_pending_trades()
            _pending_trades[key] = {"ticker": s["ticker"], "direction": direction,
                                    "entry": p, "sl": sl, "tp1": t1, "tp2": t2, "tp3": t3,
                                    "ts": time.time()}
            cb = f"enter2_{key}"
            kb.append([InlineKeyboardButton(f"✅ Войти {s['ticker']} ({direction})", callback_data=cb)])
    markup = InlineKeyboardMarkup(kb) if kb else None

    for chat_id in SCANNER_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
        except Exception as e:
            logger.warning(f"Futures broadcast {chat_id}: {e}")

async def retro_check_signals_loop():
    """
    Раз в 15 минут проверяет все pending-сигналы (зарегистрированные в
    _pending_outcomes_add) - дошла ли цена до TP1/SL, или сигнал протух.

    Логика исхода:
    - TP1/TP2/TP3 достигнут → outcome = "tp1_hit" / "tp2_hit" / "tp3_hit"
    - SL достигнут → outcome = "sl_hit"
    - Прошло больше 4 часов без исхода → outcome = "expired_no_move"
    Результат пишется в RK_OUTCOME_STATS для последующего /outcome_stats.
    """
    while True:
        try:
            pending = _pending_outcomes_get_all()
            now_ts  = time.time()

            for signal_id, data in pending.items():
                try:
                    signal_dt = datetime.fromisoformat(data["signal_ts"])
                    if signal_dt.tzinfo is None:
                        signal_dt = signal_dt.replace(tzinfo=timezone.utc)
                    elapsed_min = (datetime.now(timezone.utc) - signal_dt).total_seconds() / 60

                    figi = data.get("figi", "")
                    if not figi:
                        _pending_outcomes_remove(signal_id)
                        continue

                    cur_price = await fetch_last_price_tinkoff(figi)
                    if not cur_price:
                        continue  

                    is_long = data["direction"] == "LONG"
                    entry   = data["entry_price"]
                    sl      = data["sl"]
                    tp1, tp2, tp3 = data["tp1"], data["tp2"], data["tp3"]

                    if not entry or entry <= 0:
                        
                        _pending_outcomes_remove(signal_id)
                        continue

                    cur_move_pct = ((cur_price - entry) / entry * 100 if is_long
                                    else (entry - cur_price) / entry * 100)
                    mfe_pct = max(data.get("mfe_pct", 0.0), cur_move_pct)
                    mae_pct = min(data.get("mae_pct", 0.0), cur_move_pct)

                    outcome = None
                    if is_long:
                        if sl and cur_price <= sl:
                            outcome = "sl_hit"
                        elif tp3 and cur_price >= tp3:
                            outcome = "tp3_hit"
                        elif tp2 and cur_price >= tp2:
                            outcome = "tp2_hit"
                        elif tp1 and cur_price >= tp1:
                            outcome = "tp1_hit"
                    else:
                        if sl and cur_price >= sl:
                            outcome = "sl_hit"
                        elif tp3 and cur_price <= tp3:
                            outcome = "tp3_hit"
                        elif tp2 and cur_price <= tp2:
                            outcome = "tp2_hit"
                        elif tp1 and cur_price <= tp1:
                            outcome = "tp1_hit"

                    if outcome is None and elapsed_min > 240:
                        outcome = "expired_no_move"

                    if outcome:
                        pnl_pct = ((cur_price - entry) / entry * 100 if is_long
                                   else (entry - cur_price) / entry * 100)
                        
                        mfe_pct = max(mfe_pct, pnl_pct)
                        mae_pct = min(mae_pct, pnl_pct)
                        # ИСПРАВЛЕНИЕ: Автоматически обучаем память ИИ по результатам исхода!
                        is_ai_hit = "tp" in outcome or pnl_pct > 0.3
                        ev_type_logged = data.get("event_type", "")
                        if ev_type_logged:
                            update_ai_memory_stat(ev_type_logged, is_ai_hit)
                        _outcome_stats_append({
                            "ticker":        data["ticker"],
                            "tf":            data["tf"],
                            "direction":     data["direction"],
                            "outcome":       outcome,
                            "pnl_pct":       round(pnl_pct, 2),
                            "mfe_pct":       round(mfe_pct, 2),
                            "mae_pct":       round(mae_pct, 2),
                            "tech_score":    data.get("tech_score", 0),
                            "filter_status": data.get("filter_status", ""),
                            "event_type":    ev_type_logged,
                            "signal_ts":     data["signal_ts"],
                            "resolved_ts":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            "elapsed_min":   round(elapsed_min, 0),
                        })
                        _pending_outcomes_remove(signal_id)
                    else:
                        
                        data["mfe_pct"] = mfe_pct
                        data["mae_pct"] = mae_pct
                        _pending_outcomes_add(signal_id, data)

                except Exception as item_err:
                    logger.debug(f"retro_check item {signal_id}: {item_err}")

        except Exception as e:
            logger.warning(f"retro_check_signals_loop error: {e}")

        await asyncio.sleep(900)  

async def scanner_loop(app):
    """Главный фоновый цикл сканера рынка."""
    global _last_broadcast_signals
    global _last_futures_signals
    global _last_news_signals
    while True:
        try:
            day, hour, minute = get_msk_time()
            is_trading_day     = is_trading_day_moex()
            is_morning_session = (7 <= hour < 9) or (hour == 9 and minute < 49)
            is_main_session    = (10 <= hour < 18) or (hour == 18 and minute < 40)
            is_evening_session = (19 <= hour < 23) or (hour == 23 and minute < 50)
            is_trading_time    = is_trading_day and (
                is_morning_session or is_main_session or is_evening_session
            )

            if is_trading_day and hour == 7 and minute < 2:
                _last_broadcast_signals = set()
                _last_futures_signals   = set()
                _last_news_signals      = set()
                _pd_sent.clear()
            if is_trading_day and hour == 10 and minute < 2:
                _last_broadcast_signals = set()
                _last_futures_signals   = set()
                _last_news_signals      = set()
                _pd_sent.clear()
            if is_trading_day and hour == 19 and minute < 2:
                _last_broadcast_signals = set()
                _last_futures_signals   = set()
                _last_news_signals      = set()

            if is_trading_day and hour == 23 and 34 <= minute <= 36:
                for chat_id in SCANNER_CHAT_IDS:
                    try:
                        await app.bot.send_message(
                            chat_id,
                            "🚨 <b>ВНИМАНИЕ! До закрытия вечерней сессии осталось 15 минут!</b>\n"
                            "Убедитесь, что все открытые интрадей-сделки закрыты.",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.warning(f"Failed warning to {chat_id}: {e}")
                await asyncio.sleep(120)
                continue

            if is_trading_time and SCANNER_CHAT_IDS:
                
                async def _safe_scan(name: str, coro):
                    try:
                        await coro  
                    except asyncio.TimeoutError:
                        logger.warning(f"scanner_loop: {name} timeout - пропущен")
                    except Exception as _e:
                        logger.error(f"scanner_loop: {name} error: {type(_e).__name__}: {_e}",
                                     exc_info=True)

                try:
                    await asyncio.gather(
                        _safe_scan("stocks",    run_scanner_broadcast(app)),
                        _safe_scan("futures",   run_futures_broadcast(app)),
                        _safe_scan("pump_dump", run_pump_dump_broadcast(app, tf="15m")),
                        _safe_scan("news",      run_news_driven_scan(app)),
                    )
                    
                    if _scanner_failure_state["was_failing"]:
                        await _notify_scanner_issue(
                            app,
                            "✅ <b>Сканер восстановился</b>\n"
                            "Сигналы снова приходят вовремя."
                        )
                        _scanner_failure_state["was_failing"] = False
                    _scanner_failure_state["consecutive_failures"] = 0

                except Exception as _gather_err:
                    logger.error(f"scanner_loop: gather error: {_gather_err}", exc_info=True)
                    _scanner_failure_state["consecutive_failures"] += 1
                    now_ts = time.time()
                    since_last_notify = now_ts - _scanner_failure_state["last_failure_notify_ts"]
                    if since_last_notify >= SCANNER_FAILURE_NOTIFY_COOLDOWN:
                        n = _scanner_failure_state["consecutive_failures"]
                        await _notify_scanner_issue(
                            app,
                            f"⚠️ <b>Цикл сканирования пропущен</b>\n"
                            f"Скан упал с ошибкой - пропущено циклов: {n}.\n"
                            f"Сигналы могут не приходить до следующего успешного цикла."
                        )
                        _scanner_failure_state["last_failure_notify_ts"] = now_ts
                        _scanner_failure_state["was_failing"] = True
            else:
                if not is_trading_time:
                    _last_broadcast_signals = set()
                    _last_futures_signals   = set()
                    _last_news_signals      = set()
                    _pd_sent.clear()

        except Exception as e:
            logger.error(f"Error in scanner loop: {e}", exc_info=True)
            _scanner_failure_state["consecutive_failures"] += 1
            now_ts = time.time()
            since_last_notify = now_ts - _scanner_failure_state["last_failure_notify_ts"]
            if SCANNER_CHAT_IDS and since_last_notify >= SCANNER_FAILURE_NOTIFY_COOLDOWN:
                n = _scanner_failure_state["consecutive_failures"]
                await _notify_scanner_issue(
                    app,
                    f"🔴 <b>Ошибка в цикле сканера</b>\n"
                    f"Скан упал с ошибкой ({type(e).__name__}) - пропущено циклов: {n}.\n"
                    f"Бот продолжит попытки каждые 30 минут."
                )
                _scanner_failure_state["last_failure_notify_ts"] = now_ts
                _scanner_failure_state["was_failing"] = True

        _, h, m = get_msk_time()
        mins_to_next = 30 - (m % 30)
        await asyncio.sleep(mins_to_next * 60)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MAIN_MENU_KEYBOARD, MENU_BUTTON_LABELS
    raw_text = update.message.text.strip()
    chat_id  = update.effective_chat.id

    awaiting_key = context.chat_data.get("awaiting_setting")
    awaiting_ts  = context.chat_data.get("awaiting_setting_ts", 0)
    is_stale     = (time.time() - awaiting_ts) > 300  
    is_menu_button = raw_text in MENU_BUTTON_LABELS
    is_command     = raw_text.startswith("/")

    if awaiting_key and is_stale:
        context.chat_data.pop("awaiting_setting", None)
        context.chat_data.pop("awaiting_setting_ts", None)
        awaiting_key = None

    if awaiting_key and not is_menu_button and not is_command:
        try:
            new_value = float(raw_text.replace(",", "."))
        except ValueError:
            await update.message.reply_text(
                "❌ Нужно числовое значение (например: 5 или 1.5). Попробуй снова, "
                "или отправь /settings чтобы отменить."
            )
            return
        ok = set_bot_setting(awaiting_key, new_value)
        context.chat_data.pop("awaiting_setting", None)
        context.chat_data.pop("awaiting_setting_ts", None)
        if ok:
            label = BOT_SETTINGS_LABELS.get(awaiting_key, awaiting_key)
            await update.message.reply_text(
                f"✅ <b>{label}</b> изменено на <b>{new_value}</b>.\n"
                f"Открой /settings чтобы увидеть все текущие значения.",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text(
                "❌ Не удалось сохранить - проверь что Redis подключён."
            )
        return

    if raw_text in AI_MENU_LABELS:
        new_state = not get_ai_enabled()
        set_ai_enabled(new_state)
        MAIN_MENU_KEYBOARD = _build_main_menu_keyboard()
        MENU_BUTTON_LABELS = {
            btn.text for row in MAIN_MENU_KEYBOARD.keyboard for btn in row
        }
        icon = "🟢" if new_state else "🔴"
        status = "включены" if new_state else "выключены"
        await update.message.reply_text(
            f"{icon} <b>Все ИИ-функции {status}</b>"
            f"{'Бот будет использовать Gemini + Groq + OpenRouter для анализа новостей и сигналов.' if new_state else 'Сканер работает по технике без ИИ. Новости не классифицируются.'}",
            parse_mode="HTML",
            reply_markup=MAIN_MENU_KEYBOARD
        )
        return

    menu_actions = {
        "📊 Анализ":     lambda: update.message.reply_text(
            "Введи тикер для анализа, например: <code>SBER</code> или <code>/analyze GAZP</code>",
            parse_mode="HTML"),
        "🔍 Скан":       lambda: cmd_scan(update, context),
        "🗂 Ватчлист":   lambda: cmd_watchlist(update, context),
        "📂 Сделки":     lambda: cmd_trades(update, context),
        "📈 Фьючерсы":   lambda: cmd_futures_list(update, context),
        "🚀 Памп/Дамп":  lambda: cmd_pump_dump(update, context),
        "📅 Календарь":  lambda: cmd_calendar(update, context),
        "⚙️ Режим":      lambda: cmd_mode(update, context),
        "📰 Новости":    lambda: update.message.reply_text(
            "Введи: <code>/news SBER</code> для новостей по тикеру, "
            "или просто <code>/news</code> для общих новостей рынка.",
            parse_mode="HTML"),
        "🔧 Настройки бота": lambda: cmd_settings(update, context),
        "🔍 Диагностика":   lambda: cmd_diagnostics(update, context),
        "📈 Стата сигналов": lambda: cmd_outcome_stats(update, context),
        "ℹ️ Помощь":     lambda: cmd_start(update, context),
        "❌ Скрыть меню": lambda: update.message.reply_text(
            "Меню скрыто. Команда <code>/menu</code> вернёт его обратно.",
            parse_mode="HTML", reply_markup=ReplyKeyboardRemove()),
    }
    if raw_text in menu_actions:
        await menu_actions[raw_text]()
        return

    text       = raw_text.upper()
    is_stock   = text in MOEX_STOCKS
    is_futures = text in FUTURES

    if is_stock or is_futures:
        msg = await update.message.reply_text(
            f"⏳ Анализирую <b>{text}</b>...", parse_mode="HTML")
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
        tf = get_user_state(chat_id)["tf"]
        try:
            result = await (analyze_stock(text, tf, mode_cfg) if is_stock
                            else analyze_futures(text, tf, mode_cfg))
            formatted = format_analysis(result)
            try:
                await msg.edit_text(formatted, parse_mode="HTML")
            except Exception as fmt_err:
                logger.warning(f"HTML format error {text}: {fmt_err}")
                plain = (formatted
                         .replace("<b>","").replace("</b>","")
                         .replace("<i>","").replace("</i>","")
                         .replace("<code>","").replace("</code>",""))
                await msg.edit_text(plain[:4000])
        except Exception as e:
            logger.error(f"handle_message analyze {text}: {e}", exc_info=True)
            await msg.edit_text(
                f"❌ Ошибка анализа <b>{esc(text)}</b>:\n<code>{esc(str(e)[:300])}</code>\n\n"
                f"Проверь /update_figi или попробуй другой TF.",
                parse_mode="HTML")
    else:
        matches = [t for t in list(MOEX_STOCKS.keys()) + list(FUTURES.keys())
                   if t.startswith(text[:3])]
        if matches:
            await update.message.reply_text(
                f"Тикер <code>{esc(text)}</code> не найден.\n"
                f"Возможно: {', '.join(f'<code>{m}</code>' for m in matches[:5])}",
                parse_mode="HTML")

async def health_check(request):
    return web.Response(text="OK", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌐 Healthcheck server started on port {PORT}")

async def _calendar_loop():
    while True:
        count = await auto_update_calendar()
        logger.info(f"Calendar refresh: {count} events")
        await asyncio.sleep(21600)

import signal as _signal

async def _on_shutdown(app):
    """Graceful shutdown - закрываем HTTP сессию и Redis."""
    global _http_session
    logger.info("Shutting down gracefully...")
    if _http_session and not _http_session.closed:
        await _http_session.close()
        logger.info("HTTP session closed")
    r = _get_redis()
    if r:
        try:
            r.close()
        except Exception:
            pass
    logger.info("Shutdown complete")

async def post_init(app):
    
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Startup: webhook cleared, pending updates dropped")
    except Exception as e:
        logger.warning(f"Startup: delete_webhook failed: {e}")

    await _set_bot_commands(app)

    loaded = _load_figi_from_file()
    if loaded:
        logger.info(f"Startup: loaded {loaded} FIGIs from file")
    else:
        logger.warning("Startup: cache cleared, starting fresh.")

    if _gemini_client and GEMINI_MODEL:
        logger.info(f"✅ Gemini ready: {GEMINI_MODEL} (google-genai SDK)")
    else:
        logger.warning("⚠️ Gemini недоступен - нет ключа или ошибка инициализации")
    if groq_client:
        logger.info(f"✅ Groq ready (fallback): {GROQ_MODELS[0]}")
    else:
        logger.warning("⚠️ Groq недоступен - нет ключа GROQ_API_KEY")

    _safe_redis_cleanup()

    _rss_check_urls = COMMODITY_NEWS_RSS[:3]  
    session = _get_http_session()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MOEXBot/1.0)"}
    for url in _rss_check_urls:
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status == 200:
                    logger.info(f"Startup RSS check OK: {url}")
                else:
                    logger.warning(f"Startup RSS check FAIL {resp.status}: {url} - источник будет пропускаться")
        except Exception as e:
            logger.warning(f"Startup RSS check ERROR: {url} - {e}")

    asyncio.create_task(auto_update_figi_loop())
    asyncio.create_task(auto_update_calendar())

    try:
        await asyncio.wait_for(fetch_nearest_futures(), timeout=25.0)
    except asyncio.TimeoutError:
        logger.warning("Startup: обновление FIGI фьючерсов не успело за 25с, "
                       "продолжаю с текущими данными")
    except Exception as e:
        logger.warning(f"Startup: fetch_nearest_futures упала ({e})")
    asyncio.create_task(auto_update_futures_loop())

    asyncio.create_task(scanner_loop(app))
    asyncio.create_task(retro_check_signals_loop())
    asyncio.create_task(run_geopolitical_scan())
    asyncio.create_task(monitor_trades_loop(app, fetch_last_price_tinkoff))
    asyncio.create_task(_calendar_loop())
    asyncio.create_task(start_web_server())
    # Автоматическая диагностика при старте (если указан ADMIN_CHAT_ID)
    if ADMIN_CHAT_ID:
        try:
            report = await _build_diagnostics_report()
            text = "\n".join(report)
            await app.bot.send_message(int(ADMIN_CHAT_ID), text, parse_mode="HTML")
            logger.info("Startup diagnostics sent to ADMIN_CHAT_ID")
        except Exception as e:
            logger.warning(f"Startup diagnostics send failed: {e}")
    elif SCANNER_CHAT_IDS:
        try:
            report = await _build_diagnostics_report()
            text = "\n".join(report)
            for cid in SCANNER_CHAT_IDS:
                try:
                    await app.bot.send_message(cid, text, parse_mode="HTML")
                except Exception:
                    pass
            logger.info(f"Startup diagnostics sent to {len(SCANNER_CHAT_IDS)} chats")
        except Exception as e:
            logger.warning(f"Startup diagnostics send failed: {e}")
    else:
        logger.info("No ADMIN_CHAT_ID or SCANNER_CHAT_IDS - diagnostics skipped")
    
    logger.info("🚀 MOEX Railway Bot fully initialized")




async def cmd_diagnostics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Полная проверка работоспособности всех компонентов бота."""
    report = await _build_diagnostics_report()
    await update.message.reply_text("\n".join(report), parse_mode="HTML")


async def _build_diagnostics_report() -> list[str]:
    """Собирает отчёт диагностики (без привязки к update/context).
    Используется в cmd_diagnostics и при автодиагностике на старте.
    """
    status = {"✅": [], "❌": [], "⚠️": []}
    
    # 1. Проверка Telegram API
    if TELEGRAM_TOKEN:
        status["✅"].append("🔑 Телеграм API: токен настроен")
    else:
        status["❌"].append("🔑 Телеграм API: ТОКЕН не настроен")
    
    # 2. Проверка Redis подключения
    try:
        redis = _get_redis()
        if redis:
            status["✅"].append("🗄 Redis: соединение успешно")
        else:
            status["⚠️"].append("🗄 Redis: не подключено (используются файлы)")
    except Exception as e:
        status["❌"].append(f"🗄 Redis: ошибка - {str(e)}")
    
    # 2.5 AI Toggle status
    if get_ai_enabled():
        status["✅"].append("🤖 ИИ-режим: ВКЛЁН (Gemini + Groq + OpenRouter)")
    else:
        status["⚠️"].append("🤖 ИИ-режим: ВЫКЛЁН (сканер работает без ИИ)")
    
    # 3. Проверка Gemini AI
    if google_genai and GEMINI_API_KEY:
        try:
            _gemini_client = google_genai.Client(api_key=GEMINI_API_KEY)
            status["✅"].append(f"🤖 Gemini AI: {GEMINI_MODEL} готов")
        except Exception:
            status["⚠️"].append("🤖 Gemini AI: ключ настроен, но клиент не работает")
    else:
        status["❌"].append("🤖 Gemini AI: не подключен (нет ключа или библиотеки)")
    
    # 4. Проверка Groq AI
    if Groq and GROQ_API_KEY:
        status["✅"].append(f"🧠 Groq AI: {GROQ_MODELS[0]} готов")
    else:
        status["❌"].append("🧠 Groq AI: не подключен (нет ключа или библиотеки)")
    

    # 4.5. Проверка OpenRouter
    if OPENROUTER_API_KEY:
        status["✅"].append("🌐 OpenRouter API: ключ настроен")
    else:
        status["⚠️"].append("🌐 OpenRouter API: нет ключа")

    # 5. Проверка Tinkoff API
    if TINKOFF_TOKEN:
        status["✅"].append("💰 Tinkoff API: токен настроен")
    else:
        status["⚠️"].append("💰 Tinkoff API: нет токена")

    # 5.1. Тест Tinkoff API (свечи SBER)
    if TINKOFF_TOKEN:
        try:
            figi_sber = MOEX_STOCKS.get("SBER", (None,))[0]
            if figi_sber:
                df_test = await fetch_candles_tinkoff(figi_sber, "CANDLE_INTERVAL_DAY", 5)
                if df_test is not None and len(df_test) > 0:
                    status["✅"].append(f"📊 Tinkoff свечи SBER: {len(df_test)} свечей (OK)")
                else:
                    status["❌"].append("📊 Tinkoff свечи SBER: пустой ответ от API")
        except Exception as tinkoff_err:
            status["⚠️"].append(f"📊 Tinkoff API тест: {tinkoff_err}")

    
    # 6. Проверка RSS источников и личного RSSHub
    russian_count = len(RUSSIAN_NEWS_RSS)
    commodity_count = len(COMMODITY_NEWS_RSS)
    total_rss = russian_count + commodity_count
    status["✅"].append(f"📰 RSS источников настроено: {total_rss} (РФ + ТГ: {russian_count}, Мировые: {commodity_count})")

    if RSSHUB_URL:
        test_url = f"{RSSHUB_URL}/telegram/channel/markettwits"
        try:
            session = _get_http_session()
            async with session.get(test_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    status["✅"].append(f"📡 Личный RSSHub ({RSSHUB_URL}): РАБОТАЕТ (HTTP 200 OK)")
                else:
                    status["⚠️"].append(f"📡 Личный RSSHub ({RSSHUB_URL}): отвечает HTTP {resp.status} - добавьте TELEGRAM_BOT_TOKEN в настройки RSSHub в Railway")
        except Exception as rss_err:
            status["⚠️"].append(f"📡 Личный RSSHub ({RSSHUB_URL}): ошибка подключения ({rss_err})")
    else:
        status["⚠️"].append("📡 RSSHub: переменная RSSHUB_URL не задана в Railway")
    
    # 7. Проверка календаря событий
    try:
        # Check if calendar data exists
        from pathlib import Path
        if MOEX_HOLIDAYS_2026 and len(MOEX_HOLIDAYS_2026) > 0:
            status["✅"].append(f"📅 Календарь: {len(MOEX_HOLIDAYS_2026)} праздников настроено")
        else:
            status["⚠️"].append("📅 Календарь: может быть устарел")
    except Exception:
        status["❌"].append("📅 Календарь: данные не найдены")
    
    # 8. Проверка списка инструментов
    if len(MOEX_STOCKS) > 100:
        status["✅"].append(f"📋 Инструменты: настроено {len(MOEX_STOCKS)} инструментов")
    else:
        status["⚠️"].append(f"📋 Инструменты: настроено {len(MOEX_STOCKS)} инструментов")
    
    # 9. Проверка фьючерсов
    if len(FUTURES) > 0:
        status["✅"].append(f"🔄 Фьючерсы: настроено {len(FUTURES)} инструментов")
    else:
        status["❌"].append("🔄 Фьючерсы: не настроены")
    
    # 10. Проверка watchlist
    try:
        watchlist = load_watchlist()
        if len(watchlist) > 0:
            status["✅"].append(f"⭐ Watchlist: настроено {len(watchlist)} инструментов")
        else:
            status["⚠️"].append("⭐ Watchlist: настроено 0 инструментов (используются дефолтные)")
    except Exception:
        status["❌"].append("⭐ Watchlist: ошибка загрузки")
    
    # 11. Проверка web-сервера
    try:
        # Try to import and check server availability
        import sys
        from moex_web_server import start_web_server
        status["✅"].append("🌐 Web-сервер: модуль доступен")
    except ImportError:
        status["⚠️"].append("🌐 Web-сервер: не импортирован (может быть отдельным файлом)")
    except Exception as e:
        status["❌"].append(f"🌐 Web-сервер: ошибка - {str(e)}")
    
    # 12. Проверка OpenRouter (третий AI fallback)
    if OPENROUTER_API_KEY:
        if openrouter_client:
            status["✅"].append("🔌 OpenRouter AI: ключ есть, клиент инициализирован")
        else:
            status["⚠️"].append("🔌 OpenRouter AI: ключ есть, но клиент не создан")
    else:
        status["⚠️"].append("🔌 OpenRouter AI: не настроен (нет ключа)")

    try:
        if scanner_loop and callable(scanner_loop):
            status["✅"].append("🔍 Сканер: функция доступна")
        else:
            status["❌"].append("🔍 Сканер: функция недоступна")
    except Exception:
        status["❌"].append("🔍 Сканер: ошибка")

    
    # 13. Проверка/geopolitical сканирования
    try:
        if run_geopolitical_scan and callable(run_geopolitical_scan):
            status["✅"].append("🌍 Geo-политический сканер: функция доступна")
        else:
            status["❌"].append("🌍 Geo-политический сканер: функция недоступна")
    except Exception:
        status["❌"].append("🌍 Geo-политический сканер: ошибка")
    
    # 14. Проверка Redis ключей на WRONGTYPE
    try:
        r = _get_redis()
        if r:
            problem_keys = []
            for test_key in [RK_CHAT_DATA, RK_SECTOR_MODIFIERS, RK_SIGNAL_OUTCOMES, RK_OUTCOME_STATS, RK_SIGNAL_LOG]:
                if r.exists(test_key) and r.type(test_key) == "string":
                    problem_keys.append(test_key)
            if problem_keys:
                status["❌"].append(f"🗄 Redis: {len(problem_keys)} ключей имеют неверный тип (string вместо hash/list)")
                for pk in problem_keys:
                    status["❌"].append(f"   - {pk}")
            else:
                status["✅"].append("🗄 Redis: все ключи имеют корректный тип")
    except Exception:
        pass
    
    # Форматирование ответа
    report = ["🔧 <b>Диагностика бота - полная проверка</b>\n\n"]
    
    if status["✅"]:
        report.append("<b>✅ РАБОТАЕТ (" + str(len(status["✅"])) + ")</b>")
        for item in status["✅"]:
            report.append(f"  {item}")
        report.append("")
    
    if status["⚠️"]:
        report.append("<b>⚠️ ПРЕДУПРЕЖДЕНИЯ (" + str(len(status["⚠️"])) + ")</b>")
        for item in status["⚠️"]:
            report.append(f"  {item}")
        report.append("")
    
    if status["❌"]:
        report.append("<b>❌ ОШИБКИ (" + str(len(status["❌"])) + ")</b>")
        for item in status["❌"]:
            report.append(f"  {item}")
    
    # Общая статистика
    total = len(status["✅"]) + len(status["⚠️"]) + len(status["❌"])
    failed = len(status["❌"])
    warnings = len(status["⚠️"])
    
    report.append(f"\n<b>Statistika:</b> всего {total} компонентов, не работает {failed}, предупреждений {warnings}")
    
    if failed == 0:
        report.append("🎉 <b>Все системы в порядке!</b>")
    elif warnings == 0:
        report.append("⚠️ <b>Есть критические ошибки</b>")
    else:
        report.append("⚠️ <b>Есть предупреждения и ошибки</b>")
    
    return report


def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN отсутствует в настройках среды.")

    persistence = None
    if REDIS_URL and (REDIS_URL.startswith("redis://") or REDIS_URL.startswith("rediss://")):
        try:
            persistence = RedisChatDataPersistence()
            logger.info("Применение RedisChatDataPersistence для сохранения сессий")
        except Exception as pe:
            logger.warning(f"Не удалось инициализировать RedisChatDataPersistence: {pe}")
            persistence = None

    builder = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(_on_shutdown)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
    )
    if persistence:
        builder = builder.persistence(persistence)

    application = builder.build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("menu", cmd_menu))
    application.add_handler(CommandHandler("analyze", cmd_analyze))
    application.add_handler(CommandHandler("scan", cmd_scan))
    application.add_handler(CommandHandler("news", cmd_news))
    application.add_handler(CommandHandler("market", cmd_market))
    application.add_handler(CommandHandler("mode", cmd_mode))
    application.add_handler(CommandHandler("tf", cmd_tf))
    application.add_handler(CommandHandler("trades", cmd_trades))
    application.add_handler(CommandHandler("export_log", cmd_export_log))
    application.add_handler(CommandHandler("open_trade", cmd_open_trade))
    application.add_handler(CommandHandler("close_trade", cmd_close_trade))
    application.add_handler(CommandHandler("watchlist", cmd_watchlist))
    application.add_handler(CommandHandler("all_tickers", cmd_all_tickers))
    application.add_handler(CommandHandler("add", cmd_add))
    application.add_handler(CommandHandler("remove", cmd_remove))
    application.add_handler(CommandHandler("clear_watchlist", cmd_clear_watchlist))
    application.add_handler(CommandHandler("add_sector", cmd_add_sector))
    application.add_handler(CommandHandler("add_all", cmd_add_all))
    application.add_handler(CommandHandler("scan_start", cmd_scan_start))
    application.add_handler(CommandHandler("scan_stop", cmd_scan_stop))
    application.add_handler(CommandHandler("update_figi",    cmd_update_figi))
    application.add_handler(CommandHandler("calendar",        cmd_calendar))
    application.add_handler(CommandHandler("calendar_add",    cmd_calendar_add))
    application.add_handler(CommandHandler("futures",        cmd_futures))
    application.add_handler(CommandHandler("futures_list",   cmd_futures_list))
    application.add_handler(CommandHandler("scan_futures",   cmd_scan_futures))
    application.add_handler(CommandHandler("fadd",           cmd_fadd))
    application.add_handler(CommandHandler("fadd_all",       cmd_fadd_all))
    application.add_handler(CommandHandler("fremove",        cmd_fremove))
    application.add_handler(CommandHandler("pd",             cmd_pump_dump))
    application.add_handler(CommandHandler("stats",          cmd_stats))
    application.add_handler(CommandHandler("settings",       cmd_settings))
    application.add_handler(CommandHandler("outcome_stats",  cmd_outcome_stats))
    application.add_handler(CommandHandler("sectors",        cmd_sectors))
    application.add_handler(CommandHandler("diagnostics", cmd_diagnostics))
    application.add_handler(CommandHandler("ai", cmd_ai_toggle))
    application.add_handler(CommandHandler("ai_memory", cmd_ai_memory))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def error_handler(update, context):
        err = context.error
        if "Conflict" in str(err):
            logger.warning(f"Conflict (другой инстанс?): {err}")
            return
        logger.error(f"Update error: {err}", exc_info=context.error)

    application.add_error_handler(error_handler)

    logger.info("Bot starting in polling mode...")

    application.run_polling(
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=False,  
        poll_interval=2.0,
        timeout=30,
    )

if __name__ == "__main__":
    main()
