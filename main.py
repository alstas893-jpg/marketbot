import asyncio
import aiohttp
import aiosqlite
import os
import time
import logging

from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =========================
# ENV
# =========================

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
FREE_LIMIT = int(os.getenv("FREE_LIMIT", 3))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 12))
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 300))  # 5 минут
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", 3600))  # 1 час

# =========================
# BOT
# =========================

bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

DB = "users.db"

SIGNAL_TRANSLATION = {
    "BREAKOUT": "ПРОБОЙ",
    "REVERSAL": "РАЗВОРОТ",
    "TREND": "ТРЕНД"
}

# Кеш для дедупликации сигналов
sent_signals = defaultdict(dict)

# Кеш для тикеров MOEX
tickers_cache = {"data": None, "timestamp": 0}

# =========================
# DB
# =========================

async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            is_pro INTEGER DEFAULT 0,
            signals_today INTEGER DEFAULT 0,
            last_reset TEXT
        )
        """)
        await db.commit()

async def add_user(user_id):
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
        INSERT OR IGNORE INTO users (user_id, last_reset)
        VALUES (?, ?)
        """, (user_id, datetime.now().date().isoformat()))
        await db.commit()

async def update_and_check_limit(user_id, is_pro):
    """Атомарно обновляет счётчик и проверяет лимит.
    Возвращает True если можно отправить сигнал."""
    if is_pro:
        return True
    
    async with aiosqlite.connect(DB) as db:
        cursor = await db.execute(
            "UPDATE users SET signals_today = signals_today + 1 "
            "WHERE user_id = ? AND signals_today < ? "
            "RETURNING signals_today",
            (user_id, FREE_LIMIT)
        )
        result = await cursor.fetchone()
        await db.commit()
        return result is not None

async def reset_daily():
    async with aiosqlite.connect(DB) as db:
        today = datetime.now().date().isoformat()
        await db.execute("""
        UPDATE users
        SET signals_today = 0, last_reset = ?
        WHERE last_reset != ?
        """, (today, today))
        await db.commit()

# =========================
# INDICATORS
# =========================

def rsi(prices, period=14):
    """Корректный расчёт RSI"""
    if len(prices) < period + 1:
        return None
    
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    
    # Берём последние period изменений
    gains = [max(d, 0) for d in deltas[-period:]]
    losses = [abs(min(d, 0)) for d in deltas[-period:]]
    
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def sma(prices, period=20):
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period

def atr(closes, period=14):
    """Упрощённый ATR для фильтрации волатильности"""
    if len(closes) < period + 1:
        return 0
    
    changes = [abs(closes[i] - closes[i-1]) for i in range(1, len(closes))]
    return sum(changes[-period:]) / period

# =========================
# DATA
# =========================

async def get_tickers(session):
    """Получение тикеров с кешированием на 1 час"""
    now = time.time()
    
    if tickers_cache["data"] and (now - tickers_cache["timestamp"]) < 3600:
        logger.debug("Using cached tickers")
        return tickers_cache["data"]
    
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
    
    try:
        async with session.get(url, timeout=30) as r:
            r.raise_for_status()
            data = await r.json()
        
        tickers = [x[0] for x in data["securities"]["data"]]
        tickers_cache["data"] = tickers
        tickers_cache["timestamp"] = now
        logger.info(f"Loaded {len(tickers)} tickers")
        return tickers
    
    except Exception as e:
        logger.error(f"Failed to get tickers: {e}")
        return tickers_cache["data"] or []

async def get_candles(session, ticker):
    """Получение свечей с обработкой ошибок"""
    url = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/"
        f"securities/{ticker}/candles.json?interval=60&limit=100"
    )
    
    try:
        async with session.get(url, timeout=15) as r:
            r.raise_for_status()
            data = await r.json()
        
        candles = data.get("candles", {}).get("data", [])
        
        if not candles:
            return [], []
        
        closes = [c[4] for c in candles]
        volumes = [c[5] for c in candles]
        
        return closes, volumes
    
    except Exception as e:
        logger.debug(f"Failed to get candles for {ticker}: {e}")
        return [], []

# =========================
# UTIL
# =========================

def tv(ticker):
    return f"https://www.tradingview.com/chart/?symbol=MOEX:{ticker}"

def is_signal_duplicate(ticker, signal_type):
    """Проверка на дубликат сигнала за последний час"""
    now = datetime.now()
    last_sent = sent_signals[ticker].get(signal_type)
    
    if last_sent and (now - last_sent) < timedelta(seconds=SIGNAL_COOLDOWN):
        return True
    
    sent_signals[ticker][signal_type] = now
    return False

# =========================
# ANALYSIS
# =========================

async def analyze(session, ticker):
    try:
        closes, volumes = await get_candles(session, ticker)
        
        if len(closes) < 50:
            return
        
        price = closes[-1]
        
        # Проверка объёмов
        if len(volumes) < 22:
            return
        
        recent_volumes = volumes[-21:-1]  # 20 предыдущих свечей
        avg_vol = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
        cur_vol = volumes[-1]
        
        rvol = cur_vol / avg_vol if avg_vol > 0 else 0
        
        # Фильтр: объём должен быть не менее 1000 и всплеск в 1.5 раза
        if avg_vol < 1000 or rvol < 1.5:
            return
        
        # Фильтр волатильности (цена должна меняться хотя бы на 1% в среднем)
        volatility = atr(closes)
        if volatility / price < 0.01:
            return
        
        rsi_val = rsi(closes)
        sma_val = sma(closes)
        
        signals = []
        signal_type = "UNKNOWN"
        
        # RSI сигналы
        if rsi_val is not None:
            if rsi_val < 30:
                signals.append("🟢 Перепроданность")
                signal_type = "REVERSAL"
            elif rsi_val > 70:
                signals.append("🔴 Перекупленность")
                signal_type = "REVERSAL"
        
        # SMA сигналы
        if sma_val is not None:
            if price > sma_val:
                signals.append("📈 Тренд вверх (выше SMA20)")
                signal_type = signal_type if signal_type != "UNKNOWN" else "TREND"
            else:
                signals.append("📉 Тренд вниз (ниже SMA20)")
                signal_type = signal_type if signal_type != "UNKNOWN" else "TREND"
        
        # Пробой (исправлено: проверяем предыдущие свечи)
        if len(closes) >= 21:
            prev_high = max(closes[-21:-1])
            prev_low = min(closes[-21:-1])
            
            if price > prev_high:
                signals.append("🚀 Пробой вверх")
                signal_type = "BREAKOUT"
            elif price < prev_low:
                signals.append("📉 Пробой вниз")
                signal_type = "BREAKOUT"
        
        # Нужно минимум 2 сигнала
        if len(signals) < 2:
            return
        
        # Проверка на дубликат
        if is_signal_duplicate(ticker, signal_type):
            logger.debug(f"Duplicate signal skipped: {ticker} {signal_type}")
            return
        
        tv_link = tv(ticker)
        
        text = f"""
<b><a href="{tv_link}">{ticker}</a></b>

Тип: <b>{SIGNAL_TRANSLATION.get(signal_type, "НЕИЗВЕСТНО")}</b>
Цена: {price:.2f} ₽
Объём: {cur_vol:,.0f}
RVOL: {rvol:.1f}x
RSI: {rsi_val:.1f}
ATR: {volatility:.2f}

📊 Сигналы:
""" + "\n".join(signals)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Открыть график", url=tv_link)]
        ])
        
        await send(text, keyboard, ticker, signal_type)
    
    except Exception as e:
        logger.error(f"Error analyzing {ticker}: {e}")

# =========================
# SEND
# =========================

async def send(text, keyboard, ticker, signal_type):
    """Рассылка сигнала всем пользователям с проверкой лимитов"""
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT user_id, is_pro FROM users"
        ) as cur:
            users = await cur.fetchall()
    
    sent_count = 0
    
    for uid, is_pro in users:
        # Проверяем лимит и атомарно обновляем счётчик
        can_send = await update_and_check_limit(uid, is_pro)
        
        if not can_send:
            logger.debug(f"User {uid} reached daily limit")
            continue
        
        try:
            await bot.send_message(uid, text, reply_markup=keyboard)
            sent_count += 1
            await asyncio.sleep(0.05)  # Антиспам пауза
        except Exception as e:
            logger.warning(f"Failed to send to {uid}: {e}")
    
    logger.info(f"Signal {ticker} {signal_type} sent to {sent_count} users")

# =========================
# SCAN
# =========================

async def scan():
    """Сканирование всех тикеров"""
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tickers = await get_tickers(session)
        
        if not tickers:
            logger.warning("No tickers to scan")
            return
        
        logger.info(f"Scanning {len(tickers)} tickers...")
        start_time = time.time()
        
        processed = 0
        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i:i+BATCH_SIZE]
            await asyncio.gather(*[analyze(session, t) for t in batch])
            processed += len(batch)
            await asyncio.sleep(0.5)  # Пауза между батчами
        
        elapsed = time.time() - start_time
        logger.info(f"Scan completed: {processed} tickers in {elapsed:.1f}s")

# =========================
# COMMANDS
# =========================

@dp.message(Command("start"))
async def start(m: types.Message):
    await add_user(m.from_user.id)
    
    text = (
        "📊 <b>MOEX Scanner запущен!</b>\n\n"
        f"Бесплатный лимит: <b>{FREE_LIMIT}</b> сигналов в день\n"
        "PRO-пользователи получают безлимитный доступ\n\n"
        "Типы сигналов:\n"
        "• ПРОБОЙ — выход из диапазона\n"
        "• РАЗВОРОТ — перекупленность/перепроданность\n"
        "• ТРЕНД — направленное движение"
    )
    
    await m.answer(text)

@dp.message(Command("status"))
async def status(m: types.Message):
    """Показывает статистику пользователя"""
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT is_pro, signals_today FROM users WHERE user_id=?",
            (m.from_user.id,)
        ) as cur:
            result = await cur.fetchone()
    
    if not result:
        await m.answer("Вы не зарегистрированы. Нажмите /start")
        return
    
    is_pro, used = result
    
    if is_pro:
        text = f"✅ PRO-аккаунт активен\nИспользовано сегодня: {used} сигналов (безлимит)"
    else:
        remaining = max(0, FREE_LIMIT - used)
        text = (
            f"📊 Бесплатный аккаунт\n"
            f"Использовано: {used}/{FREE_LIMIT}\n"
            f"Осталось: {remaining}"
        )
    
    await m.answer(text)

# =========================
# LOOP
# =========================

async def loop():
    """Главный цикл сканирования с обработкой ошибок"""
    logger.info("Scanner loop started")
    
    while True:
        try:
            await reset_daily()
            await scan()
        except Exception as e:
            logger.error(f"Scan iteration failed: {e}", exc_info=True)
            await asyncio.sleep(60)  # Ждём минуту при ошибке
        else:
            logger.info(f"Waiting {SCAN_INTERVAL}s until next scan...")
            await asyncio.sleep(SCAN_INTERVAL)

# =========================
# MAIN
# =========================

async def main():
    await init_db()
    
    # Запускаем цикл сканирования
    loop_task = asyncio.create_task(loop())
    
    logger.info("Bot starting...")
    
    try:
        await dp.start_polling(bot)
    finally:
        # Graceful shutdown
        logger.info("Shutting down...")
        loop_task.cancel()
        try:
            await loop_task
        except asyncio.CancelledError:
            pass
        
        await bot.session.close()
        logger.info("Bot stopped")

if __name__ == "__main__":
    asyncio.run(main())