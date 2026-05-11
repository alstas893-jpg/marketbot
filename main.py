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
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("scanner")

# =========================
# ENV
# =========================

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
FREE_LIMIT = int(os.getenv("FREE_LIMIT", 3))
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 300))
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", 3600))

if not TOKEN:
    raise ValueError("BOT_TOKEN not found")

# =========================
# BOT
# =========================

bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

DB = "users.db"

sent_cache = defaultdict(dict)

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


async def add_user(uid):
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
        INSERT OR IGNORE INTO users (user_id, last_reset)
        VALUES (?, ?)
        """, (uid, datetime.now().date().isoformat()))
        await db.commit()


async def get_users():
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT user_id, is_pro FROM users")
        return await cur.fetchall()


async def update_limit(uid, is_pro):
    if is_pro:
        return True

    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("""
        UPDATE users
        SET signals_today = signals_today + 1
        WHERE user_id = ? AND signals_today < ?
        """, (uid, FREE_LIMIT))

        await db.commit()
        return cur.rowcount > 0


# =========================
# INDICATORS
# =========================

def rsi(prices, period=14):
    if len(prices) < period + 1:
        return None

    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

    gains = [max(x, 0) for x in deltas[-period:]]
    losses = [abs(min(x, 0)) for x in deltas[-period:]]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100 if avg_gain > 0 else 50

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def sma(prices, period=20):
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def atr(closes):
    if len(closes) < 15:
        return 0
    return sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes))) / len(closes)


# =========================
# MOEX DATA
# =========================

async def get_tickers(session):
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"

    try:
        async with session.get(url, timeout=20) as r:
            data = await r.json()
            return [x[0] for x in data["securities"]["data"]]
    except Exception as e:
        log.error(f"tickers error: {e}")
        return []


async def get_candles(session, ticker):
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?interval=60&limit=80"

    try:
        async with session.get(url, timeout=15) as r:
            data = await r.json()

        candles = data.get("candles", {}).get("data", [])
        if not candles:
            return [], []

        closes = [c[4] for c in candles]
        volumes = [c[5] for c in candles]

        return closes, volumes

    except Exception:
        return [], []


# =========================
# SIGNAL LOGIC
# =========================

def is_duplicate(ticker, sig):
    now = datetime.now()
    last = sent_cache[ticker].get(sig)

    if last and (now - last) < timedelta(seconds=SIGNAL_COOLDOWN):
        return True

    sent_cache[ticker][sig] = now
    return False


def format_volume(v):
    if v > 1e9:
        return f"{v/1e9:.1f}B"
    if v > 1e6:
        return f"{v/1e6:.1f}M"
    if v > 1e3:
        return f"{v/1e3:.1f}K"
    return str(v)


# =========================
# ANALYZE
# =========================

async def analyze(session, ticker, sem):
    async with sem:
        try:
            closes, volumes = await get_candles(session, ticker)

            if len(closes) < 40:
                return

            price = closes[-1]
            rsi_val = rsi(closes)
            sma_val = sma(closes)
            vol = volumes[-1] if volumes else 0

            avg_vol = sum(volumes[-20:]) / 20 if len(volumes) > 20 else 0
            rvol = vol / avg_vol if avg_vol else 0

            signals = []
            sig_type = "TREND"

            # RSI
            if rsi_val is not None:
                if rsi_val < 30:
                    signals.append("RSI перепродан")
                    sig_type = "REVERSAL"
                elif rsi_val > 70:
                    signals.append("RSI перекуплен")
                    sig_type = "REVERSAL"

            # SMA
            if sma_val:
                if price > sma_val:
                    signals.append("Цена выше SMA")
                else:
                    signals.append("Цена ниже SMA")

            # breakout
            if price > max(closes[-20:]):
                signals.append("Пробой вверх")
                sig_type = "BREAKOUT"

            if len(signals) < 2:
                return

            if is_duplicate(ticker, sig_type):
                return

            text = f"""
<b>{ticker}</b>

Тип: <b>{sig_type}</b>
Цена: {price:.2f}
RSI: {rsi_val:.1f if rsi_val else 'N/A'}
RVOL: {rvol:.2f}

📊
""" + "\n".join(signals)

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="TradingView", url=f"https://www.tradingview.com/chart/?symbol=MOEX:{ticker}")]
            ])

            await send_signal(text, kb, ticker, sig_type)

        except Exception as e:
            log.error(f"{ticker}: {e}")


# =========================
# SEND
# =========================

async def send_signal(text, kb, ticker, sig_type):
    users = await get_users()

    if not users:
        return

    sent = 0

    for uid, is_pro in users:
        if not await update_limit(uid, is_pro):
            continue

        try:
            await bot.send_message(uid, text, reply_markup=kb)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass

    log.info(f"{ticker} -> {sig_type} sent to {sent} users")


# =========================
# SCAN ENGINE
# =========================

async def scan():
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tickers = await get_tickers(session)

        if not tickers:
            log.warning("no tickers")
            return

        sem = asyncio.Semaphore(8)

        tasks = [
            analyze(session, t, sem)
            for t in tickers[:200]
        ]

        await asyncio.gather(*tasks)


# =========================
# LOOP
# =========================

async def loop():
    while True:
        try:
            await scan()
        except Exception as e:
            log.error(f"loop error: {e}")

        await asyncio.sleep(SCAN_INTERVAL)


# =========================
# COMMANDS
# =========================

@dp.message(Command("start"))
async def start(m: types.Message):
    await add_user(m.from_user.id)
    await m.answer("Бот запущен")


@dp.message(Command("status"))
async def status(m: types.Message):
    await m.answer("OK")


# =========================
# MAIN
# =========================

async def main():
    await init_db()

    asyncio.create_task(loop())

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())