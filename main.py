import asyncio
import aiohttp
import aiosqlite
import os
import logging

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

# =========================
# ANTI-DUPLICATES (BY CANDLE)
# =========================

sent_signals = set()

def is_duplicate(ticker, candle_time, sig_type):
    key = (ticker, candle_time, sig_type)
    if key in sent_signals:
        return True
    sent_signals.add(key)
    return False

# =========================
# PRICE NORMALIZATION (MOEX FIX)
# =========================

def normalize_price(price: float) -> float:
    if price is None:
        return 0

    # MOEX garbage protection
    if price > 10_000_000:
        return price / 1000
    if price > 1_000_000:
        return price / 100
    if price <= 0:
        return 0

    return price

# =========================
# RSI (CLEAN VERSION)
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
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# =========================
# SMA
# =========================

def sma(prices, period=20):
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period

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
            return [], [], []

        times = [c[0] for c in candles]

        # RAW CLOSE
        raw_closes = [c[4] for c in candles]
        volumes = [c[5] for c in candles]

        # CLEAN PRICE
        closes = [normalize_price(c) for c in raw_closes]

        return times, closes, volumes

    except Exception as e:
        log.error(f"candles error: {e}")
        return [], [], []

# =========================
# ANALYSIS
# =========================

async def analyze(session, ticker, sem):
    async with sem:
        try:
            times, closes, volumes = await get_candles(session, ticker)

            if len(closes) < 40:
                return

            candle_time = times[-1]

            price = closes[-1]
            if price <= 0:
                return

            rsi_val = rsi(closes)
            sma_val = sma(closes)

            signals = []
            sig_type = "TREND"

            # RSI FIXED
            if rsi_val is not None:
                if rsi_val < 30:
                    signals.append("RSI перепродан")
                    sig_type = "REVERSAL"
                elif rsi_val > 70:
                    signals.append("RSI перекуплен")
                    sig_type = "REVERSAL"

            # SMA
            if sma_val:
                signals.append("Цена относительно SMA")

            # breakout
            if price > max(closes[-20:]):
                signals.append("Пробой вверх")
                sig_type = "BREAKOUT"

            if len(signals) < 2:
                return

            # anti-duplicate by candle
            if is_duplicate(ticker, candle_time, sig_type):
                return

            tv_link = f"https://www.tradingview.com/chart/?symbol=MOEX:{ticker}"

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="TradingView", url=tv_link)]
            ])

            text = f"""
<b>{ticker}</b>

Тип: <b>{sig_type}</b>
Цена: {price:.2f}

📊 Сигналы:
""" + "\n".join(signals)

            users = await get_users()

            for uid, is_pro in users:
                try:
                    await bot.send_message(uid, text, reply_markup=kb)
                except:
                    pass

        except Exception as e:
            log.error(f"{ticker}: {e}")

# =========================
# USERS
# =========================

async def get_users():
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT user_id, is_pro FROM users")
        return await cur.fetchall()

# =========================
# SCAN
# =========================

async def scan():
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tickers = await get_tickers(session)

        if not tickers:
            return

        sem = asyncio.Semaphore(8)

        await asyncio.gather(*[
            analyze(session, t, sem)
            for t in tickers[:200]
        ])

# =========================
# LOOP
# =========================

async def loop():
    while True:
        try:
            await scan()
        except Exception as e:
            log.error(f"loop error: {e}")

        await asyncio.sleep(300)

# =========================
# MAIN
# =========================

async def main():
    asyncio.create_task(loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())