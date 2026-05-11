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
DB_PATH = "users.db"

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

# =========================
# ANTI DUPLICATES BY CANDLE
# =========================

sent_signals = set()

def is_duplicate(ticker, candle_time, sig_type):
    key = (ticker, candle_time, sig_type)
    if key in sent_signals:
        return True
    sent_signals.add(key)
    return False

# =========================
# DB INIT (ПРАВИЛЬНАЯ)
# =========================

async def init_db():
    """
    ЕДИНСТВЕННОЕ место создания БД
    гарантированно вызывается ДО scan()
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            is_pro INTEGER DEFAULT 0,
            signals_today INTEGER DEFAULT 0
        )
        """)
        await db.commit()

    log.info("Database initialized")

# =========================
# USERS
# =========================

async def add_user(uid):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT OR IGNORE INTO users (user_id)
        VALUES (?)
        """, (uid,))
        await db.commit()


async def get_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id, is_pro FROM users")
        return await cur.fetchall()


async def update_limit(uid, is_pro):
    if is_pro:
        return True

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        UPDATE users
        SET signals_today = signals_today + 1
        WHERE user_id = ? AND signals_today < 3
        """, (uid,))

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
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def sma(prices, period=20):
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period

# =========================
# MOEX DATA
# =========================

async def get_tickers(session):
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"

    async with session.get(url, timeout=20) as r:
        data = await r.json()

    return [x[0] for x in data["securities"]["data"]]


async def get_candles(session, ticker):
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?interval=60&limit=80"

    async with session.get(url, timeout=15) as r:
        data = await r.json()

    candles = data.get("candles", {}).get("data", [])

    if not candles:
        return [], [], []

    times = [c[0] for c in candles]
    closes = [c[4] for c in candles]
    volumes = [c[5] for c in candles]

    return times, closes, volumes

# =========================
# SEND
# =========================

async def send_signal(text, kb, ticker, sig_type):
    users = await get_users()

    for uid, is_pro in users:
        if not await update_limit(uid, is_pro):
            continue

        try:
            await bot.send_message(uid, text, reply_markup=kb)
        except:
            pass

# =========================
# ANALYZE
# =========================

async def analyze(session, ticker, sem):
    async with sem:
        try:
            times, closes, volumes = await get_candles(session, ticker)

            if len(closes) < 40:
                return

            candle_time = times[-1]
            price = closes[-1]

            rsi_val = rsi(closes)
            sma_val = sma(closes)

            signals = []
            sig_type = "TREND"

            if rsi_val is not None:
                if rsi_val < 30:
                    signals.append("RSI перепродан")
                    sig_type = "REVERSAL"
                elif rsi_val > 70:
                    signals.append("RSI перекуплен")
                    sig_type = "REVERSAL"

            if sma_val:
                signals.append("Цена относительно SMA")

            if price > max(closes[-20:]):
                signals.append("Пробой вверх")
                sig_type = "BREAKOUT"

            if len(signals) < 2:
                return

            # 🔥 АНТИДУБЛИКАТ ПО СВЕЧЕ
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

            await send_signal(text, kb, ticker, sig_type)

        except Exception as e:
            log.error(f"{ticker}: {e}")

# =========================
# SCAN LOOP
# =========================

async def scan():
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tickers = await get_tickers(session)

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
# COMMANDS
# =========================

@dp.message(Command("start"))
async def start(m: types.Message):
    await add_user(m.from_user.id)
    await m.answer("Бот запущен")


# =========================
# MAIN (ПРАВИЛЬНЫЙ ПОРЯДОК)
# =========================

async def main():
    # 1. СНАЧАЛА БАЗА
    await init_db()

    # 2. ПОТОМ СКАНЕР
    asyncio.create_task(loop())

    # 3. ПОТОМ BOT
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())