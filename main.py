import asyncio
import aiohttp
import aiosqlite
import os

from datetime import datetime
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# =========================
# ENV
# =========================

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
FREE_LIMIT = int(os.getenv("FREE_LIMIT", 3))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 12))

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

async def update_signals(user_id):
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
        UPDATE users SET signals_today = signals_today + 1
        WHERE user_id=?
        """, (user_id,))
        await db.commit()

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
    if len(prices) < period:
        return None

    gains, losses = [], []

    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def sma(prices, period=20):
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period

# =========================
# DATA
# =========================

async def get_tickers(session):
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
    async with session.get(url) as r:
        data = await r.json()
    return [x[0] for x in data["securities"]["data"]]

async def get_candles(session, ticker):
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?interval=60&limit=100"
    async with session.get(url) as r:
        data = await r.json()

    closes = [c[4] for c in data["candles"]["data"]]
    volumes = [c[5] for c in data["candles"]["data"]]

    return closes, volumes

# =========================
# UTIL
# =========================

def tv(ticker):
    return f"https://www.tradingview.com/chart/?symbol=MOEX:{ticker}"

# =========================
# ANALYSIS
# =========================

async def analyze(session, ticker):
    try:
        closes, volumes = await get_candles(session, ticker)

        if len(closes) < 50:
            return

        price = closes[-1]

        avg_vol = sum(volumes[-21:-1]) / 20
        cur_vol = volumes[-1]

        rvol = cur_vol / avg_vol if avg_vol else 0

        if avg_vol < 100000 or rvol < 1.2:
            return

        rsi_val = rsi(closes)
        sma_val = sma(closes)

        signals = []
        signal_type = "UNKNOWN"

        if rsi_val:
            if rsi_val < 30:
                signals.append("🟢 Перепроданность")
                signal_type = "REVERSAL"
            elif rsi_val > 70:
                signals.append("🔴 Перекупленность")
                signal_type = "REVERSAL"

        if sma_val:
            if price > sma_val:
                signals.append("📈 Тренд вверх")
                signal_type = signal_type if signal_type != "UNKNOWN" else "TREND"
            else:
                signals.append("📉 Тренд вниз")
                signal_type = signal_type if signal_type != "UNKNOWN" else "TREND"

        high = max(closes[-20:])
        low = min(closes[-20:])

        if price > high:
            signals.append("🚀 Пробой вверх")
            signal_type = "BREAKOUT"

        if price < low:
            signals.append("📉 Пробой вниз")
            signal_type = "BREAKOUT"

        if len(signals) < 2:
            return

        tv_link = tv(ticker)

        text = f"""
<b><a href="{tv_link}">{ticker}</a></b>

Тип: <b>{SIGNAL_TRANSLATION.get(signal_type, "НЕИЗВЕСТНО")}</b>
Цена: {price}

📊 Сигналы:
""" + "\n".join(signals)

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Открыть график", url=tv_link)]
        ])

        await send(text, keyboard)

    except:
        pass

# =========================
# SEND
# =========================

async def send(text, keyboard):
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT user_id, is_pro, signals_today FROM users") as cur:
            users = await cur.fetchall()

    for uid, is_pro, count in users:
        if not is_pro and count >= FREE_LIMIT:
            continue

        try:
            await bot.send_message(uid, text, reply_markup=keyboard)
            await update_signals(uid)
        except:
            pass

# =========================
# SCAN
# =========================

async def scan():
    async with aiohttp.ClientSession() as session:
        tickers = await get_tickers(session)

        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i:i+BATCH_SIZE]
            await asyncio.gather(*[analyze(session, t) for t in batch])
            await asyncio.sleep(1)

# =========================
# COMMANDS
# =========================

@dp.message(Command("start"))
async def start(m: types.Message):
    await add_user(m.from_user.id)
    await m.answer("📊 Scanner запущен")

# =========================
# LOOP
# =========================

async def loop():
    while True:
        await reset_daily()
        await scan()
        await asyncio.sleep(300)

# =========================
# MAIN
# =========================

async def main():
    await init_db()
    asyncio.create_task(loop())
    print("Bot started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())