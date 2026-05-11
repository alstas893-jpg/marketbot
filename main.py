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
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 300))
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", 3600))

if not TOKEN:
    raise ValueError("BOT_TOKEN not found in .env file")

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
    """Инициализация базы данных"""
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
        logger.info("Database initialized")

async def add_user(user_id):
    """Добавление нового пользователя"""
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
        INSERT OR IGNORE INTO users (user_id, last_reset)
        VALUES (?, ?)
        """, (user_id, datetime.now().date().isoformat()))
        await db.commit()
        logger.info(f"New user registered: {user_id}")

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
    """Сброс дневных лимитов"""
    async with aiosqlite.connect(DB) as db:
        today = datetime.now().date().isoformat()
        cursor = await db.execute("""
        UPDATE users
        SET signals_today = 0, last_reset = ?
        WHERE last_reset != ?
        """, (today, today))
        await db.commit()
        if cursor.rowcount > 0:
            logger.info(f"Daily limits reset for {cursor.rowcount} users")

# =========================
# INDICATORS
# =========================

def rsi(prices, period=14):
    """Расчёт RSI"""
    if len(prices) < period + 1:
        return None
    
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    
    gains = [max(d, 0) for d in deltas[-period:]]
    losses = [abs(min(d, 0)) for d in deltas[-period:]]
    
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def sma(prices, period=20):
    """Расчёт SMA"""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period

def atr(closes, period=14):
    """Упрощённый ATR для фильтрации волатильности"""
    if len(closes) < period + 1:
        return 0
    
    changes = [abs(closes[i] - closes[i-1]) for i in range(1, len(closes))]
    return sum(changes[-period:]) / period

def ema(prices, period=20):
    """Расчёт EMA"""
    if len(prices) < period:
        return None
    
    multiplier = 2 / (period + 1)
    ema_val = sum(prices[:period]) / period
    
    for price in prices[period:]:
        ema_val = (price - ema_val) * multiplier + ema_val
    
    return ema_val

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
    """Ссылка на TradingView"""
    return f"https://www.tradingview.com/chart/?symbol=MOEX:{ticker}"

def is_signal_duplicate(ticker, signal_type):
    """Проверка на дубликат сигнала"""
    now = datetime.now()
    last_sent = sent_signals[ticker].get(signal_type)
    
    if last_sent and (now - last_sent) < timedelta(seconds=SIGNAL_COOLDOWN):
        return True
    
    sent_signals[ticker][signal_type] = now
    return False

def format_volume(volume):
    """Форматирование объёма"""
    if volume >= 1_000_000_000:
        return f"{volume/1_000_000_000:.1f}B"
    elif volume >= 1_000_000:
        return f"{volume/1_000_000:.1f}M"
    elif volume >= 1_000:
        return f"{volume/1_000:.1f}K"
    return str(volume)

# =========================
# ANALYSIS
# =========================

async def analyze(session, ticker):
    """Анализ тикера на сигналы"""
    try:
        closes, volumes = await get_candles(session, ticker)
        
        if len(closes) < 50:
            return
        
        price = closes[-1]
        
        # Проверка объёмов
        if len(volumes) < 22:
            return
        
        recent_volumes = volumes[-21:-1]
        avg_vol = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
        cur_vol = volumes[-1]
        
        rvol = cur_vol / avg_vol if avg_vol > 0 else 0
        
        # Фильтры
        if avg_vol < 1000 or rvol < 1.5:
            return
        
        volatility = atr(closes)
        if volatility / price < 0.01:
            return
        
        rsi_val = rsi(closes)
        sma_val = sma(closes)
        ema_val = ema(closes)
        
        signals = []
        signal_type = "UNKNOWN"
        
        # RSI сигналы
        if rsi_val is not None:
            if rsi_val < 30:
                signals.append("🟢 Перепроданность (RSI < 30)")
                signal_type = "REVERSAL"
            elif rsi_val > 70:
                signals.append("🔴 Перекупленность (RSI > 70)")
                signal_type = "REVERSAL"
        
        # SMA сигналы
        if sma_val is not None:
            if price > sma_val:
                signals.append("📈 Цена выше SMA20")
                if signal_type == "UNKNOWN":
                    signal_type = "TREND"
            else:
                signals.append("📉 Цена ниже SMA20")
                if signal_type == "UNKNOWN":
                    signal_type = "TREND"
        
        # EMA сигналы
        if ema_val is not None:
            if price > ema_val:
                signals.append("📈 Цена выше EMA20")
            else:
                signals.append("📉 Цена ниже EMA20")
        
        # Пробой
        if len(closes) >= 21:
            prev_high = max(closes[-21:-1])
            prev_low = min(closes[-21:-1])
            
            if price > prev_high:
                signals.append("🚀 Пробой 20-периодного максимума")
                signal_type = "BREAKOUT"
            elif price < prev_low:
                signals.append("📉 Пробой 20-периодного минимума")
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

Тип сигнала: <b>{SIGNAL_TRANSLATION.get(signal_type, "НЕИЗВЕСТНО")}</b>
Цена: <b>{price:.2f} ₽</b>
Объём: <b>{format_volume(cur_vol)}</b>
RVOL: <b>{rvol:.1f}x</b>
RSI(14): <b>{rsi_val:.1f}</b>
ATR(14): <b>{volatility:.2f}</b>

📊 Сигналы:
""" + "\n".join(f"• {s}" for s in signals)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Открыть график", url=tv_link)],
            [
                InlineKeyboardButton(text="📈 TradingView", url=tv_link),
                InlineKeyboardButton(text="📋 MOEX", url=f"https://www.moex.com/ru/issue.aspx?code={ticker}")
            ]
        ])
        
        await send(text, keyboard, ticker, signal_type)
    
    except Exception as e:
        logger.error(f"Error analyzing {ticker}: {e}")

# =========================
# SEND
# =========================

async def send(text, keyboard, ticker, signal_type):
    """Рассылка сигнала всем пользователям"""
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT user_id, is_pro FROM users"
        ) as cur:
            users = await cur.fetchall()
    
    if not users:
        logger.warning("No users in database to send signals to")
        return
    
    sent_count = 0
    
    for uid, is_pro in users:
        can_send = await update_and_check_limit(uid, is_pro)
        
        if not can_send:
            logger.debug(f"User {uid} reached daily limit")
            continue
        
        try:
            await bot.send_message(uid, text, reply_markup=keyboard)
            sent_count += 1
            await asyncio.sleep(0.05)
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
            await asyncio.sleep(0.5)
        
        elapsed = time.time() - start_time
        logger.info(f"Scan completed: {processed} tickers in {elapsed:.1f}s")

# =========================
# COMMANDS
# =========================

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    """Регистрация пользователя"""
    await add_user(m.from_user.id)
    
    text = (
        "📊 <b>MOEX Scanner</b>\n\n"
        "Я анализирую рынок акций Московской биржи "
        "и присылаю сигналы о потенциальных движениях.\n\n"
        f"🔹 Бесплатный лимит: <b>{FREE_LIMIT}</b> сигналов в день\n"
        "🔹 PRO: безлимитные сигналы\n\n"
        "📈 <b>Типы сигналов:</b>\n"
        "• <b>ПРОБОЙ</b> — выход цены из диапазона\n"
        "• <b>РАЗВОРОТ</b> — перекупленность/перепроданность\n"
        "• <b>ТРЕНД</b> — направленное движение\n\n"
        "⚙️ Команды:\n"
        "/status — ваша статистика\n"
        "/debug — проверка сканера\n\n"
        "Сигналы приходят автоматически каждые 5 минут!"
    )
    
    await m.answer(text)

@dp.message(Command("status"))
async def cmd_status(m: types.Message):
    """Статистика пользователя"""
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT is_pro, signals_today FROM users WHERE user_id=?",
            (m.from_user.id,)
        ) as cur:
            result = await cur.fetchone()
    
    if not result:
        await m.answer("❌ Вы не зарегистрированы.\nИспользуйте /start")
        return
    
    is_pro, used = result
    
    if is_pro:
        text = (
            "✅ <b>PRO-аккаунт</b>\n"
            f"Использовано сегодня: {used} сигналов\n"
            "Лимит: безлимитный"
        )
    else:
        remaining = max(0, FREE_LIMIT - used)
        text = (
            "📊 <b>Бесплатный аккаунт</b>\n"
            f"Использовано: {used}/{FREE_LIMIT}\n"
            f"Осталось: {remaining}\n\n"
            "Для увеличения лимита обратитесь к администратору."
        )
    
    await m.answer(text)

@dp.message(Command("debug"))
async def cmd_debug(m: types.Message):
    """Отладка: проверка первых 30 тикеров"""
    msg = await m.answer("🔍 Сканирую первые 30 тикеров...")
    
    timeout = aiohttp.ClientTimeout(total=30)
    signals = []
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tickers = await get_tickers(session)
        
        if not tickers:
            await msg.edit_text("❌ Не удалось загрузить тикеры")
            return
        
        for ticker in tickers[:30]:
            try:
                closes, volumes = await get_candles(session, ticker)
                
                if len(closes) < 50:
                    continue
                
                price = closes[-1]
                rsi_val = rsi(closes)
                sma_val = sma(closes)
                avg_vol = sum(volumes[-21:-1]) / 20 if len(volumes) >= 21 else 0
                rvol = volumes[-1] / avg_vol if avg_vol > 0 else 0
                
                if rsi_val and (rsi_val < 35 or rsi_val > 65):
                    signals.append(
                        f"{'🟢' if rsi_val < 35 else '🔴'} "
                        f"<b>{ticker}</b>: RSI={rsi_val:.1f}, "
                        f"Цена={price:.2f}, RVOL={rvol:.1f}x"
                    )
            except Exception as e:
                logger.debug(f"Debug error {ticker}: {e}")
        
        if signals:
            text = (
                "📊 <b>Найдены отклонения RSI:</b>\n\n" +
                "\n".join(signals[:15]) +
                "\n\n⚠️ Это тестовый вывод. Реальные сигналы учитывают больше факторов."
            )
        else:
            text = "✅ В первых 30 тикерах нет сильных отклонений RSI"
        
        await msg.edit_text(text)

@dp.message(Command("scan"))
async def cmd_scan(m: types.Message):
    """Ручной запуск сканирования"""
    msg = await m.answer("🔍 Запускаю полное сканирование...")
    
    try:
        start = time.time()
        await scan()
        elapsed = time.time() - start
        await msg.edit_text(f"✅ Сканирование завершено за {elapsed:.1f} сек")
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка сканирования: {e}")

@dp.message()
async def handle_all_messages(m: types.Message):
    """Обработчик всех остальных сообщений"""
    await m.answer(
        "👋 Я работаю в автоматическом режиме.\n\n"
        "📋 <b>Доступные команды:</b>\n"
        "/start — регистрация\n"
        "/status — статистика\n"
        "/debug — проверка сканера\n"
        "/scan — запустить сканирование\n\n"
        "Сигналы приходят автоматически!"
    )

# =========================
# LOOP
# =========================

async def scanner_loop():
    """Главный цикл сканирования"""
    logger.info("Scanner loop started")
    
    while True:
        try:
            await reset_daily()
            await scan()
        except Exception as e:
            logger.error(f"Scan iteration failed: {e}", exc_info=True)
            await asyncio.sleep(60)
        else:
            logger.info(f"Waiting {SCAN_INTERVAL}s until next scan...")
            await asyncio.sleep(SCAN_INTERVAL)

# =========================
# MAIN
# =========================

async def main():
    """Точка входа"""
    logger.info("Initializing...")
    
    # Инициализация БД
    await init_db()
    
    # Запуск сканера в фоне
    scanner_task = asyncio.create_task(scanner_loop())
    
    logger.info("Bot starting...")
    
    try:
        # Запуск поллинга
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
    finally:
        # Корректное завершение
        logger.info("Shutting down...")
        scanner_task.cancel()
        try:
            await scanner_task
        except asyncio.CancelledError:
            pass
        
        await bot.session.close()
        logger.info("Bot stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")