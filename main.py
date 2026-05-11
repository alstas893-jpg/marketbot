import asyncio
import aiohttp
import aiosqlite
import os
import logging
from datetime import datetime, timedelta

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
# CONFIG
# =========================

# Интервал сканирования в секундах (5 минут)
SCAN_INTERVAL = 300

# Максимальное количество одновременных запросов к API
MAX_CONCURRENT_REQUESTS = 15

# Размер чанка для обработки тикеров
CHUNK_SIZE = 100

# Минимальное количество свечей для анализа
MIN_CANDLES = 40

# =========================
# ANTI DUPLICATES BY CANDLE
# =========================

sent_signals = {}

def clean_old_signals():
    """Очистка старых сигналов (старше 1 часа)"""
    current_time = datetime.now()
    expired_keys = []
    
    for key, timestamp in sent_signals.items():
        if (current_time - timestamp).total_seconds() > 3600:  # 1 час
            expired_keys.append(key)
    
    for key in expired_keys:
        del sent_signals[key]
    
    if len(sent_signals) > 50000:
        sent_signals.clear()
        log.info("Cleared all signal cache due to size limit")

def is_duplicate(ticker, candle_time, sig_type):
    """Проверка на дубликат сигнала"""
    key = (ticker, candle_time, sig_type)
    if key in sent_signals:
        return True
    sent_signals[key] = datetime.now()
    return False

# =========================
# DB INIT
# =========================

async def init_db():
    """Инициализация базы данных"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            is_pro INTEGER DEFAULT 0,
            signals_today INTEGER DEFAULT 0,
            last_reset_date TEXT DEFAULT CURRENT_DATE
        )
        """)
        
        # Индекс для быстрого поиска
        await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_users_signals 
        ON users(user_id, signals_today, last_reset_date)
        """)
        
        await db.commit()

    log.info("Database initialized")

# =========================
# USERS
# =========================

async def add_user(uid):
    """Добавление нового пользователя"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT OR IGNORE INTO users (user_id, last_reset_date)
        VALUES (?, CURRENT_DATE)
        """, (uid,))
        await db.commit()


async def get_users():
    """Получение списка всех активных пользователей"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Сброс счетчика для нового дня
        await db.execute("""
        UPDATE users 
        SET signals_today = 0, last_reset_date = CURRENT_DATE
        WHERE last_reset_date != CURRENT_DATE
        """)
        await db.commit()
        
        cur = await db.execute("SELECT user_id, is_pro FROM users")
        return await cur.fetchall()


async def update_limit(uid, is_pro):
    """Проверка и обновление лимита сигналов"""
    if is_pro:
        return True

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT signals_today, last_reset_date FROM users WHERE user_id = ?
        """, (uid,))
        row = await cur.fetchone()
        
        if not row:
            return False
        
        signals_today, last_reset_date = row
        
        # Сброс счетчика для нового дня
        today = datetime.now().strftime('%Y-%m-%d')
        if last_reset_date != today:
            signals_today = 0
            await db.execute("""
            UPDATE users SET signals_today = 0, last_reset_date = ?
            WHERE user_id = ?
            """, (today, uid))
            await db.commit()
        
        if signals_today >= 3:
            return False
        
        # Атомарное увеличение счетчика
        await db.execute("""
        UPDATE users
        SET signals_today = signals_today + 1
        WHERE user_id = ? AND signals_today < 3
        """, (uid,))
        await db.commit()
        
        return True

# =========================
# INDICATORS
# =========================

def rsi(prices, period=14):
    """Расчет Relative Strength Index"""
    if len(prices) < period + 1:
        return None

    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    
    gains = [max(x, 0) for x in deltas[-period:]]
    losses = [abs(min(x, 0)) for x in deltas[-period:]]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def sma(prices, period=20):
    """Расчет Simple Moving Average"""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def ema(prices, period=20):
    """Расчет Exponential Moving Average"""
    if len(prices) < period:
        return None
    
    multiplier = 2 / (period + 1)
    ema_val = sum(prices[:period]) / period
    
    for price in prices[period:]:
        ema_val = (price - ema_val) * multiplier + ema_val
    
    return ema_val


def macd(prices, fast=12, slow=26, signal=9):
    """Расчет MACD индикатора"""
    if len(prices) < slow + signal:
        return None, None, None
    
    # EMA fast
    ema_fast = ema(prices, fast)
    # EMA slow
    ema_slow = ema(prices, slow)
    
    if ema_fast is None or ema_slow is None:
        return None, None, None
    
    macd_line = ema_fast - ema_slow
    
    # Для signal line используем последние значения MACD
    macd_values = []
    for i in range(slow, len(prices)):
        ema_f = ema(prices[:i+1], fast)
        ema_s = ema(prices[:i+1], slow)
        if ema_f and ema_s:
            macd_values.append(ema_f - ema_s)
    
    if len(macd_values) < signal:
        return macd_line, None, None
    
    signal_line = ema(macd_values, signal)
    histogram = macd_line - signal_line if signal_line else None
    
    return macd_line, signal_line, histogram


def bollinger_bands(prices, period=20, std_dev=2):
    """Расчет полос Боллинджера"""
    if len(prices) < period:
        return None, None, None
    
    middle = sma(prices, period)
    if middle is None:
        return None, None, None
    
    recent_prices = prices[-period:]
    variance = sum((x - middle) ** 2 for x in recent_prices) / period
    std = variance ** 0.5
    
    upper = middle + (std_dev * std)
    lower = middle - (std_dev * std)
    
    return upper, middle, lower

# =========================
# MOEX DATA
# =========================

async def get_all_tickers(session):
    """Получение ВСЕХ тикеров со всех основных рынков MOEX"""
    all_tickers = set()  # Используем set для уникальности
    
    # Основные рынки для сканирования
    markets = [
        # Основной рынок акций
        ("stock", "shares", "TQBR"),
        # Сектор ПИР (паи)
        ("stock", "shares", "TQTF"),
        # Рынок инноваций и инвестиций
        ("stock", "shares", "TQIF"),
        # Сектор Роста
        ("stock", "shares", "TQDE"),
        # Утренняя сессия
        ("stock", "shares", "TQWR"),
    ]
    
    for engine, market, board in markets:
        try:
            url = (
                f"https://iss.moex.com/iss/engines/{engine}/"
                f"markets/{market}/boards/{board}/securities.json"
            )
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    securities = data.get("securities", {}).get("data", [])
                    
                    for sec in securities:
                        if sec[0]:  # SECID
                            all_tickers.add(sec[0])
                    
                    log.info(f"Loaded {len(securities)} tickers from {board}")
                else:
                    log.warning(f"Failed to load {board}: HTTP {r.status}")
                    
        except Exception as e:
            log.error(f"Error loading tickers from {board}: {e}")
    
    tickers_list = list(all_tickers)
    log.info(f"Total unique tickers loaded: {len(tickers_list)}")
    return tickers_list


async def get_candles(session, ticker):
    """Получение свечей для тикера"""
    url = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/"
        f"securities/{ticker}/candles.json"
        f"?interval=60"  # Часовые свечи
    )
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None, None, None
            
            data = await r.json()
            candles = data.get("candles", {}).get("data", [])
            
            if not candles or len(candles) < MIN_CANDLES:
                return None, None, None
            
            times = []
            opens = []
            highs = []
            lows = []
            closes = []
            volumes = []
            
            for candle in candles:
                try:
                    times.append(candle[6])     # begin
                    opens.append(float(candle[0]))  # open
                    closes.append(float(candle[1])) # close
                    highs.append(float(candle[2]))  # high
                    lows.append(float(candle[3]))   # low
                    volumes.append(float(candle[5])) # volume
                except (IndexError, ValueError, TypeError):
                    continue
            
            if len(closes) < MIN_CANDLES:
                return None, None, None
            
            return {
                'times': times,
                'opens': opens,
                'highs': highs,
                'lows': lows,
                'closes': closes,
                'volumes': volumes
            }, times[-1], closes[-1]
            
    except asyncio.TimeoutError:
        return None, None, None
    except Exception as e:
        log.error(f"Error getting candles for {ticker}: {e}")
        return None, None, None

# =========================
# SEND
# =========================

async def send_signal(text, kb, ticker, sig_type):
    """Отправка сигнала всем пользователям"""
    users = await get_users()
    
    if not users:
        log.debug("No users to send signals")
        return
    
    sent_count = 0
    failed_count = 0
    
    for uid, is_pro in users:
        try:
            if await update_limit(uid, bool(is_pro)):
                try:
                    await bot.send_message(uid, text, reply_markup=kb)
                    sent_count += 1
                    await asyncio.sleep(0.03)  # Задержка для избежания флуда
                except Exception as e:
                    failed_count += 1
                    log.debug(f"Failed to send to user {uid}: {e}")
        except Exception as e:
            log.error(f"Error processing user {uid}: {e}")
    
    if sent_count > 0:
        log.info(f"Signal [{sig_type}] {ticker} sent to {sent_count} users (failed: {failed_count})")

# =========================
# ANALYZE
# =========================

async def analyze(session, ticker, sem):
    """Комплексный анализ тикера"""
    async with sem:
        try:
            result = await get_candles(session, ticker)
            
            if result[0] is None:
                return
            
            data, candle_time, price = result
            closes = data['closes']
            volumes = data['volumes']
            highs = data['highs']
            lows = data['lows']
            
            if len(closes) < MIN_CANDLES:
                return

            signals = []
            signal_strength = 0
            sig_type = "TECHNICAL"

            # === RSI ===
            rsi_val = rsi(closes)
            if rsi_val is not None:
                if rsi_val < 25:
                    signals.append(f"🔴 RSI сильно перепродан ({rsi_val:.1f})")
                    sig_type = "STRONG_REVERSAL"
                    signal_strength += 3
                elif rsi_val < 35:
                    signals.append(f"🟡 RSI перепродан ({rsi_val:.1f})")
                    if sig_type != "STRONG_REVERSAL":
                        sig_type = "REVERSAL"
                    signal_strength += 2
                elif rsi_val > 75:
                    signals.append(f"🔴 RSI сильно перекуплен ({rsi_val:.1f})")
                    sig_type = "STRONG_REVERSAL"
                    signal_strength += 3
                elif rsi_val > 65:
                    signals.append(f"🟡 RSI перекуплен ({rsi_val:.1f})")
                    if sig_type != "STRONG_REVERSAL":
                        sig_type = "REVERSAL"
                    signal_strength += 2

            # === Moving Averages ===
            sma20 = sma(closes, 20)
            sma50 = sma(closes, 50)
            ema20 = ema(closes, 20)
            
            ma_signals = []
            if sma20 and sma50:
                # Пересечение SMA
                if closes[-2] < sma20 and closes[-1] > sma20:
                    ma_signals.append("📈 Пробой SMA20 вверх")
                    signal_strength += 2
                elif closes[-2] > sma20 and closes[-1] < sma20:
                    ma_signals.append("📉 Пробой SMA20 вниз")
                    signal_strength += 2
                
                # Золотой крест / Мертвый крест
                if sma20 > sma50 and len(closes) > 51:
                    prev_sma20 = sma(closes[:-1], 20)
                    prev_sma50 = sma(closes[:-1], 50)
                    if prev_sma20 and prev_sma50:
                        if prev_sma20 <= prev_sma50 and sma20 > sma50:
                            ma_signals.append("🥇 Золотой крест (SMA20 > SMA50)")
                            sig_type = "GOLDEN_CROSS"
                            signal_strength += 4
                        elif prev_sma20 >= prev_sma50 and sma20 < sma50:
                            ma_signals.append("💀 Мертвый крест (SMA20 < SMA50)")
                            sig_type = "DEAD_CROSS"
                            signal_strength += 4
            
            if sma20:
                diff_from_sma = ((price - sma20) / sma20) * 100
                if abs(diff_from_sma) > 3:
                    direction = "выше" if price > sma20 else "ниже"
                    ma_signals.append(f"Цена {direction} SMA20 на {abs(diff_from_sma):.1f}%")
                    signal_strength += 1
            
            signals.extend(ma_signals)

            # === Bollinger Bands ===
            bb_upper, bb_middle, bb_lower = bollinger_bands(closes)
            if bb_upper and bb_lower:
                bb_width = ((bb_upper - bb_lower) / bb_middle) * 100
                
                if price >= bb_upper:
                    signals.append(f"📊 Пробой верхней полосы Боллинджера")
                    signal_strength += 2
                elif price <= bb_lower:
                    signals.append(f"📊 Пробой нижней полосы Боллинджера")
                    signal_strength += 2
                
                if bb_width < 5:  # Узкий канал - предвестник сильного движения
                    signals.append("📊 Сужение полос Боллинджера (возможен прорыв)")
                    signal_strength += 2

            # === MACD ===
            macd_line, signal_line, histogram = macd(closes)
            if macd_line is not None and signal_line is not None and histogram is not None:
                # Пересечение MACD
                if len(closes) > 2:
                    prev_macd, _, _ = macd(closes[:-1])
                    if prev_macd is not None:
                        if prev_macd < signal_line and macd_line > signal_line:
                            signals.append("📈 MACD пересечение вверх")
                            signal_strength += 2
                        elif prev_macd > signal_line and macd_line < signal_line:
                            signals.append("📉 MACD пересечение вниз")
                            signal_strength += 2

            # === Уровни поддержки/сопротивления ===
            if len(closes) >= 20:
                recent_high = max(highs[-20:])
                recent_low = min(lows[-20:])
                
                resistance_level = 0
                support_level = 0
                
                # Поиск уровней
                for i in range(len(highs) - 20, len(highs) - 1):
                    if highs[i] == recent_high:
                        resistance_level += 1
                    if lows[i] == recent_low:
                        support_level += 1
                
                if price >= recent_high * 0.995:  # Подход к сопротивлению
                    signals.append(f"🎯 Подход к сопротивлению {recent_high:.2f}")
                    signal_strength += 1
                    if price >= recent_high:
                        signals.append(f"🚀 Пробой сопротивления {recent_high:.2f}")
                        sig_type = "BREAKOUT"
                        signal_strength += 3
                
                if price <= recent_low * 1.005:  # Подход к поддержке
                    signals.append(f"🎯 Подход к поддержке {recent_low:.2f}")
                    signal_strength += 1
                    if price <= recent_low:
                        signals.append(f"📉 Пробой поддержки {recent_low:.2f}")
                        sig_type = "BREAKDOWN"
                        signal_strength += 3

            # === Анализ объемов ===
            if len(volumes) >= 20:
                avg_volume = sum(volumes[-20:-1]) / 19
                current_volume = volumes[-1]
                
                if current_volume > avg_volume * 3:
                    signals.append(f"📊 Экстремальный объем (x{current_volume/avg_volume:.1f})")
                    signal_strength += 3
                elif current_volume > avg_volume * 2:
                    signals.append(f"📊 Повышенный объем (x{current_volume/avg_volume:.1f})")
                    signal_strength += 2
                elif current_volume > avg_volume * 1.5:
                    signals.append(f"📊 Объем выше среднего (x{current_volume/avg_volume:.1f})")
                    signal_strength += 1

            # === Паттерны свечей ===
            if len(closes) >= 3:
                # Молот / Повешенный
                last_candle_body = abs(closes[-1] - data['opens'][-1])
                last_candle_shadow = data['highs'][-1] - max(closes[-1], data['opens'][-1])
                last_candle_lower = min(closes[-1], data['opens'][-1]) - data['lows'][-1]
                
                if last_candle_lower > last_candle_body * 2 and last_candle_shadow < last_candle_body * 0.5:
                    signals.append("🔨 Паттерн 'Молот' (бычий разворот)")
                    sig_type = "PATTERN"
                    signal_strength += 3
                elif last_candle_shadow > last_candle_body * 2 and last_candle_lower < last_candle_body * 0.5:
                    signals.append("🔨 Паттерн 'Повешенный' (медвежий разворот)")
                    sig_type = "PATTERN"
                    signal_strength += 3

            # Проверяем, достаточно ли сигналов
            if signal_strength < 3 or len(signals) < 2:
                return

            # Антидубликат
            if is_duplicate(ticker, candle_time, sig_type):
                return

            # Очистка старых сигналов
            clean_old_signals()

            # Формируем сообщение
            tv_link = f"https://www.tradingview.com/chart/?symbol=MOEX:{ticker}"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📊 Открыть в TradingView", url=tv_link)]
            ])
            
            # Определяем эмодзи для силы сигнала
            strength_emoji = "🟢" if signal_strength >= 6 else "🟡" if signal_strength >= 4 else "🔴"
            
            # Определяем тип сигнала для отображения
            type_names = {
                "STRONG_REVERSAL": "Сильный разворот",
                "REVERSAL": "Разворот",
                "BREAKOUT": "Пробой вверх",
                "BREAKDOWN": "Пробой вниз",
                "GOLDEN_CROSS": "Золотой крест",
                "DEAD_CROSS": "Мертвый крест",
                "PATTERN": "Свечной паттерн",
                "TECHNICAL": "Технический сигнал"
            }
            
            type_name = type_names.get(sig_type, sig_type)
            
            text = f"""
<b>🔔 {ticker}</b>

Тип сигнала: <b>{type_name}</b>
Цена: <b>{price:.2f} ₽</b>
Сила сигнала: {strength_emoji} <b>{signal_strength}/10</b>

📊 Сигналы:
• {'\n• '.join(signals)}

⏰ Время: {candle_time}
"""

            await send_signal(text, kb, ticker, sig_type)

        except Exception as e:
            log.error(f"Error analyzing {ticker}: {e}", exc_info=True)

# =========================
# SCAN
# =========================

async def scan():
    """Сканирование всего рынка"""
    start_time = datetime.now()
    log.info(f"Starting market scan at {start_time}")
    
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Получаем ВСЕ тикеры
        tickers = await get_all_tickers(session)
        
        if not tickers:
            log.warning("No tickers loaded, skipping scan")
            return None
        
        total_tickers = len(tickers)
        log.info(f"Starting analysis of {total_tickers} tickers")
        
        # Семафор для ограничения одновременных запросов
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        # Счетчики для статистики
        processed = 0
        errors = 0
        
        # Обрабатываем тикеры чанками
        for i in range(0, total_tickers, CHUNK_SIZE):
            chunk = tickers[i:i + CHUNK_SIZE]
            
            try:
                tasks = [analyze(session, ticker, sem) for ticker in chunk]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                processed += len(chunk)
                
                # Прогресс каждые 500 тикеров
                if processed % 500 == 0:
                    progress = (processed / total_tickers) * 100
                    log.info(f"Progress: {processed}/{total_tickers} ({progress:.1f}%)")
                
                # Пауза между чанками для снижения нагрузки
                if i + CHUNK_SIZE < total_tickers:
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                errors += 1
                log.error(f"Error processing chunk {i}: {e}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        stats = {
            'total': total_tickers,
            'processed': processed,
            'errors': errors,
            'duration': duration,
            'tickers_per_second': total_tickers / duration if duration > 0 else 0
        }
        
        log.info(
            f"Scan completed in {duration:.2f}s | "
            f"Processed: {processed}/{total_tickers} | "
            f"Errors: {errors} | "
            f"Speed: {stats['tickers_per_second']:.1f} tickers/s"
        )
        
        return stats

# =========================
# LOOP
# =========================

async def loop():
    """Основной цикл сканирования"""
    scan_number = 0
    
    while True:
        try:
            scan_number += 1
            log.info(f"=== Scan #{scan_number} started ===")
            
            stats = await scan()
            
            if stats:
                log.info(f"=== Scan #{scan_number} completed successfully ===")
            else:
                log.warning(f"=== Scan #{scan_number} failed ===")
            
        except Exception as e:
            log.error(f"Critical error in scan #{scan_number}: {e}", exc_info=True)
        
        # Ожидание до следующего сканирования
        log.info(f"Waiting {SCAN_INTERVAL}s until next scan...")
        await asyncio.sleep(SCAN_INTERVAL)

# =========================
# COMMANDS
# =========================

@dp.message(Command("start"))
async def start(m: types.Message):
    """Команда старта"""
    await add_user(m.from_user.id)
    await m.answer(
        "🤖 <b>Сканер сигналов MOEX</b>\n\n"
        "Бот анализирует ВСЕ акции Московской биржи и отправляет "
        "сигналы на основе комплексного технического анализа:\n\n"
        "📊 <b>Индикаторы:</b>\n"
        "• RSI (перекупленность/перепроданность)\n"
        "• SMA 20/50 (скользящие средние)\n"
        "• EMA (экспоненциальная средняя)\n"
        "• MACD (схождение/расхождение)\n"
        "• Полосы Боллинджера\n"
        "• Уровни поддержки/сопротивления\n"
        "• Свечные паттерны\n"
        "• Объемы торгов\n\n"
        "⚡ <b>Типы сигналов:</b>\n"
        "• Сильный разворот\n"
        "• Пробой/Пробой вниз\n"
        "• Золотой/Мертвый крест\n"
        "• Свечные паттерны\n\n"
        "📈 <b>Лимиты:</b>\n"
        "• Бесплатно: 3 сигнала в день\n"
        "• PRO: безлимитно\n\n"
        "🔄 Сканирование каждые 5 минут\n\n"
        "Команды:\n"
        "/start - Начать работу\n"
        "/status - Проверить лимиты\n"
        "/help - Помощь"
    )

@dp.message(Command("status"))
async def status(m: types.Message):
    """Проверка статуса пользователя"""
    uid = m.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT is_pro, signals_today, last_reset_date 
        FROM users WHERE user_id = ?
        """, (uid,))
        row = await cur.fetchone()
        
        if row:
            is_pro, signals_today, last_reset = row
            remaining = max(0, 3 - signals_today) if not is_pro else "∞"
            
            status_text = (
                f"📊 <b>Ваш статус:</b>\n\n"
                f"Про-аккаунт: {'✅ Да' if is_pro else '❌ Нет'}\n"
                f"Сигналов сегодня: {signals_today}/3\n"
                f"Осталось сигналов: {remaining}\n"
                f"Дата сброса: {last_reset}\n\n"
                f"{'🔓 Безлимитный доступ' if is_pro else '⏳ Лимит исчерпан' if signals_today >= 3 else '✅ Доступны сигналы'}"
            )
        else:
            status_text = "❌ Пользователь не найден. Используйте /start для регистрации"
    
    await m.answer(status_text)

@dp.message(Command("help"))
async def help_command(m: types.Message):
    """Помощь по боту"""
    await m.answer(
        "📚 <b>Помощь по боту</b>\n\n"
        "<b>Как это работает:</b>\n"
        "Бот каждые 5 минут сканирует ВСЕ акции Мосбиржи "
        "(более 200 инструментов) и отправляет сигналы "
        "при обнаружении технических паттернов.\n\n"
        "<b>Методология анализа:</b>\n"
        "Используются часовые свечи для поиска "
        "среднесрочных сигналов. Каждый сигнал "
        "оценивается по шкале силы от 1 до 10.\n\n"
        "<b>Сила сигнала:</b>\n"
        "🟢 6-10: Сильный сигнал\n"
        "🟡 4-5: Средний сигнал\n"
        "🔴 1-3: Слабый сигнал\n\n"
        "<b>Важно:</b>\n"
        "• Сигналы не являются инвестиционной рекомендацией\n"
        "• Всегда проводите собственный анализ\n"
        "• Используйте стоп-лоссы\n"
        "• Учитывайте фундаментальные факторы"
    )

# =========================
# ERROR HANDLER
# =========================

@dp.errors()
async def error_handler(update: types.Update, exception: Exception):
    """Глобальный обработчик ошибок"""
    log.error(f"Update {update} caused error: {exception}", exc_info=True)
    return True

# =========================
# MAIN
# =========================

async def main():
    """Главная функция запуска"""
    log.info("=" * 50)
    log.info("MOEX Scanner Bot Starting...")
    log.info(f"Scan interval: {SCAN_INTERVAL}s")
    log.info(f"Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    log.info(f"Chunk size: {CHUNK_SIZE}")
    log.info("=" * 50)
    
    # 1. Инициализация БД
    await init_db()

    # 2. Запуск сканера в фоне
    scanner_task = asyncio.create_task(loop())
    
    # 3. Запуск бота
    try:
        log.info("Starting bot polling...")
        await dp.start_polling(bot)
    finally:
        log.info("Shutting down...")
        scanner_task.cancel()
        try:
            await scanner_task
        except asyncio.CancelledError:
            log.info("Scanner task cancelled")
        except Exception as e:
            log.error(f"Error cancelling scanner: {e}")
        
        log.info("Bot stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot stopped by user")
    except Exception as e:
        log.critical(f"Fatal error: {e}", exc_info=True)