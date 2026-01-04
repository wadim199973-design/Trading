import pandas as pd
import numpy as np
import time
import datetime
import os
import concurrent.futures
import matplotlib.pyplot as plt
import seaborn as sns
import talib
import logging
import json
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# === Конфигурация для скальпинга ===
@dataclass
class ScalpConfig:
    """Конфигурация для скальпинг бэктестинга"""
    data_dir: str = "csv_data"  # Папка с CSV файлами
    pairs: List[str] = None
    interval: str = "1"  # 1 минута для скальпинга
    days_back: int = 7  # Меньше дней для скальпинга
    commission: float = 0.0006
    tp_percentage: float = 0.005  # 0.5% Take Profit для скальпинга
    sl_percentage: float = 0.003  # 0.3% Stop Loss для скальпинга
    max_hold_time: int = 300  # 5 минут максимальное время удержания
    min_volume: float = 1000000  # Минимальный объем для торговли
    max_open_positions: int = 8  # Максимум открытых позиций
    
    # Скальпинг индикаторы
    ema_fast: int = 9
    ema_slow: int = 21
    sma_fast: int = 5
    sma_slow: int = 10
    rsi_period: int = 14
    bb_period: int = 20
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    atr_period: int = 14
    momentum_period: int = 10
    roc_period: int = 10
    
    # Фильтры для скальпинга
    min_volume_ratio: float = 1.2  # Минимальное отношение объема
    min_price_change: float = 0.001  # Минимальное изменение цены
    min_atr_ratio: float = 0.5  # Минимальное отношение ATR к среднему
    
    max_workers: int = 5
    results_dir: str = "scalp_backtest_results"

# Загрузка конфигурации
def load_scalp_config() -> ScalpConfig:
    """Загружает конфигурацию скальпинга из файла или создает по умолчанию"""
    config_file = "scalp_config.json"
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return ScalpConfig(**data)
    else:
        # Создаем конфигурацию по умолчанию для скальпинга
        config = ScalpConfig(
            pairs=[
                "DOGEUSDT", "SOLUSDT", "DOTUSDT", "CVCUSDT", "YGGUSDT", "MASAUSDT",
                "ADAUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT", "API3USDT", "C98USDT", "COTIUSDT",
                "LTCUSDT", "APTUSDT", "NEARUSDT", "ICPUSDT", "AAVEUSDT", "BELUSDT", "ONTUSDT", "KAVAUSDT",
                "ATOMUSDT", "ARBUSDT", "OPUSDT", "SEIUSDT", "PYTHUSDT", "WAVESUSDT", "XNOUSDT", "LDOUSDT", "SUNUSDT",
                "GMTUSDT", "APEUSDT", "RUNEUSDT", "ZILUSDT", "JASMYUSDT", "OGNUSDT", "SHIB1000USDT", "XRPUSDT", "BNBUSDT",
                "MANAUSDT", "SFPUSDT", "UNIUSDT", "CROUSDT", "FILUSDT", "ALGOUSDT", "XLMUSDT", "HBARUSDT", "THETAUSDT", "EGLDUSDT",
                "EOSUSDT", "ZENUSDT", "GALAUSDT", "CRVUSDT", "DYDXUSDT", "STORJUSDT", "SUSHIUSDT", "1000CATSUSDT", "1000CATUSDT", "ZROUSDT",
                "POPCATUSDT", "DYMUSDT", "ZETAUSDT", "ALTUSDT", "WIFUSDT", "NFPUSDT", "POLYXUSDT", "STPTUSDT", "TONUSDT", "OGUSDT", "HIFIUSDT",
                "WLDUSDT", "PENDLEUSDT", "XVGUSDT", "1000PEPEUSDT", "PHBUSDT", "EDUUSDT", "IDUSDT", "BLURUSDT", "1000FLOKIUSDT", "TWTUSDT", "INJUSDT",
                "OMNIUSDT", "ORCAUSDT", "STRKUSDT", "XAIUSDT", "JTOUSDT", "CAKEUSDT", "AUCTIONUSDT", "MAVUSDT", "HFTUSDT", "CFXUSDT", "AGLDUSDT", "KDAUSDT", "MNTUSDT"
            ]
        )
        # Сохраняем конфигурацию
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config.__dict__, f, indent=2, ensure_ascii=False)
        return config

# === Настройка логирования ===
def setup_logging(config: ScalpConfig):
    """Настраивает логирование"""
    os.makedirs(config.results_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{config.results_dir}/scalp_backtest.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# === Загрузка данных из CSV ===
class ScalpDataFetcher:
    """Класс для загрузки данных для скальпинга"""
    
    def __init__(self, config: ScalpConfig):
        self.config = config
        self.cache = {}
        
    def get_available_pairs(self) -> List[str]:
        """Получает список доступных пар из CSV файлов"""
        if not os.path.exists(self.config.data_dir):
            logging.warning(f"Папка {self.config.data_dir} не существует")
            return []
        
        available_pairs = []
        for file in os.listdir(self.config.data_dir):
            if file.endswith('.csv'):
                pair_name = file.replace('.csv', '').upper()
                available_pairs.append(pair_name)
        
        return available_pairs
        
    def fetch_ohlcv(self, symbol: str) -> Optional[pd.DataFrame]:
        """Загружает OHLCV данные из CSV файла для скальпинга"""
        try:
            # Проверяем кэш
            cache_key = f"{symbol}_{self.config.interval}_{self.config.days_back}"
            if cache_key in self.cache:
                return self.cache[cache_key]
            
            # Формируем путь к CSV файлу
            csv_file = os.path.join(self.config.data_dir, f"{symbol}.csv")
            
            if not os.path.exists(csv_file):
                logging.warning(f"CSV файл не найден: {csv_file}")
                return None
            
            # Читаем CSV файл
            df = pd.read_csv(csv_file)
            
            # Проверяем наличие необходимых колонок
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                logging.error(f"CSV файл {symbol} не содержит необходимые колонки: {required_columns}")
                return None
            
            # Преобразуем timestamp в datetime
            if df['timestamp'].dtype == 'object':
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Преобразуем числовые колонки
            numeric_columns = ["open", "high", "low", "close", "volume"]
            df[numeric_columns] = df[numeric_columns].astype(float)
            
            # Сортируем по времени
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # Фильтруем по количеству дней назад
            if self.config.days_back > 0:
                cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=self.config.days_back)
                df = df[df['timestamp'] >= cutoff_date]
            
            # Проверка на аномалии
            if df["volume"].sum() < self.config.min_volume:
                logging.warning(f"Low volume for {symbol}: {df['volume'].sum():.0f}")
                return None
            
            # Проверяем минимальное количество данных
            if len(df) < 50:  # Меньше данных для скальпинга
                logging.warning(f"Insufficient data for {symbol}: {len(df)} rows")
                return None
                
            # Кэшируем результат
            self.cache[cache_key] = df
            return df
            
        except Exception as e:
            logging.error(f"Critical error loading CSV data for {symbol}: {e}")
            return None

# === Скальпинг индикаторы ===
def calculate_scalp_indicators(df: pd.DataFrame, config: ScalpConfig) -> pd.DataFrame:
    """Вычисляет индикаторы для скальпинга"""
    try:
        close = df["close"].values
        high = df["high"].values
        low = df["low"].values
        volume = df["volume"].values
        
        # EMA индикаторы
        df["ema_fast"] = talib.EMA(close, timeperiod=config.ema_fast)
        df["ema_slow"] = talib.EMA(close, timeperiod=config.ema_slow)
        
        # SMA индикаторы
        df["sma_fast"] = talib.SMA(close, timeperiod=config.sma_fast)
        df["sma_slow"] = talib.SMA(close, timeperiod=config.sma_slow)
        
        # RSI
        df["rsi"] = talib.RSI(close, timeperiod=config.rsi_period)
        
        # MACD
        df["macd"], df["macd_signal"], df["macd_hist"] = talib.MACD(
            close, fastperiod=config.macd_fast, slowperiod=config.macd_slow, signalperiod=config.macd_signal
        )
        
        # Bollinger Bands
        df["bb_upper"], df["bb_middle"], df["bb_lower"] = talib.BBANDS(
            close, timeperiod=config.bb_period, nbdevup=2, nbdevdn=2
        )
        
        # Объемные индикаторы
        df["volume_sma"] = talib.SMA(volume, timeperiod=20)
        df["volume_ratio"] = volume / df["volume_sma"]
        
        # ATR для волатильности
        df["atr"] = talib.ATR(high, low, close, timeperiod=config.atr_period)
        df["atr_mean"] = df["atr"].rolling(window=20).mean()
        df["atr_ratio"] = df["atr"] / df["atr_mean"]
        
        # Моментум индикаторы
        df["momentum"] = talib.MOM(close, timeperiod=config.momentum_period)
        df["roc"] = talib.ROC(close, timeperiod=config.roc_period)
        
        return df.dropna().reset_index(drop=True)
        
    except Exception as e:
        logging.error(f"Error calculating scalp indicators: {e}")
        return df

# === Скальпинг сигналы ===
def check_scalp_signals(row: pd.Series, prev_row: pd.Series, config: ScalpConfig) -> Tuple[bool, str]:
    """Проверяет сигналы для скальпинга"""
    try:
        # Проверяем минимальный объем
        if row["volume"] < config.min_volume:
            return False, ""
        
        # Проверяем волатильность (ATR)
        if row["atr_ratio"] < config.min_atr_ratio:
            return False, ""  # Слишком низкая волатильность
        
        # Проверяем объем
        if row["volume_ratio"] < config.min_volume_ratio:
            return False, ""
        
        # Рассчитываем изменение цены
        price_change = (row["close"] - prev_row["close"]) / prev_row["close"]
        
        # LONG сигналы для скальпинга
        long_conditions = [
            row["ema_fast"] > row["ema_slow"],  # Быстрая EMA выше медленной
            row["close"] > row["sma_fast"],   # Цена выше быстрой SMA
            row["sma_fast"] > row["sma_slow"],  # Быстрая SMA выше медленной
            row["rsi"] > 40 and row["rsi"] < 70,  # RSI в рабочем диапазоне
            row["macd"] > row["macd_signal"],  # MACD выше сигнальной
            row["momentum"] > 0,  # Положительный моментум
            row["close"] > row["bb_lower"],  # Цена выше нижней полосы BB
            row["close"] < row["bb_upper"] * 0.98,  # Но не слишком близко к верхней
            price_change > config.min_price_change  # Минимальный рост цены
        ]
        
        # SHORT сигналы для скальпинга
        short_conditions = [
            row["ema_fast"] < row["ema_slow"],  # Быстрая EMA ниже медленной
            row["close"] < row["sma_fast"],   # Цена ниже быстрой SMA
            row["sma_fast"] < row["sma_slow"],  # Быстрая SMA ниже медленной
            row["rsi"] > 30 and row["rsi"] < 60,  # RSI в рабочем диапазоне
            row["macd"] < row["macd_signal"],  # MACD ниже сигнальной
            row["momentum"] < 0,  # Отрицательный моментум
            row["close"] < row["bb_upper"],  # Цена ниже верхней полосы BB
            row["close"] > row["bb_lower"] * 1.02,  # Но не слишком близко к нижней
            price_change < -config.min_price_change  # Минимальное падение цены
        ]
        
        # LONG сигнал
        if all(long_conditions):
            return True, "long"
        
        # SHORT сигнал
        elif all(short_conditions):
            return True, "short"
        
        return False, ""
        
    except Exception as e:
        logging.error(f"Error checking scalp signals: {e}")
        return False, ""

# === Скальпинг бэктест ===
def scalp_backtest(df: pd.DataFrame, config: ScalpConfig) -> List[Dict]:
    """Выполняет бэктест скальпинг стратегии"""
    trades = []
    open_positions = {}  # Словарь открытых позиций
    
    for i in range(30, len(df) - 1):  # Начинаем с 30 для достаточного количества данных
        current_time = df.iloc[i]["timestamp"]
        current_row = df.iloc[i]
        prev_row = df.iloc[i-1]
        
        # Проверяем и закрываем позиции по времени
        positions_to_close = []
        for pair, position in open_positions.items():
            if (current_time - position["open_time"]).total_seconds() > config.max_hold_time:
                # Закрываем позицию по времени
                exit_price = current_row["close"]
                pnl = (exit_price - position["entry_price"]) / position["entry_price"]
                if position["side"] == "short":
                    pnl = -pnl
                
                trades.append({
                    "time": position["open_time"],
                    "exit_time": current_time,
                    "type": position["side"],
                    "entry": position["entry_price"],
                    "exit": exit_price,
                    "pnl": pnl - config.commission,
                    "exit_reason": "time",
                    "hold_period": (current_time - position["open_time"]).total_seconds() / 60
                })
                positions_to_close.append(pair)
        
        # Удаляем закрытые позиции
        for pair in positions_to_close:
            del open_positions[pair]
        
        # Проверяем лимит открытых позиций
        if len(open_positions) >= config.max_open_positions:
            continue
        
        # Проверяем скальпинг сигналы
        should_enter, trade_type = check_scalp_signals(current_row, prev_row, config)
        
        if not should_enter:
            continue
        
        entry_price = current_row["close"]
        entry_time = current_time
        
        # Рассчитываем уровни для скальпинга
        if trade_type == "long":
            tp_price = entry_price * (1 + config.tp_percentage)
            sl_price = entry_price * (1 - config.sl_percentage)
        else:
            tp_price = entry_price * (1 - config.tp_percentage)
            sl_price = entry_price * (1 + config.sl_percentage)
        
        # Открываем позицию
        open_positions[f"position_{len(trades)}"] = {
            "side": trade_type,
            "entry_price": entry_price,
            "open_time": entry_time,
            "tp_price": tp_price,
            "sl_price": sl_price
        }
        
        # Ищем выход в следующих свечах
        for j in range(i + 1, min(i + 50, len(df))):  # Максимум 50 свечей вперед
            price = df.iloc[j]["close"]
            position_key = f"position_{len(trades)}"
            
            if position_key not in open_positions:
                break
            
            position = open_positions[position_key]
            
            # Проверяем TP/SL
            if trade_type == "long":
                if price >= tp_price or price <= sl_price:
                    pnl = (price - entry_price) / entry_price
                    exit_reason = "tp" if price >= tp_price else "sl"
                    del open_positions[position_key]
                    break
            else:
                if price <= tp_price or price >= sl_price:
                    pnl = (entry_price - price) / entry_price
                    exit_reason = "tp" if price <= tp_price else "sl"
                    del open_positions[position_key]
                    break
        else:
            # Позиция не закрылась, закрываем по времени в следующем цикле
            continue
        
        # Добавляем сделку
        trades.append({
            "time": entry_time,
            "exit_time": df.iloc[j]["timestamp"],
            "type": trade_type,
            "entry": entry_price,
            "exit": price,
            "pnl": pnl - config.commission,
            "exit_reason": exit_reason,
            "hold_period": (df.iloc[j]["timestamp"] - entry_time).total_seconds() / 60
        })
    
    # Закрываем оставшиеся позиции
    for position_key, position in open_positions.items():
        exit_price = df.iloc[-1]["close"]
        pnl = (exit_price - position["entry_price"]) / position["entry_price"]
        if position["side"] == "short":
            pnl = -pnl
        
        trades.append({
            "time": position["open_time"],
            "exit_time": df.iloc[-1]["timestamp"],
            "type": position["side"],
            "entry": position["entry_price"],
            "exit": exit_price,
            "pnl": pnl - config.commission,
            "exit_reason": "end",
            "hold_period": (df.iloc[-1]["timestamp"] - position["open_time"]).total_seconds() / 60
        })
    
    return trades

# === Обработка символа для скальпинга ===
def process_scalp_symbol(symbol: str, config: ScalpConfig, data_fetcher: ScalpDataFetcher) -> Optional[Dict]:
    """Обрабатывает один символ для скальпинг бэктестинга"""
    try:
        logging.info(f"Processing scalp strategy for {symbol}...")
        
        # Загрузка данных из CSV
        df = data_fetcher.fetch_ohlcv(symbol)
        if df is None or len(df) < 50:
            logging.warning(f"Insufficient data for {symbol}")
            return None
        
        # Вычисление скальпинг индикаторов
        df = calculate_scalp_indicators(df, config)
        if len(df) < 30:
            logging.warning(f"Not enough data after indicators for {symbol}")
            return None
        
        # Скальпинг бэктест
        trades = scalp_backtest(df, config)
        
        if not trades:
            return {
                "symbol": symbol,
                "trades": 0,
                "total_pnl_%": 0,
                "avg_pnl_%": 0,
                "winrate_%": 0,
                "sharpe_ratio": 0,
                "max_drawdown_%": 0,
                "profit_factor": 0,
                "avg_hold_period": 0,
                "avg_trades_per_day": 0
            }
        
        # Расширенная аналитика для скальпинга
        df_trades = pd.DataFrame(trades)
        
        # Основные метрики
        total_pnl = df_trades["pnl"].sum()
        avg_pnl = df_trades["pnl"].mean()
        winrate = (df_trades["pnl"] > 0).mean() * 100
        
        # Дополнительные метрики
        returns = df_trades["pnl"].values
        sharpe_ratio = np.mean(returns) / np.std(returns) if np.std(returns) > 0 else 0
        
        # Максимальная просадка
        cumulative = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max * 100
        max_drawdown = np.min(drawdown) if len(drawdown) > 0 else 0
        
        # Profit Factor
        gross_profit = df_trades[df_trades["pnl"] > 0]["pnl"].sum()
        gross_loss = abs(df_trades[df_trades["pnl"] < 0]["pnl"].sum())
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        # Средний период удержания
        avg_hold_period = df_trades["hold_period"].mean()
        
        # Среднее количество сделок в день
        if len(df_trades) > 0:
            total_days = (df_trades["exit_time"].max() - df_trades["time"].min()).days + 1
            avg_trades_per_day = len(df_trades) / total_days if total_days > 0 else 0
        else:
            avg_trades_per_day = 0
        
        # Сохранение детальных результатов
        df_trades.to_csv(f"{config.results_dir}/{symbol}_scalp_trades.csv", index=False)
        
        return {
            "symbol": symbol,
            "trades": len(trades),
            "total_pnl_%": round(total_pnl * 100, 2),
            "avg_pnl_%": round(avg_pnl * 100, 2),
            "winrate_%": round(winrate, 2),
            "sharpe_ratio": round(sharpe_ratio, 3),
            "max_drawdown_%": round(max_drawdown, 2),
            "profit_factor": round(profit_factor, 2),
            "avg_hold_period": round(avg_hold_period, 1),
            "avg_trades_per_day": round(avg_trades_per_day, 1)
        }
        
    except Exception as e:
        logging.error(f"Error processing scalp strategy for {symbol}: {e}")
        return None

# === Скальпинг визуализация ===
def create_scalp_charts(df_summary: pd.DataFrame, config: ScalpConfig):
    """Создает графики анализа для скальпинга"""
    plt.style.use('seaborn-v0_8')
    
    # 1. Общая доходность скальпинга
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Доходность по символам
    top_symbols = df_summary.nlargest(10, "total_pnl_%")
    axes[0, 0].barh(top_symbols["symbol"], top_symbols["total_pnl_%"], color='green')
    axes[0, 0].set_title("Топ-10 по доходности скальпинга (%)")
    axes[0, 0].set_xlabel("Доходность %")
    
    # Распределение винрейта
    axes[0, 1].hist(df_summary["winrate_%"], bins=15, color='blue', alpha=0.7, edgecolor='black')
    axes[0, 1].set_title("Распределение винрейта скальпинга (%)")
    axes[0, 1].set_xlabel("Winrate %")
    axes[0, 1].set_ylabel("Частота")
    
    # Средний период удержания
    axes[1, 0].hist(df_summary["avg_hold_period"], bins=15, color='orange', alpha=0.7, edgecolor='black')
    axes[1, 0].set_title("Распределение среднего периода удержания (мин)")
    axes[1, 0].set_xlabel("Время удержания (мин)")
    axes[1, 0].set_ylabel("Частота")
    
    # Сделки в день
    axes[1, 1].scatter(df_summary["avg_trades_per_day"], df_summary["total_pnl_%"], alpha=0.6)
    axes[1, 1].set_title("Доходность vs Сделки в день")
    axes[1, 1].set_xlabel("Среднее сделок в день")
    axes[1, 1].set_ylabel("Доходность %")
    
    plt.tight_layout()
    plt.savefig(f"{config.results_dir}/scalp_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Корреляционная матрица для скальпинга
    numeric_cols = ["total_pnl_%", "winrate_%", "sharpe_ratio", "max_drawdown_%", "profit_factor", "avg_hold_period", "avg_trades_per_day"]
    correlation_matrix = df_summary[numeric_cols].corr()
    
    plt.figure(figsize=(12, 10))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, square=True)
    plt.title("Корреляционная матрица метрик скальпинга")
    plt.tight_layout()
    plt.savefig(f"{config.results_dir}/scalp_correlation_matrix.png", dpi=300, bbox_inches='tight')
    plt.close()

# === Главная функция скальпинг бэктестинга ===
def main():
    """Главная функция скальпинг бэктестинга"""
    # Загрузка конфигурации
    config = load_scalp_config()
    
    # Настройка логирования
    setup_logging(config)
    
    logging.info("🚀 Запуск скальпинг бэктестинга...")
    
    # Инициализация компонентов
    data_fetcher = ScalpDataFetcher(config)
    
    # Получаем доступные пары из CSV файлов
    available_pairs = data_fetcher.get_available_pairs()
    if not available_pairs:
        logging.error(f"❌ Не найдено CSV файлов в папке {config.data_dir}")
        print(f"Создайте папку {config.data_dir} и поместите туда CSV файлы с данными")
        return
    
    # Фильтруем пары, которые есть в CSV
    if config.pairs:
        pairs_to_process = [pair for pair in config.pairs if pair in available_pairs]
        logging.info(f"Найдено {len(pairs_to_process)} пар из {len(config.pairs)} запрошенных")
    else:
        pairs_to_process = available_pairs
        config.pairs = available_pairs
    
    if not pairs_to_process:
        logging.error("❌ Нет доступных пар для обработки")
        return
    
    logging.info(f"Конфигурация скальпинга: {pairs_to_process[:5]}... ({len(pairs_to_process)} пар)")
    
    # Мультипоточная обработка
    summary = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {executor.submit(process_scalp_symbol, symbol, config, data_fetcher): symbol 
                  for symbol in pairs_to_process}
        
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                if result:
                    summary.append(result)
                    logging.info(f"✅ {symbol}: {result['trades']} сделок, {result['total_pnl_%']:.2f}%, {result['avg_hold_period']:.1f} мин")
                else:
                    logging.warning(f"❌ {symbol}: нет результатов")
            except Exception as e:
                logging.error(f"❌ Ошибка обработки {symbol}: {e}")
    
    # Итоговый отчет
    if summary:
        df_summary = pd.DataFrame(summary)
        df_summary = df_summary.sort_values("total_pnl_%", ascending=False)
        
        # Сохранение результатов
        df_summary.to_csv(f"{config.results_dir}/scalp_summary.csv", index=False)
        
        # Создание графиков
        create_scalp_charts(df_summary, config)
        
        # Вывод статистики
        logging.info(f"\n📊 ИТОГОВАЯ СТАТИСТИКА СКАЛЬПИНГА:")
        logging.info(f"Всего пар: {len(df_summary)}")
        logging.info(f"Прибыльных пар: {(df_summary['total_pnl_%'] > 0).sum()}")
        logging.info(f"Средняя доходность: {df_summary['total_pnl_%'].mean():.2f}%")
        logging.info(f"Средний винрейт: {df_summary['winrate_%'].mean():.2f}%")
        logging.info(f"Средний период удержания: {df_summary['avg_hold_period'].mean():.1f} мин")
        logging.info(f"Среднее сделок в день: {df_summary['avg_trades_per_day'].mean():.1f}")
        logging.info(f"Лучшая пара: {df_summary.iloc[0]['symbol']} ({df_summary.iloc[0]['total_pnl_%']:.2f}%)")
        
        print(f"\n✅ Скальпинг бэктестинг завершен! Результаты в папке {config.results_dir}")
    else:
        logging.error("❌ Нет данных для итогового отчета")

if __name__ == "__main__":
    main() 