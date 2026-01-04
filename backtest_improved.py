import pandas as pd
import numpy as np
import time
import datetime
import os
import concurrent.futures
import matplotlib.pyplot as plt
import seaborn as sns
from pybit.unified_trading import HTTP
import talib
import logging
import json
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# === Конфигурация ===
@dataclass
class Config:
    """Конфигурация для бэктестинга"""
    pairs: List[str]
    interval: str = "1"
    days_back: int = 30
    hold_period: int = 12
    commission: float = 0.0006
    tp_percentage: float = 0.02
    sl_percentage: float = 0.01
    max_position_size: float = 0.1  # Максимальный размер позиции от капитала
    max_daily_loss: float = 0.05    # Максимальный дневной убыток
    min_volume: float = 1000000     # Минимальный объем для торговли
    ema_fast: int = 9
    ema_slow: int = 21
    rsi_period: int = 14
    bb_period: int = 20
    rsi_oversold: float = 30
    rsi_overbought: float = 70
    max_workers: int = 5
    results_dir: str = "backtest_scalping_improved"

# Загрузка конфигурации
def load_config() -> Config:
    """Загружает конфигурацию из файла или создает по умолчанию"""
    config_file = "backtest_config.json"
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return Config(**data)
    else:
        # Создаем конфигурацию по умолчанию
        config = Config(
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
def setup_logging(config: Config):
    """Настраивает логирование"""
    os.makedirs(config.results_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{config.results_dir}/backtest.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# === Улучшенная загрузка данных ===
class DataFetcher:
    """Класс для загрузки и кэширования данных"""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = HTTP(testnet=False)
        self.cache = {}
        
    def fetch_ohlcv(self, symbol: str) -> Optional[pd.DataFrame]:
        """Загружает OHLCV данные с обработкой ошибок и кэшированием"""
        try:
            # Проверяем кэш
            cache_key = f"{symbol}_{self.config.interval}_{self.config.days_back}"
            if cache_key in self.cache:
                return self.cache[cache_key]
            
            all_data = []
            now = int(time.time()) * 1000
            start = now - self.config.days_back * 86400 * 1000
            retries = 3
            
            while retries > 0:
                try:
                    response = self.session.get_kline(
                        category="linear",
                        symbol=symbol,
                        interval=self.config.interval,
                        start=start,
                        limit=1000
                    )
                    
                    if response.get('retCode') != 0:
                        logging.error(f"API Error for {symbol}: {response.get('retMsg')}")
                        retries -= 1
                        time.sleep(1)
                        continue
                    
                    candles = response['result']['list']
                    if not candles:
                        break
                        
                    all_data += candles
                    last_time = int(candles[-1][0])
                    start = last_time + 60 * 1000
                    
                    if last_time > now:
                        break
                        
                    time.sleep(0.1)  # Увеличиваем задержку для избежания rate limits
                    
                except Exception as e:
                    logging.error(f"Error fetching data for {symbol}: {e}")
                    retries -= 1
                    time.sleep(2)
                    continue
            
            if not all_data:
                logging.warning(f"No data received for {symbol}")
                return None
                
            df = pd.DataFrame(all_data, columns=[
                "timestamp", "open", "high", "low", "close", "volume", "turnover"
            ])
            
            # Валидация данных
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
            numeric_columns = ["open", "high", "low", "close", "volume"]
            df[numeric_columns] = df[numeric_columns].astype(float)
            
            # Проверка на аномалии
            if df["volume"].sum() < self.config.min_volume:
                logging.warning(f"Low volume for {symbol}: {df['volume'].sum():.0f}")
                return None
                
            # Кэшируем результат
            self.cache[cache_key] = df
            return df
            
        except Exception as e:
            logging.error(f"Critical error fetching data for {symbol}: {e}")
            return None

# === Улучшенные индикаторы ===
def calculate_indicators(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Вычисляет технические индикаторы с валидацией"""
    try:
        close = df["close"].values
        volume = df["volume"].values
        
        # EMA
        df["ema_fast"] = talib.EMA(close, timeperiod=config.ema_fast)
        df["ema_slow"] = talib.EMA(close, timeperiod=config.ema_slow)
        
        # RSI
        df["rsi"] = talib.RSI(close, timeperiod=config.rsi_period)
        
        # Bollinger Bands
        upper, middle, lower = talib.BBANDS(close, timeperiod=config.bb_period)
        df["bb_upper"] = upper
        df["bb_middle"] = middle
        df["bb_lower"] = lower
        
        # Дополнительные индикаторы
        df["sma_volume"] = talib.SMA(volume, timeperiod=20)
        df["atr"] = talib.ATR(df["high"].values, df["low"].values, close, timeperiod=14)
        
        # Фильтр объема
        df["volume_filter"] = df["volume"] > df["sma_volume"] * 0.5
        
        return df.dropna().reset_index(drop=True)
        
    except Exception as e:
        logging.error(f"Error calculating indicators: {e}")
        return df

# === Улучшенный фильтр времени ===
def in_trading_hours(timestamp: pd.Timestamp) -> bool:
    """Проверяет, находится ли время в торговых часах (GMT+3)"""
    t = timestamp.time()
    
    # Торговые сессии (GMT+3)
    tokyo = (datetime.time(3, 0), datetime.time(12, 0))
    london = (datetime.time(11, 0), datetime.time(20, 0))
    newyork = (datetime.time(15, 0), datetime.time(23, 59))
    
    # Проверяем попадание в торговые часы
    if tokyo[0] <= t <= tokyo[1]: return True
    if london[0] <= t <= london[1]: return True
    if newyork[0] <= t <= newyork[1]: return True
    if t >= datetime.time(0, 0) and t <= datetime.time(3, 0): return True
    
    return False

# === Улучшенная логика стратегии ===
class TradingStrategy:
    """Класс для реализации торговой стратегии"""
    
    def __init__(self, config: Config):
        self.config = config
        self.daily_pnl = 0.0
        self.max_daily_loss = config.max_daily_loss
        
    def reset_daily_pnl(self, date: pd.Timestamp):
        """Сбрасывает дневной PnL при смене дня"""
        current_date = date.date()
        if hasattr(self, 'last_date') and self.last_date != current_date:
            self.daily_pnl = 0.0
        self.last_date = current_date
    
    def check_risk_limits(self, pnl: float) -> bool:
        """Проверяет лимиты риска"""
        self.daily_pnl += pnl
        return self.daily_pnl > -self.max_daily_loss
    
    def check_entry_conditions(self, row: pd.Series, config: Config) -> Tuple[bool, str]:
        """Проверяет условия входа в позицию"""
        # Лонг условия
        if (row["ema_fast"] > row["ema_slow"] and 
            row["rsi"] < config.rsi_overbought and 
            row["close"] < row["bb_lower"] and
            row["volume_filter"]):
            return True, "long"
        
        # Шорт условия
        elif (row["ema_fast"] < row["ema_slow"] and 
              row["rsi"] > config.rsi_oversold and 
              row["close"] > row["bb_upper"] and
              row["volume_filter"]):
            return True, "short"
        
        return False, ""

# === Улучшенный бэктест ===
def backtest(df: pd.DataFrame, config: Config) -> List[Dict]:
    """Выполняет бэктест с улучшенной логикой"""
    strategy = TradingStrategy(config)
    trades = []
    
    for i in range(config.bb_period, len(df) - config.hold_period):
        row = df.iloc[i]
        
        # Сброс дневного PnL
        strategy.reset_daily_pnl(row["timestamp"])
        
        # Проверка торговых часов
        if not in_trading_hours(row["timestamp"]):
            continue
        
        # Проверка условий входа
        should_enter, trade_type = strategy.check_entry_conditions(row, config)
        
        if not should_enter:
            continue
        
        entry_price = row["close"]
        entry_time = row["timestamp"]
        
        # Расчет уровней для лонга
        if trade_type == "long":
            tp_price = entry_price * (1 + config.tp_percentage)
            sl_price = entry_price * (1 - config.sl_percentage)
            
            # Поиск выхода
            for j in range(i + 1, min(i + config.hold_period, len(df))):
                price = df.iloc[j]["close"]
                
                if price >= tp_price or price <= sl_price:
                    pnl = (price - entry_price) / entry_price - config.commission
                    
                    # Проверка лимитов риска
                    if not strategy.check_risk_limits(pnl):
                        logging.warning(f"Daily loss limit reached: {strategy.daily_pnl:.4f}")
                        break
                    
                    trades.append({
                        "time": entry_time,
                        "type": trade_type,
                        "entry": entry_price,
                        "exit": price,
                        "pnl": pnl,
                        "exit_reason": "tp" if price >= tp_price else "sl",
                        "hold_period": j - i
                    })
                    break
        
        # Расчет уровней для шорта
        elif trade_type == "short":
            tp_price = entry_price * (1 - config.tp_percentage)
            sl_price = entry_price * (1 + config.sl_percentage)
            
            # Поиск выхода
            for j in range(i + 1, min(i + config.hold_period, len(df))):
                price = df.iloc[j]["close"]
                
                if price <= tp_price or price >= sl_price:
                    pnl = (entry_price - price) / entry_price - config.commission
                    
                    # Проверка лимитов риска
                    if not strategy.check_risk_limits(pnl):
                        logging.warning(f"Daily loss limit reached: {strategy.daily_pnl:.4f}")
                        break
                    
                    trades.append({
                        "time": entry_time,
                        "type": trade_type,
                        "entry": entry_price,
                        "exit": price,
                        "pnl": pnl,
                        "exit_reason": "tp" if price <= tp_price else "sl",
                        "hold_period": j - i
                    })
                    break
    
    return trades

# === Улучшенная обработка символа ===
def process_symbol(symbol: str, config: Config, data_fetcher: DataFetcher) -> Optional[Dict]:
    """Обрабатывает один символ с улучшенной аналитикой"""
    try:
        logging.info(f"Processing {symbol}...")
        
        # Загрузка данных
        df = data_fetcher.fetch_ohlcv(symbol)
        if df is None or len(df) < 100:
            logging.warning(f"Insufficient data for {symbol}")
            return None
        
        # Вычисление индикаторов
        df = calculate_indicators(df, config)
        if len(df) < config.bb_period:
            logging.warning(f"Not enough data after indicators for {symbol}")
            return None
        
        # Бэктест
        trades = backtest(df, config)
        
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
                "avg_hold_period": 0
            }
        
        # Расширенная аналитика
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
        
        # Сохранение детальных результатов
        df_trades.to_csv(f"{config.results_dir}/{symbol}_trades.csv", index=False)
        
        return {
            "symbol": symbol,
            "trades": len(trades),
            "total_pnl_%": round(total_pnl * 100, 2),
            "avg_pnl_%": round(avg_pnl * 100, 2),
            "winrate_%": round(winrate, 2),
            "sharpe_ratio": round(sharpe_ratio, 3),
            "max_drawdown_%": round(max_drawdown, 2),
            "profit_factor": round(profit_factor, 2),
            "avg_hold_period": round(df_trades["hold_period"].mean(), 1)
        }
        
    except Exception as e:
        logging.error(f"Error processing {symbol}: {e}")
        return None

# === Улучшенная визуализация ===
def create_advanced_charts(df_summary: pd.DataFrame, config: Config):
    """Создает расширенные графики анализа"""
    plt.style.use('seaborn-v0_8')
    
    # 1. Общая доходность
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Доходность по символам
    top_symbols = df_summary.nlargest(10, "total_pnl_%")
    axes[0, 0].barh(top_symbols["symbol"], top_symbols["total_pnl_%"], color='green')
    axes[0, 0].set_title("Топ-10 по доходности (%)")
    axes[0, 0].set_xlabel("Доходность %")
    
    # Распределение винрейта
    axes[0, 1].hist(df_summary["winrate_%"], bins=15, color='blue', alpha=0.7, edgecolor='black')
    axes[0, 1].set_title("Распределение винрейта (%)")
    axes[0, 1].set_xlabel("Winrate %")
    axes[0, 1].set_ylabel("Частота")
    
    # Sharpe Ratio vs Winrate
    axes[1, 0].scatter(df_summary["winrate_%"], df_summary["sharpe_ratio"], alpha=0.6)
    axes[1, 0].set_title("Sharpe Ratio vs Winrate")
    axes[1, 0].set_xlabel("Winrate %")
    axes[1, 0].set_ylabel("Sharpe Ratio")
    
    # Profit Factor vs Max Drawdown
    axes[1, 1].scatter(df_summary["max_drawdown_%"], df_summary["profit_factor"], alpha=0.6)
    axes[1, 1].set_title("Profit Factor vs Max Drawdown")
    axes[1, 1].set_xlabel("Max Drawdown %")
    axes[1, 1].set_ylabel("Profit Factor")
    
    plt.tight_layout()
    plt.savefig(f"{config.results_dir}/advanced_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Корреляционная матрица
    numeric_cols = ["total_pnl_%", "winrate_%", "sharpe_ratio", "max_drawdown_%", "profit_factor"]
    correlation_matrix = df_summary[numeric_cols].corr()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, square=True)
    plt.title("Корреляционная матрица метрик")
    plt.tight_layout()
    plt.savefig(f"{config.results_dir}/correlation_matrix.png", dpi=300, bbox_inches='tight')
    plt.close()

# === Главная функция ===
def main():
    """Главная функция бэктестинга"""
    # Загрузка конфигурации
    config = load_config()
    
    # Настройка логирования
    setup_logging(config)
    
    logging.info("🚀 Запуск улучшенного бэктестинга...")
    logging.info(f"Конфигурация: {config.pairs[:5]}... ({len(config.pairs)} пар)")
    
    # Инициализация компонентов
    data_fetcher = DataFetcher(config)
    
    # Мультипоточная обработка
    summary = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {executor.submit(process_symbol, symbol, config, data_fetcher): symbol 
                  for symbol in config.pairs}
        
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                if result:
                    summary.append(result)
                    logging.info(f"✅ {symbol}: {result['trades']} сделок, {result['total_pnl_%']:.2f}%")
                else:
                    logging.warning(f"❌ {symbol}: нет результатов")
            except Exception as e:
                logging.error(f"❌ Ошибка обработки {symbol}: {e}")
    
    # Итоговый отчет
    if summary:
        df_summary = pd.DataFrame(summary)
        df_summary = df_summary.sort_values("total_pnl_%", ascending=False)
        
        # Сохранение результатов
        df_summary.to_csv(f"{config.results_dir}/summary_improved.csv", index=False)
        
        # Создание графиков
        create_advanced_charts(df_summary, config)
        
        # Вывод статистики
        logging.info(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
        logging.info(f"Всего пар: {len(df_summary)}")
        logging.info(f"Прибыльных пар: {(df_summary['total_pnl_%'] > 0).sum()}")
        logging.info(f"Средняя доходность: {df_summary['total_pnl_%'].mean():.2f}%")
        logging.info(f"Средний винрейт: {df_summary['winrate_%'].mean():.2f}%")
        logging.info(f"Лучшая пара: {df_summary.iloc[0]['symbol']} ({df_summary.iloc[0]['total_pnl_%']:.2f}%)")
        
        print(f"\n✅ Бэктестинг завершен! Результаты в папке {config.results_dir}")
    else:
        logging.error("❌ Нет данных для итогового отчета")

if __name__ == "__main__":
    main() 