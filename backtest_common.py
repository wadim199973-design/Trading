# -*- coding: utf-8 -*-
"""
Общие утилиты для бэктестинга
Содержит функции и классы, которые используются в нескольких файлах бэктестинга
"""
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
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# ===== ОБЩИЕ КЛАССЫ КОНФИГУРАЦИИ =====
@dataclass
class BaseBacktestConfig:
    """Базовая конфигурация для бэктестинга"""
    data_dir: str = "csv_data"
    pairs: List[str] = None
    interval: str = "1"
    days_back: int = 30
    commission: float = 0.0006
    tp_percentage: float = 0.01
    sl_percentage: float = 0.005
    max_hold_time: int = 3600
    min_volume: float = 1000000
    max_open_positions: int = 5
    max_workers: int = 5
    results_dir: str = "backtest_results"

@dataclass
class ScalpConfig(BaseBacktestConfig):
    """Конфигурация для скальпинг бэктестинга"""
    tp_percentage: float = 0.005  # 0.5% Take Profit для скальпинга
    sl_percentage: float = 0.003  # 0.3% Stop Loss для скальпинга
    max_hold_time: int = 300  # 5 минут максимальное время удержания
    max_open_positions: int = 8  # Максимум открытых позиций
    days_back: int = 7  # Меньше дней для скальпинга
    results_dir: str = "scalp_backtest_results"
    
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
    min_volume_ratio: float = 1.2
    min_price_change: float = 0.001
    min_atr_ratio: float = 0.5

# ===== ОБЩИЕ ФУНКЦИИ =====
def load_config_from_file(config_class, config_file: str) -> BaseBacktestConfig:
    """Загружает конфигурацию из файла"""
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return config_class(**data)
        except Exception as e:
            logging.error(f"Ошибка загрузки конфигурации: {str(e)}")
    
    # Возвращаем конфигурацию по умолчанию
    return config_class()

def save_config_to_file(config: BaseBacktestConfig, config_file: str):
    """Сохраняет конфигурацию в файл"""
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(config), f, indent=2, ensure_ascii=False)
        logging.info(f"💾 Конфигурация сохранена в {config_file}")
    except Exception as e:
        logging.error(f"Ошибка сохранения конфигурации: {str(e)}")

def setup_logging(config: BaseBacktestConfig, log_file: str = None):
    """Настраивает логирование"""
    os.makedirs(config.results_dir, exist_ok=True)
    
    if log_file is None:
        log_file = f"{config.results_dir}/backtest.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# ===== ОБЩИЕ КЛАССЫ =====
class BaseDataFetcher:
    """Базовый класс для загрузки данных"""
    
    def __init__(self, config: BaseBacktestConfig):
        self.config = config
        self.data_cache = {}
    
    def load_csv_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Загружает данные из CSV файла"""
        try:
            file_path = os.path.join(self.config.data_dir, f"{symbol}.csv")
            if not os.path.exists(file_path):
                logging.warning(f"Файл не найден: {file_path}")
                return None
            
            df = pd.read_csv(file_path)
            
            # Конвертируем timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Конвертируем числовые колонки
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'turnover']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Удаляем строки с NaN
            df = df.dropna()
            
            # Сортируем по времени
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # Фильтруем по количеству дней
            if self.config.days_back > 0:
                cutoff_date = df['timestamp'].max() - pd.Timedelta(days=self.config.days_back)
                df = df[df['timestamp'] >= cutoff_date].reset_index(drop=True)
            
            logging.info(f"📊 Загружено {len(df)} записей для {symbol}")
            return df
            
        except Exception as e:
            logging.error(f"Ошибка загрузки данных для {symbol}: {str(e)}")
            return None
    
    def get_available_symbols(self) -> List[str]:
        """Получает список доступных символов"""
        if not os.path.exists(self.config.data_dir):
            return []
        
        symbols = []
        for file in os.listdir(self.config.data_dir):
            if file.endswith('.csv'):
                symbol = file.replace('.csv', '')
                symbols.append(symbol)
        
        return symbols

class BaseIndicatorCalculator:
    """Базовый класс для расчета индикаторов"""
    
    @staticmethod
    def calculate_ema(df: pd.DataFrame, period: int, column: str = 'close') -> pd.Series:
        """Рассчитывает EMA"""
        return talib.EMA(df[column], timeperiod=period)
    
    @staticmethod
    def calculate_sma(df: pd.DataFrame, period: int, column: str = 'close') -> pd.Series:
        """Рассчитывает SMA"""
        return talib.SMA(df[column], timeperiod=period)
    
    @staticmethod
    def calculate_rsi(df: pd.DataFrame, period: int = 14, column: str = 'close') -> pd.Series:
        """Рассчитывает RSI"""
        return talib.RSI(df[column], timeperiod=period)
    
    @staticmethod
    def calculate_macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, 
                      signal_period: int = 9, column: str = 'close') -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Рассчитывает MACD"""
        macd, signal, hist = talib.MACD(df[column], fastperiod=fast_period, 
                                       slowperiod=slow_period, signalperiod=signal_period)
        return macd, signal, hist
    
    @staticmethod
    def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, 
                                 column: str = 'close') -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Рассчитывает Bollinger Bands"""
        upper, middle, lower = talib.BBANDS(df[column], timeperiod=period)
        return upper, middle, lower
    
    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Рассчитывает ATR"""
        return talib.ATR(df['high'], df['low'], df['close'], timeperiod=period)
    
    @staticmethod
    def calculate_momentum(df: pd.DataFrame, period: int = 10, column: str = 'close') -> pd.Series:
        """Рассчитывает Momentum"""
        return talib.MOM(df[column], timeperiod=period)
    
    @staticmethod
    def calculate_roc(df: pd.DataFrame, period: int = 10, column: str = 'close') -> pd.Series:
        """Рассчитывает Rate of Change"""
        return talib.ROC(df[column], timeperiod=period)

def print_backtest_summary(trades: List[Dict], start_time: float, config_name: str = "бэктестинга") -> None:
    """Выводит сводку результатов бэктестинга"""
    if not trades:
        print("❌ Нет сделок для анализа")
        return
    
    end_time = time.time()
    duration = end_time - start_time
    
    df_trades = pd.DataFrame(trades)
    
    total_trades = len(df_trades)
    win_trades = len(df_trades[df_trades['profit'] > 0])
    loss_trades = len(df_trades[df_trades['profit'] < 0])
    win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
    
    total_profit = df_trades['profit'].sum()
    avg_profit = df_trades['profit'].mean()
    max_profit = df_trades['profit'].max()
    max_loss = df_trades['profit'].min()
    
    print("\n" + "="*60)
    print(f"📊 РЕЗУЛЬТАТЫ {config_name.upper()}")
    print("="*60)
    print(f"⏱️  Время выполнения: {duration:.2f} секунд")
    print(f"📈 Всего сделок: {total_trades}")
    print(f"✅ Прибыльные: {win_trades}")
    print(f"❌ Убыточные: {loss_trades}")
    print(f"🎯 Винрейт: {win_rate:.1f}%")
    print(f"💰 Общая прибыль: {total_profit:.2f}")
    print(f"📊 Средняя прибыль: {avg_profit:.2f}")
    print(f"🚀 Макс. прибыль: {max_profit:.2f}")
    print(f"📉 Макс. убыток: {max_loss:.2f}")
    print("="*60)
