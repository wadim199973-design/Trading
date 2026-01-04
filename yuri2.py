# -*- coding: utf-8 -*-
"""
Created on Tue May 13 07:20:30 2025

@author: wadim
"""

import time
import logging
import pandas as pd
from pybit.unified_trading import HTTP
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.volatility import AverageTrueRange
from ta.momentum import RSIIndicator
import talib

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Конфигурация
API_KEY = 'gn2LMo3fdYcKqbXlpN'
API_SECRET = '5vfVimlCQy3VAuZY94cVMHrBqz96OQavOElf'
SYMBOL = [
    "DOGEUSDT", "SOLUSDT", "DOTUSDT", "CVCUSDT", "YGGUSDT", "MASAUSDT", 
    "ADAUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT","API3USDT", "C98USDT", "COTIUSDT", 
    "LTCUSDT", "APTUSDT", "NEARUSDT", "ICPUSDT", "AAVEUSDT","BELUSDT", "ONTUSDT","KAVAUSDT",
    "ATOMUSDT", "ARBUSDT", "OPUSDT", "SEIUSDT", "PYTHUSDT", "WAVESUSDT", "XNOUSDT", "LDOUSDT", "SUNUSDT",
    "GMTUSDT", "APEUSDT", "RUNEUSDT", "ZILUSDT", "JASMYUSDT", "OGNUSDT", "SHIB1000USDT","XRPUSDT", "BNBUSDT", 
    "MANAUSDT", "SFPUSDT", "UNIUSDT", "CROUSDT", "FILUSDT", "ALGOUSDT", "XLMUSDT", "HBARUSDT", "THETAUSDT", "EGLDUSDT",
    "EOSUSDT", "ZENUSDT", "GALAUSDT", "CRVUSDT", "DYDXUSDT", "STORJUSDT", "SUSHIUSDT", "1000CATSUSDT", "1000CATUSDT", "ZROUSDT",
    "POPCATUSDT", "DYMUSDT", "ZETAUSDT", "ALTUSDT", "WIFUSDT", "NFPUSDT", "POLYXUSDT", "STPTUSDT", "TONUSDT", "OGUSDT", "HIFIUSDT",
    "WLDUSDT", "PENDLEUSDT", "XVGUSDT", "1000PEPEUSDT", "PHBUSDT", "EDUUSDT", "IDUSDT", "BLURUSDT", "1000FLOKIUSDT", "TWTUSDT", "INJUSDT", 
    "OMNIUSDT", "ORCAUSDT", "STRKUSDT", "XAIUSDT", "JTOUSDT", "CAKEUSDT", "AUCTIONUSDT", "MAVUSDT", "HFTUSDT", "CFXUSDT", "AGLDUSDT", "KDAUSDT", "MNTUSDT"


]
INTERVAL = 60  # Время ожидания между итерациями (в секундах)
QUANTITY = 0.05
LEVERAGE = 50
TAKE_PROFIT = 0.005
STOP_LOSS = 0.002
TRAILING_STOP_ACTIVATION = 0.003
TRAILING_STOP_CALLBACK = 0.001
MAX_HOLD_TIME = 60 * 60  # 1 час

# Инициализация сессии Bybit
session = HTTP(
    testnet=True,
    api_key=API_KEY,
    api_secret=API_SECRET
)

def fetch_klines(symbol, interval, limit=100):
    try:
        response = session.get_kline(
            category='linear',
            symbol=symbol,
            interval=str(interval),
            limit=limit
        )
        df = pd.DataFrame(response['result']['list'], columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', '_', '__', '___', '____', '_____', '______'
        ])
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = df.astype(float)
        return df
    except Exception as e:
        logging.error(f"Ошибка при получении свечей: {e}")
        return pd.DataFrame()

def apply_indicators(df):
    try:
        df['ema_20'] = EMAIndicator(df['close'], window=20).ema_indicator()
        df['ema_50'] = EMAIndicator(df['close'], window=50).ema_indicator()
        df['rsi'] = RSIIndicator(df['close']).rsi()
        macd = MACD(df['close'])
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()
        df['adx'] = ADXIndicator(df['high'], df['low'], df['close']).adx()
        df['atr'] = AverageTrueRange(df['high'], df['low'], df['close']).average_true_range()
        return df
    except Exception as e:
        logging.error(f"Ошибка при расчете индикаторов: {e}")
        return df

def get_latest_signals(df):
    if df.empty or len(df) < 2:
        return None

    latest = df.iloc[-1]
    previous = df.iloc[-2]

    if pd.isnull([latest['ema_20'], latest['ema_50'], latest['rsi'],
                  latest['macd'], latest['macd_signal'], latest['adx']]).any():
        return None

    long_signal = (
        latest['ema_20'] > latest['ema_50'] and
        latest['rsi'] > 50 and
        latest['macd'] > latest['macd_signal'] and
        latest['adx'] > 25
    )

    short_signal = (
        latest['ema_20'] < latest['ema_50'] and
        latest['rsi'] < 50 and
        latest['macd'] < latest['macd_signal'] and
        latest['adx'] > 25
    )

    return 'long' if long_signal else 'short' if short_signal else None

def get_open_position():
    try:
        positions = session.get_positions(category='linear', symbol=SYMBOL)
        for pos in positions['result']['list']:
            if float(pos['size']) > 0:
                return pos
    except Exception as e:
        logging.error(f"Ошибка получения позиции: {e}")
    return None

def place_order(side, qty):
    try:
        return session.place_order(
            category='linear',
            symbol=SYMBOL,
            side=side,
            order_type='Market',
            qty=qty,
            time_in_force='GoodTillCancel'
        )
    except Exception as e:
        logging.error(f"Ошибка размещения ордера: {e}")
        return None

def set_trailing_stop(side, activation_price):
    try:
        session.set_trading_stop(
            category='linear',
            symbol=SYMBOL,
            trailing_stop=TRAILING_STOP_CALLBACK,
            active_price=activation_price
        )
    except Exception as e:
        logging.error(f"Ошибка установки трейлинг-стопа: {e}")

def close_position(position):
    side = 'Sell' if position['side'] == 'Buy' else 'Buy'
    qty = float(position['size'])
    try:
        session.place_order(
            category='linear',
            symbol=SYMBOL,
            side=side,
            order_type='Market',
            qty=qty,
            time_in_force='GoodTillCancel',
            reduce_only=True
        )
        logging.info(f"Позиция закрыта: {side} {qty}")
    except Exception as e:
        logging.error(f"Ошибка закрытия позиции: {e}")

# Основной торговый цикл
def main():
    
    logging.info("Бот запущен.")
    entry_time = None

while True:
    df = fetch_klines(SYMBOL, interval=1)
    df = apply_indicators(df)
    signal = get_latest_signals(df)
    position = get_open_position()

    if position is None and signal:
        side = 'Buy' if signal == 'long' else 'Sell'
        order = place_order(side, QUANTITY)
        entry_time = time.time()
        logging.info(f"Открыта позиция: {side}")

        try:
            last_price = df['close'].iloc[-1]
            activation_price = last_price * (1 + TRAILING_STOP_ACTIVATION if side == 'Buy' else 1 - TRAILING_STOP_ACTIVATION)
            set_trailing_stop(side, activation_price)
        except Exception as e:
            logging.error(f"Ошибка при установке трейлинг-стопа: {e}")

    elif position:
        if entry_time and time.time() - entry_time >= MAX_HOLD_TIME:
            logging.info("Время удержания позиции истекло. Закрытие позиции.")
            close_position(position)
            entry_time = None

    time.sleep(INTERVAL)
    if __name__ == "__main__":
        main()
