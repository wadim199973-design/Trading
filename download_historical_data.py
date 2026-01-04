# -*- coding: utf-8 -*-
"""
Скрипт для загрузки исторических данных с Binance за год
Загружает свечи (klines) и сделки (trades) для всех пар
"""

import requests
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
from typing import List, Dict
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_historical.log"),
        logging.StreamHandler()
    ]
)

class BinanceHistoricalDownloader:
    """Класс для загрузки исторических данных с Binance"""
    
    def __init__(self, pairs: List[str], output_dir: str = "historical_data"):
        self.pairs = pairs
        self.output_dir = output_dir
        self.base_url = "https://api.binance.com/api/v3"
        
        # Создаём директории
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(f"{output_dir}/klines", exist_ok=True)
        os.makedirs(f"{output_dir}/trades", exist_ok=True)
        
    def download_klines(self, symbol: str, interval: str = "1m", 
                       start_date: datetime = None, end_date: datetime = None):
        """
        Загружает свечи (klines) для символа
        
        Параметры:
        - symbol: торговая пара (например, "BTCUSDT")
        - interval: интервал свечей (1m, 5m, 15m, 1h, 4h, 1d)
        - start_date: начальная дата
        - end_date: конечная дата
        """
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now()
        
        logging.info(f"📊 Загрузка klines для {symbol} ({interval}) с {start_date.date()} по {end_date.date()}")
        
        all_klines = []
        current_start = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)
        
        request_count = 0
        
        while current_start < end_timestamp:
            try:
                # Binance ограничивает 1000 свечей за запрос
                url = f"{self.base_url}/klines"
                params = {
                    'symbol': symbol,
                    'interval': interval,
                    'startTime': current_start,
                    'endTime': end_timestamp,
                    'limit': 1000
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code != 200:
                    logging.error(f"❌ Ошибка {response.status_code}: {response.text}")
                    time.sleep(60)
                    continue
                
                klines = response.json()
                
                if not klines:
                    break
                
                all_klines.extend(klines)
                
                # Обновляем timestamp для следующего запроса
                current_start = klines[-1][0] + 1
                
                request_count += 1
                
                # Показываем прогресс
                current_date = datetime.fromtimestamp(current_start / 1000)
                progress = ((current_start - int(start_date.timestamp() * 1000)) / 
                           (end_timestamp - int(start_date.timestamp() * 1000))) * 100
                
                logging.info(f"   {symbol}: {progress:.1f}% | {current_date.date()} | "
                           f"Загружено: {len(all_klines)} свечей")
                
                # Задержка для соблюдения лимитов API (1200 запросов/минута)
                if request_count % 100 == 0:
                    logging.info(f"   ⏸️ Пауза 5 сек (лимиты API)...")
                    time.sleep(5)
                else:
                    time.sleep(0.1)
                    
            except Exception as e:
                logging.error(f"❌ Ошибка загрузки {symbol}: {e}")
                time.sleep(5)
                continue
        
        # Преобразуем в DataFrame
        if all_klines:
            df = pd.DataFrame(all_klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades_count',
                'taker_buy_base', 'taker_buy_quote', 'ignore'
            ])
            
            # Конвертируем типы
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            
            # Сохраняем
            filename = f"{self.output_dir}/klines/{symbol}_{interval}_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv"
            df.to_csv(filename, index=False)
            
            logging.info(f"✅ {symbol}: Сохранено {len(df)} свечей в {filename}")
            
            return df
        
        return None
    
    def download_orderbook_snapshots(self, symbol: str, depth: int = 20):
        """
        Загружает текущий снимок orderbook (стакана)
        
        ВНИМАНИЕ: Binance не предоставляет исторические данные orderbook!
        Можно получить только текущий снимок.
        """
        logging.info(f"📖 Загрузка текущего orderbook для {symbol}")
        
        try:
            url = f"{self.base_url}/depth"
            params = {
                'symbol': symbol,
                'limit': depth
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                orderbook = response.json()
                
                # Сохраняем
                filename = f"{self.output_dir}/orderbook_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(filename, 'w') as f:
                    json.dump(orderbook, f, indent=2)
                
                logging.info(f"✅ Orderbook {symbol} сохранён в {filename}")
                return orderbook
            else:
                logging.error(f"❌ Ошибка {response.status_code}: {response.text}")
                
        except Exception as e:
            logging.error(f"❌ Ошибка загрузки orderbook {symbol}: {e}")
        
        return None
    
    def download_recent_trades(self, symbol: str, limit: int = 1000):
        """
        Загружает последние сделки
        
        ВНИМАНИЕ: Binance позволяет загрузить только последние ~1000 сделок.
        Для более старых данных используйте Historical Trades API (требует API ключ).
        """
        logging.info(f"💰 Загрузка последних сделок для {symbol}")
        
        try:
            url = f"{self.base_url}/trades"
            params = {
                'symbol': symbol,
                'limit': limit
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                trades = response.json()
                
                # Преобразуем в DataFrame
                df = pd.DataFrame(trades)
                df['time'] = pd.to_datetime(df['time'], unit='ms')
                
                # Сохраняем
                filename = f"{self.output_dir}/trades/{symbol}_recent_trades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(filename, index=False)
                
                logging.info(f"✅ {symbol}: Сохранено {len(df)} сделок в {filename}")
                return df
            else:
                logging.error(f"❌ Ошибка {response.status_code}: {response.text}")
                
        except Exception as e:
            logging.error(f"❌ Ошибка загрузки сделок {symbol}: {e}")
        
        return None
    
    def download_all_pairs(self, interval: str = "1m", days_back: int = 365):
        """
        Загружает данные для всех пар
        
        Параметры:
        - interval: интервал свечей (1m, 5m, 15m, 1h, 4h, 1d)
        - days_back: количество дней назад
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logging.info(f"🚀 Начинаем загрузку данных для {len(self.pairs)} пар")
        logging.info(f"📅 Период: {start_date.date()} - {end_date.date()}")
        logging.info(f"⏱️ Интервал: {interval}")
        
        for i, symbol in enumerate(self.pairs, 1):
            logging.info(f"\n{'='*60}")
            logging.info(f"📊 [{i}/{len(self.pairs)}] Обработка {symbol}")
            logging.info(f"{'='*60}")
            
            try:
                # Загружаем klines (свечи)
                self.download_klines(symbol, interval, start_date, end_date)
                
                # Небольшая пауза между парами
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"❌ Критическая ошибка при обработке {symbol}: {e}")
                continue
        
        logging.info(f"\n{'='*60}")
        logging.info(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА!")
        logging.info(f"📂 Данные сохранены в директории: {self.output_dir}/")
        logging.info(f"{'='*60}")


def main():
    """Главная функция"""
    
    # Список пар для загрузки
    pairs = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT",
        "DOTUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
        "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
        "SUIUSDT", "WLDUSDT", "THETAUSDT", "FILUSDT", "ICPUSDT"
    ]
    
    # Создаём загрузчик
    downloader = BinanceHistoricalDownloader(pairs=pairs)
    
    print("\n" + "="*60)
    print("📥 ЗАГРУЗКА ИСТОРИЧЕСКИХ ДАННЫХ С BINANCE")
    print("="*60)
    print(f"📊 Пар для загрузки: {len(pairs)}")
    print(f"📅 Период: последний год")
    print(f"⏱️ Интервал: 1 минута")
    print(f"📂 Папка сохранения: historical_data/")
    print("="*60)
    
    choice = input("\n🔴 Выберите режим:\n"
                   "1 - Загрузить свечи (klines) за год - РЕКОМЕНДУЕТСЯ\n"
                   "2 - Загрузить только текущие orderbooks (без истории)\n"
                   "3 - Загрузить последние сделки\n"
                   "Ваш выбор (1-3): ")
    
    if choice == "1":
        print("\n⏳ Начинаем загрузку свечей за год...")
        print("⚠️ Это может занять несколько часов!")
        print("💡 Вы можете прервать процесс в любой момент (Ctrl+C)\n")
        
        try:
            # Загружаем свечи за год
            downloader.download_all_pairs(interval="1m", days_back=365)
        except KeyboardInterrupt:
            print("\n\n⏹️ Загрузка прервана пользователем")
            print("📂 Частично загруженные данные сохранены в historical_data/")
    
    elif choice == "2":
        print("\n⚠️ ВНИМАНИЕ: Binance не предоставляет исторические orderbook!")
        print("Будут загружены только текущие снимки стакана.\n")
        
        for symbol in pairs:
            downloader.download_orderbook_snapshots(symbol, depth=20)
            time.sleep(0.5)
    
    elif choice == "3":
        print("\n💰 Загружаем последние сделки...")
        
        for symbol in pairs:
            downloader.download_recent_trades(symbol, limit=1000)
            time.sleep(0.5)
    
    else:
        print("❌ Неверный выбор!")
    
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()

