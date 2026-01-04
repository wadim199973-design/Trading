# -*- coding: utf-8 -*-
"""
Конвертер исторических свечей в симулированный orderbook
Для использования с backtest_dom_binance.py в режиме replay
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import List, Dict
import logging
import random

logging.basicConfig(level=logging.INFO)

class KlinesToOrderbookConverter:
    """Конвертирует свечи в симулированный orderbook для бектестинга"""
    
    def __init__(self, input_dir: str = "historical_data/klines", 
                 output_dir: str = "data"):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def simulate_orderbook_from_kline(self, symbol: str, kline: Dict) -> Dict:
        """
        Симулирует orderbook на основе данных свечи
        
        Примечание: Это упрощённая симуляция для бектестинга.
        Реальный orderbook более сложный.
        """
        close_price = float(kline['close'])
        high_price = float(kline['high'])
        low_price = float(kline['low'])
        volume = float(kline['volume'])
        
        # Генерируем bid/ask с небольшим спредом
        spread_percent = 0.001  # 0.1% спред
        best_bid = close_price * (1 - spread_percent / 2)
        best_ask = close_price * (1 + spread_percent / 2)
        
        # Генерируем уровни стакана (20 уровней)
        bids = []
        asks = []
        
        price_step = close_price * 0.0001  # Шаг цены 0.01%
        
        for i in range(20):
            # Bids (покупки) - ниже текущей цены
            bid_price = best_bid - (i * price_step)
            bid_size = volume * random.uniform(0.01, 0.1) / 20  # Случайный объем
            bids.append([f"{bid_price:.8f}", f"{bid_size:.8f}"])
            
            # Asks (продажи) - выше текущей цены
            ask_price = best_ask + (i * price_step)
            ask_size = volume * random.uniform(0.01, 0.1) / 20
            asks.append([f"{ask_price:.8f}", f"{ask_size:.8f}"])
        
        orderbook = {
            's': symbol,
            'bids': bids,
            'asks': asks,
            'timestamp': kline['open_time'],
            'simulated': True  # Флаг что это симуляция
        }
        
        return orderbook
    
    def convert_csv_to_orderbook_json(self, csv_file: str, symbol: str):
        """Конвертирует CSV файл со свечами в JSON orderbook"""
        logging.info(f"📊 Конвертация {csv_file}...")
        
        try:
            # Читаем CSV
            df = pd.read_csv(csv_file)
            
            if df.empty:
                logging.warning(f"⚠️ Пустой файл: {csv_file}")
                return
            
            logging.info(f"   Загружено {len(df)} свечей")
            
            # Конвертируем каждую свечу в orderbook
            orderbooks = []
            
            for idx, row in df.iterrows():
                orderbook = self.simulate_orderbook_from_kline(symbol, row)
                
                record = {
                    'timestamp': str(row['open_time']),
                    'symbol': symbol,
                    'data': orderbook
                }
                
                orderbooks.append(record)
                
                # Прогресс каждые 10000 записей
                if (idx + 1) % 10000 == 0:
                    logging.info(f"   Обработано: {idx + 1}/{len(df)}")
            
            # Сохраняем в JSON
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(
                self.output_dir, 
                f"orderbook_data_{symbol}_simulated_{timestamp}.json"
            )
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(orderbooks, f, indent=2, ensure_ascii=False)
            
            file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
            
            logging.info(f"✅ {symbol}: Сохранено {len(orderbooks)} orderbooks")
            logging.info(f"   Файл: {output_file} ({file_size_mb:.1f} MB)")
            
            return output_file
            
        except Exception as e:
            logging.error(f"❌ Ошибка конвертации {csv_file}: {e}")
            return None
    
    def convert_all_files(self):
        """Конвертирует все CSV файлы в директории"""
        if not os.path.exists(self.input_dir):
            logging.error(f"❌ Директория {self.input_dir} не найдена!")
            logging.info("💡 Сначала запустите download_historical_data.py")
            return
        
        csv_files = [f for f in os.listdir(self.input_dir) if f.endswith('.csv')]
        
        if not csv_files:
            logging.error(f"❌ CSV файлы не найдены в {self.input_dir}")
            return
        
        logging.info(f"🚀 Найдено {len(csv_files)} CSV файлов для конвертации")
        
        for i, csv_file in enumerate(csv_files, 1):
            logging.info(f"\n[{i}/{len(csv_files)}] {csv_file}")
            
            # Извлекаем символ из имени файла
            symbol = csv_file.split('_')[0]
            
            filepath = os.path.join(self.input_dir, csv_file)
            self.convert_csv_to_orderbook_json(filepath, symbol)
        
        logging.info(f"\n{'='*60}")
        logging.info(f"✅ КОНВЕРТАЦИЯ ЗАВЕРШЕНА!")
        logging.info(f"📂 JSON файлы сохранены в: {self.output_dir}/")
        logging.info(f"{'='*60}")


def main():
    print("\n" + "="*60)
    print("🔄 КОНВЕРТЕР: СВЕЧИ → ORDERBOOK JSON")
    print("="*60)
    print("Этот скрипт конвертирует загруженные CSV свечи")
    print("в формат orderbook для использования в бектесте")
    print("="*60)
    
    converter = KlinesToOrderbookConverter()
    converter.convert_all_files()


if __name__ == "__main__":
    main()

