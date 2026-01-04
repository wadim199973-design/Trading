# -*- coding: utf-8 -*-
"""
Диагностический скрипт для выявления проблем с Binance данными
"""

import logging
import json
import time
from binance import ThreadedWebsocketManager
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

class BinanceDiagnostics:
    """Диагностика Binance WebSocket"""
    
    def __init__(self):
        self.messages_received = 0
        self.last_orderbook = None
        
    def analyze_message(self, msg):
        """Анализирует сообщение от Binance"""
        try:
            self.messages_received += 1
            
            if isinstance(msg, str):
                data = json.loads(msg)
            else:
                data = msg
            
            print(f"\n{'='*80}")
            print(f"📨 Сообщение #{self.messages_received}")
            print(f"{'='*80}")
            
            # Тип данных
            print(f"Тип: {type(data)}")
            print(f"Ключи: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
            
            # Проверяем наличие orderbook данных
            if 'bids' in data and 'asks' in data:
                bids = data['bids']
                asks = data['asks']
                
                print(f"\n✅ ORDERBOOK ДАННЫЕ НАЙДЕНЫ!")
                print(f"   Symbol: {data.get('s', data.get('symbol', 'N/A'))}")
                print(f"   Bids: {len(bids)} уровней")
                print(f"   Asks: {len(asks)} уровней")
                
                if bids and asks:
                    # Лучшие цены
                    best_bid_price = float(bids[0][0])
                    best_bid_size = float(bids[0][1])
                    best_ask_price = float(asks[0][0])
                    best_ask_size = float(asks[0][1])
                    
                    print(f"\n   📊 Лучший BID: ${best_bid_price:,.2f} | Размер: {best_bid_size:.6f}")
                    print(f"   📊 Лучший ASK: ${best_ask_price:,.2f} | Размер: {best_ask_size:.6f}")
                    
                    # Спред
                    spread = best_ask_price - best_bid_price
                    spread_pct = (spread / best_bid_price) * 100
                    print(f"   📏 Спред: ${spread:.2f} ({spread_pct:.4f}%)")
                    
                    # Ликвидность в USDT (цена × размер)
                    total_bid_size = sum(float(b[1]) for b in bids)
                    total_ask_size = sum(float(a[1]) for a in asks)
                    
                    # Ликвидность в USDT
                    bid_liquidity_usdt = sum(float(b[0]) * float(b[1]) for b in bids)
                    ask_liquidity_usdt = sum(float(a[0]) * float(a[1]) for a in asks)
                    
                    print(f"\n   💰 ЛИКВИДНОСТЬ:")
                    print(f"      Bid (в монетах): {total_bid_size:.6f}")
                    print(f"      Ask (в монетах): {total_ask_size:.6f}")
                    print(f"      💵 Bid (в USDT): ${bid_liquidity_usdt:,.2f}")
                    print(f"      💵 Ask (в USDT): ${ask_liquidity_usdt:,.2f}")
                    
                    # Соотношение
                    ratio = bid_liquidity_usdt / ask_liquidity_usdt if ask_liquidity_usdt > 0 else 0
                    print(f"      Bid/Ask ratio (USDT): {ratio:.3f}")
                    
                    # Проверка порогов (теперь в USDT)
                    threshold_usdt = 50000  # $50,000
                    print(f"\n   ⚠️  ПРОВЕРКА ПОРОГОВ (В USDT):")
                    print(f"      min_liquidity_threshold = ${threshold_usdt:,}")
                    print(f"      Bid liquidity (${bid_liquidity_usdt:,.0f}) >= ${threshold_usdt:,}? {bid_liquidity_usdt >= threshold_usdt}")
                    print(f"      Ask liquidity (${ask_liquidity_usdt:,.0f}) >= ${threshold_usdt:,}? {ask_liquidity_usdt >= threshold_usdt}")
                    
                    if bid_liquidity_usdt < threshold_usdt:
                        print(f"      ❌ BID ЛИКВИДНОСТЬ НИЖЕ ПОРОГА!")
                        print(f"         Рекомендуемый порог: ${bid_liquidity_usdt * 0.8:,.0f}")
                    
                    if ask_liquidity_usdt < threshold_usdt:
                        print(f"      ❌ ASK ЛИКВИДНОСТЬ НИЖЕ ПОРОГА!")
                        print(f"         Рекомендуемый порог: ${ask_liquidity_usdt * 0.8:,.0f}")
                    
                    # Сохраняем последний orderbook
                    self.last_orderbook = {
                        'bid_liquidity_coins': total_bid_size,
                        'ask_liquidity_coins': total_ask_size,
                        'bid_liquidity_usdt': bid_liquidity_usdt,
                        'ask_liquidity_usdt': ask_liquidity_usdt,
                        'ratio': ratio,
                        'spread_pct': spread_pct
                    }
                    
                    print(f"\n   📋 Первые 5 уровней BID:")
                    for i, bid in enumerate(bids[:5]):
                        print(f"      {i+1}. ${float(bid[0]):,.2f} × {float(bid[1]):.6f}")
                    
                    print(f"\n   📋 Первые 5 уровней ASK:")
                    for i, ask in enumerate(asks[:5]):
                        print(f"      {i+1}. ${float(ask[0]):,.2f} × {float(ask[1]):.6f}")
                
            else:
                print(f"\n❌ ORDERBOOK ДАННЫЕ НЕ НАЙДЕНЫ")
                print(f"   Содержимое (первые 500 символов):")
                print(f"   {str(data)[:500]}")
                
        except Exception as e:
            print(f"\n❌ ОШИБКА АНАЛИЗА: {e}")
            import traceback
            print(traceback.format_exc())
    
    def run_diagnostics(self, symbol="BTCUSDT", duration=30):
        """Запуск диагностики"""
        print(f"\n{'='*80}")
        print(f"🔬 ДИАГНОСТИКА BINANCE WEBSOCKET")
        print(f"{'='*80}")
        print(f"Символ: {symbol}")
        print(f"Длительность: {duration} секунд")
        print(f"Время начала: {datetime.now()}")
        print(f"{'='*80}\n")
        
        try:
            # Создаём WebSocket
            twm = ThreadedWebsocketManager()
            twm.start()
            
            print("✅ WebSocket менеджер запущен")
            time.sleep(2)
            
            # Подписываемся
            stream_key = twm.start_depth_socket(
                callback=self.analyze_message,
                symbol=symbol
            )
            
            print(f"✅ Подписка создана: {stream_key}")
            print(f"\n⏳ Ожидание данных в течение {duration} секунд...\n")
            
            # Ждём
            start_time = time.time()
            while (time.time() - start_time) < duration:
                time.sleep(1)
                
                # Показываем прогресс каждые 5 секунд
                elapsed = int(time.time() - start_time)
                if elapsed % 5 == 0 and elapsed > 0:
                    print(f"\n⏱️  Прошло: {elapsed}/{duration} сек | Получено сообщений: {self.messages_received}")
            
            # Останавливаем
            print(f"\n⏹️  Остановка...")
            twm.stop()
            
            # Итоговая статистика
            print(f"\n{'='*80}")
            print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
            print(f"{'='*80}")
            print(f"Всего получено сообщений: {self.messages_received}")
            print(f"Частота: {self.messages_received / duration:.2f} сообщений/сек")
            
            if self.last_orderbook:
                print(f"\n📈 ПОСЛЕДНИЕ ДАННЫЕ ORDERBOOK:")
                print(f"   Bid liquidity: {self.last_orderbook['bid_liquidity_coins']:.6f} монет")
                print(f"   Ask liquidity: {self.last_orderbook['ask_liquidity_coins']:.6f} монет")
                print(f"   💵 Bid liquidity: ${self.last_orderbook['bid_liquidity_usdt']:,.2f} USDT")
                print(f"   💵 Ask liquidity: ${self.last_orderbook['ask_liquidity_usdt']:,.2f} USDT")
                print(f"   Bid/Ask ratio: {self.last_orderbook['ratio']:.3f}")
                print(f"   Spread: {self.last_orderbook['spread_pct']:.4f}%")
                
                # Рекомендации (теперь в USDT)
                print(f"\n💡 РЕКОМЕНДАЦИИ:")
                avg_liq_usdt = (self.last_orderbook['bid_liquidity_usdt'] + self.last_orderbook['ask_liquidity_usdt']) / 2
                recommended_threshold = avg_liq_usdt * 0.5  # 50% от средней ликвидности
                print(f"   Средняя ликвидность: ${avg_liq_usdt:,.2f} USDT")
                print(f"   Рекомендуемый min_liquidity_threshold: ${recommended_threshold:,.0f} USDT")
                print(f"   Текущий порог: $50,000 USDT")
                
                if avg_liq_usdt < 50000:
                    print(f"   ⚠️  Средняя ликвидность ниже текущего порога!")
                    print(f"   Используйте порог: ${recommended_threshold:,.0f}")
            else:
                print(f"\n❌ НЕТ ДАННЫХ ORDERBOOK - ПРОВЕРЬТЕ ПОДКЛЮЧЕНИЕ!")
            
            print(f"{'='*80}")
            
        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            print(traceback.format_exc())

def main():
    """Главная функция"""
    diagnostics = BinanceDiagnostics()
    
    # Выбор символа
    print("\n🔬 Выберите символ для диагностики:")
    print("1 - BTCUSDT (Bitcoin)")
    print("2 - ETHUSDT (Ethereum)")
    print("3 - BNBUSDT (BNB)")
    
    choice = input("\nВаш выбор (1-3, Enter=BTCUSDT): ").strip()
    
    symbols = {
        "1": "BTCUSDT",
        "2": "ETHUSDT",
        "3": "BNBUSDT",
        "": "BTCUSDT"
    }
    
    symbol = symbols.get(choice, "BTCUSDT")
    
    # Запуск
    diagnostics.run_diagnostics(symbol=symbol, duration=30)

if __name__ == "__main__":
    main()

