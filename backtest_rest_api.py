# -*- coding: utf-8 -*-
"""
Альтернативная версия: DOM бектест через REST API (БЕЗ WebSocket)
Используйте если WebSocket не работает на вашей системе
"""

import sys
import requests
import pandas as pd
import json
import time
from datetime import datetime, timezone
from typing import Dict, List

# Исправление кодировки для Windows
if sys.platform == 'win32':
    try:
        import codecs
        if hasattr(sys.stdout, 'buffer'):
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'replace')
    except:
        pass

# Импортируем классы из основного скрипта
import sys
sys.path.insert(0, '.')
from backtest_dom_binance import (DOMBacktestConfig, DOMAnalyzer, 
                                  DOMTradingStrategy, TradingTimeAnalyzer)

class RestAPIBacktester:
    """Бектестер через REST API вместо WebSocket"""
    
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
        self.strategy = DOMTradingStrategy(config)
        self.trades = []
        self.open_positions = []
        self.current_balance = 1000.0
        self.initial_balance = 1000.0
        self._last_price = {}
        
    def get_orderbook(self, symbol: str) -> Dict:
        """Получает orderbook через REST API"""
        try:
            url = "https://api.binance.com/api/v3/depth"
            params = {'symbol': symbol, 'limit': 20}
            response = requests.get(url, params=params, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                # Добавляем символ в формат как в WebSocket
                data['s'] = symbol
                return data
            return {}
        except Exception as e:
            print(f"Ошибка получения {symbol}: {e}")
            return {}
    
    def run_test(self, duration_minutes: int = 60):
        """Запуск теста через REST API"""
        print("\n" + "="*80)
        print("🚀 DOM БЕКТЕСТ ЧЕРЕЗ REST API")
        print("="*80)
        print(f"Длительность: {duration_minutes} минут")
        print(f"Пар: {len(self.config.pairs)}")
        print(f"Интервал опроса: 5 секунд")
        print("="*80 + "\n")
        
        start_time = time.time()
        iteration = 0
        
        try:
            while True:
                elapsed = (time.time() - start_time) / 60  # минуты
                if elapsed >= duration_minutes:
                    print(f"\n⏹️  Завершение по таймеру ({duration_minutes} минут)")
                    break
                
                iteration += 1
                
                # Получаем данные для каждой пары
                for symbol in self.config.pairs:
                    orderbook = self.get_orderbook(symbol)
                    
                    if not orderbook or 'bids' not in orderbook:
                        continue
                    
                    # Анализируем
                    dom_analysis = self.strategy.dom_analyzer.analyze_dom_structure(orderbook)
                    if not dom_analysis:
                        continue
                    
                    # Текущая цена
                    bids = orderbook.get('bids', [])
                    asks = orderbook.get('asks', [])
                    if not bids or not asks:
                        continue
                    
                    best_bid = float(bids[0][0])
                    best_ask = float(asks[0][0])
                    current_price = (best_bid + best_ask) / 2
                    
                    # Проверяем открытые позиции
                    self._update_positions(symbol, current_price)
                    
                    # Генерируем сигнал
                    prev_price = self._last_price.get(symbol)
                    price_movement = self.strategy.detect_price_movement(current_price, prev_price) if prev_price else 'sideways'
                    self._last_price[symbol] = current_price
                    
                    signal = self.strategy.generate_trading_signal(
                        dom_analysis, current_price, price_movement, 
                        datetime.now(timezone.utc), 0
                    )
                    
                    if signal and len(self.open_positions) < self.config.max_open_positions:
                        self._execute_trade(symbol, signal, current_price, dom_analysis)
                
                # Статистика каждые 12 итераций (~1 минута)
                if iteration % 12 == 0:
                    wins = len([t for t in self.trades if t['pnl'] > 0])
                    print(f"\n📊 [{int(elapsed)} мин] Сделок: {len(self.trades)} | "
                          f"Прибыльных: {wins} | Открыто: {len(self.open_positions)} | "
                          f"Баланс: ${self.current_balance:,.2f}")
                
                # Пауза между итерациями (5 секунд)
                time.sleep(5)
        
        except KeyboardInterrupt:
            print("\n⏹️  Остановлено по Ctrl+C")
        
        # Результаты
        return self._get_results()
    
    def _execute_trade(self, symbol: str, signal: Dict, price: float, dom_analysis: Dict):
        """Открывает позицию"""
        position_size = self.current_balance * 0.01  # 1% риска
        
        if signal['direction'] == 'Buy':
            take_profit = price * (1 + self.config.take_profit_percent)
            stop_loss = price * (1 - self.config.stop_loss_percent)
        else:
            take_profit = price * (1 - self.config.take_profit_percent)
            stop_loss = price * (1 + self.config.stop_loss_percent)
        
        position = {
            'symbol': symbol,
            'direction': signal['direction'],
            'entry_price': price,
            'size': position_size,
            'take_profit': take_profit,
            'stop_loss': stop_loss,
            'entry_time': datetime.now(timezone.utc),
            'reason': signal['reason']
        }
        
        self.open_positions.append(position)
        
        print(f"\n🟢 ОТКРЫТА ПОЗИЦИЯ #{len(self.trades) + len(self.open_positions)}")
        print(f"   {symbol} {signal['direction']} @ ${price:,.2f}")
        print(f"   TP: ${take_profit:,.2f} | SL: ${stop_loss:,.2f}")
    
    def _update_positions(self, symbol: str, current_price: float):
        """Обновляет и закрывает позиции"""
        positions_to_close = []
        
        for i, pos in enumerate(self.open_positions):
            if pos['symbol'] != symbol:
                continue
            
            # Проверяем TP/SL
            if pos['direction'] == 'Buy':
                if current_price >= pos['take_profit'] or current_price <= pos['stop_loss']:
                    positions_to_close.append(i)
            else:
                if current_price <= pos['take_profit'] or current_price >= pos['stop_loss']:
                    positions_to_close.append(i)
        
        # Закрываем
        for i in reversed(positions_to_close):
            self._close_position(i, current_price)
    
    def _close_position(self, idx: int, exit_price: float):
        """Закрывает позицию"""
        pos = self.open_positions[idx]
        
        if pos['direction'] == 'Buy':
            pnl = (exit_price - pos['entry_price']) / pos['entry_price']
        else:
            pnl = (pos['entry_price'] - exit_price) / pos['entry_price']
        
        pnl_amount = pnl * pos['size'] * 2  # leverage
        self.current_balance += pnl_amount
        
        trade = {
            'symbol': pos['symbol'],
            'direction': pos['direction'],
            'entry_price': pos['entry_price'],
            'exit_price': exit_price,
            'pnl': pnl_amount,
            'pnl_percent': pnl * 100
        }
        
        self.trades.append(trade)
        del self.open_positions[idx]
        
        emoji = "🟢" if pnl_amount > 0 else "🔴"
        result = "ПРИБЫЛЬ" if pnl_amount > 0 else "УБЫТОК"
        print(f"\n{emoji} ЗАКРЫТА ПОЗИЦИЯ #{len(self.trades)} - {result}")
        print(f"   {pos['symbol']} P&L: ${pnl_amount:,.2f} ({pnl*100:.2f}%)")
        print(f"   Новый баланс: ${self.current_balance:,.2f}")
    
    def _get_results(self) -> Dict:
        """Возвращает результаты"""
        if not self.trades:
            return {'total_trades': 0}
        
        wins = len([t for t in self.trades if t['pnl'] > 0])
        total_pnl = sum(t['pnl'] for t in self.trades)
        
        return {
            'total_trades': len(self.trades),
            'winning_trades': wins,
            'win_rate': (wins / len(self.trades)) * 100,
            'total_pnl': total_pnl,
            'total_pnl_percent': (total_pnl / self.initial_balance) * 100,
            'final_balance': self.current_balance
        }

def main():
    print("\n" + "="*80)
    print("АЛЬТЕРНАТИВНЫЙ БЕКТЕСТ (REST API)")
    print("="*80)
    print("\nЭтот скрипт работает БЕЗ WebSocket!")
    print("Использует обычные HTTP запросы к Binance.")
    print()
    print("Минусы:")
    print("  - Медленнее (опрос каждые 5 сек вместо реального времени)")
    print("  - Меньше данных")
    print()
    print("Плюсы:")
    print("  - НЕТ проблем с WebSocket/asyncio")
    print("  - ТОЧНО работает")
    print("  - Простой и понятный")
    print("="*80 + "\n")
    
    # Конфигурация
    config = DOMBacktestConfig(
        pairs=["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
        min_bid_ask_ratio=1.02,
        min_liquidity_threshold=3000.0,
        max_spread_percent=3.0,
        max_open_positions=10,
    )
    
    # Запуск
    backtester = RestAPIBacktester(config)
    results = backtester.run_test(duration_minutes=60)  # 60 минут
    
    # Результаты
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТЫ")
    print("="*80)
    print(f"Всего сделок: {results.get('total_trades', 0)}")
    
    if results.get('total_trades', 0) > 0:
        print(f"Прибыльных: {results['winning_trades']} ({results['win_rate']:.1f}%)")
        print(f"Общий P&L: ${results['total_pnl']:,.2f} ({results['total_pnl_percent']:.2f}%)")
        print(f"Конечный баланс: ${results['final_balance']:,.2f}")
        print("="*80)
        print("\n✅ REST API ВЕРСИЯ РАБОТАЕТ!")
        print("   Если здесь есть сделки, значит проблема в WebSocket версии.")
    else:
        print("="*80)
        print("\n⚠️  НЕТ СДЕЛОК даже через REST API!")
        print("   Проблема в условиях фильтрации, не в WebSocket.")
    
    # Сохраняем
    with open('rest_api_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nРезультаты сохранены: rest_api_results.json")

if __name__ == "__main__":
    main()

