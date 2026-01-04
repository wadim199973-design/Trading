# -*- coding: utf-8 -*-
import time
import pandas as pd
import logging
import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
try:
    from pybit.unified_trading import HTTP, WebSocket
except ImportError:
    from pybit import HTTP, WebSocket
from dataclasses import dataclass
import queue

# ===== НАСТРОЙКИ =====
@dataclass
class RealtimeDOMConfig:
    """Конфигурация для реального времени DOM торгового бота"""
    # API ключи
    api_key: str = "YMpyspOSx3m52Zx3va"
    api_secret: str = "MGo33EXT6BiFhcc0gdxWATcKgttKOC5bAl5f"
    testnet: bool = True
    
    # Торговые пары (ограничено для реального времени)
    pairs: List[str] = None
    
    # Параметры DOM анализа
    dom_levels: int = 25
    min_bid_ask_ratio: float = 1.3  # Еще более ослаблено для реального времени
    min_liquidity_threshold: float = 10000  # Снижено для реального времени
    price_movement_threshold: float = 0.005  # Более чувствительно
    
    # Параметры торговли
    leverage: int = 20
    max_open_positions: int = 5  # Меньше для реального времени
    risk_per_trade: float = 0.8  # Снижен риск
    take_profit_percent: float = 0.015  # 1.5%
    stop_loss_percent: float = 0.008   # 0.8%
    max_hold_time: int = 180  # 3 минуты
    
    # Интервалы
    dom_update_interval: float = 0.5  # 500мс для реального времени
    position_check_interval: int = 5  # секунды
    
    # Фильтры по времени торговли
    trade_only_major_sessions: bool = True
    new_york_session: bool = True
    london_session: bool = True
    tokyo_session: bool = True
    
    # WebSocket настройки
    websocket_reconnect_interval: int = 5
    max_websocket_errors: int = 10
    
    def __post_init__(self):
        if self.pairs is None:
            # Ограниченный список для реального времени
            self.pairs = [
                "BTCUSDT", "ETHUSDT", "DOGEUSDT", "SOLUSDT", "DOTUSDT", 
                "ADAUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT", "ATOMUSDT"
            ]

# ===== КЛАСС ДЛЯ АНАЛИЗА ВРЕМЕНИ ТОРГОВЛИ =====
class TradingTimeAnalyzer:
    """Класс для анализа времени торговли и определения активных сессий"""
    
    def __init__(self, config: RealtimeDOMConfig):
        self.config = config
        
    def is_major_session_active(self, timestamp: datetime = None) -> bool:
        """Проверяет, активна ли одна из основных торговых сессий"""
        if not self.config.trade_only_major_sessions:
            return True
            
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        else:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
            
        hour = timestamp.hour
        minute = timestamp.minute
        
        # Нью-Йорк: 14:30-21:00 UTC
        if self.config.new_york_session:
            if hour > 14 and hour < 21:
                return True
            elif hour == 14 and minute >= 30:
                return True
            elif hour == 21 and minute == 0:
                return True
            
        # Лондон: 08:00-16:00 UTC
        if self.config.london_session and 8 <= hour < 16:
            return True
            
        # Токио: 00:00-09:00 UTC
        if self.config.tokyo_session and (0 <= hour < 9):
            return True
            
        return False

# ===== КЛАСС ДЛЯ РЕАЛЬНОГО ВРЕМЕНИ DOM =====
class RealtimeDOMAnalyzer:
    """Класс для анализа DOM в реальном времени"""
    
    def __init__(self, config: RealtimeDOMConfig):
        self.config = config
        self.time_analyzer = TradingTimeAnalyzer(config)
        self.orderbook_data = {}
        self.price_history = {}
        self.websocket_errors = 0
        
    def analyze_realtime_dom(self, symbol: str, orderbook_data: Dict) -> Dict:
        """Анализирует DOM в реальном времени"""
        try:
            if not orderbook_data or 'data' not in orderbook_data:
                return {}
            
            data = orderbook_data['data']
            bids = data.get('b', [])
            asks = data.get('a', [])
            
            if not bids or not asks:
                return {}
            
            # Преобразуем в DataFrame
            bids_df = pd.DataFrame(bids, columns=['price', 'size'])
            asks_df = pd.DataFrame(asks, columns=['price', 'size'])
            
            bids_df['price'] = bids_df['price'].astype(float)
            bids_df['size'] = bids_df['size'].astype(float)
            asks_df['price'] = asks_df['price'].astype(float)
            asks_df['size'] = asks_df['size'].astype(float)
            
            # Анализ ликвидности
            bid_liquidity = bids_df['size'].sum()
            ask_liquidity = asks_df['size'].sum()
            bid_ask_ratio = bid_liquidity / ask_liquidity if ask_liquidity > 1e-10 else 0
            
            # Анализ спреда
            spread = float(asks_df.iloc[0]['price']) - float(bids_df.iloc[0]['price'])
            spread_percent = (spread / float(bids_df.iloc[0]['price'])) * 100
            
            # Поиск кластеров (упрощенный для реального времени)
            bid_clusters = self.find_fast_clusters(bids_df, 'bid')
            ask_clusters = self.find_fast_clusters(asks_df, 'ask')
            
            # Определение уровней (упрощенное)
            support_levels = self.identify_fast_support_levels(bids_df, bid_clusters)
            resistance_levels = self.identify_fast_resistance_levels(asks_df, ask_clusters)
            
            # Анализ движения цены
            current_price = float(bids_df.iloc[0]['price'])
            price_movement = self.analyze_price_movement(symbol, current_price)
            
            return {
                'symbol': symbol,
                'bid_liquidity': bid_liquidity,
                'ask_liquidity': ask_liquidity,
                'bid_ask_ratio': bid_ask_ratio,
                'spread': spread,
                'spread_percent': spread_percent,
                'support_levels': support_levels,
                'resistance_levels': resistance_levels,
                'current_price': current_price,
                'price_movement': price_movement,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logging.error(f"Ошибка анализа реального времени DOM для {symbol}: {str(e)}")
            return {}
    
    def find_fast_clusters(self, orders_df: pd.DataFrame, order_type: str) -> List[Dict]:
        """Быстрый поиск кластеров для реального времени"""
        clusters = []
        
        if len(orders_df) < 2:
            return clusters
        
        # Упрощенный алгоритм для скорости
        price_threshold = orders_df['price'].std() * 0.15  # Увеличен порог для скорости
        
        current_cluster = {
            'start_price': orders_df.iloc[0]['price'],
            'end_price': orders_df.iloc[0]['price'],
            'total_size': orders_df.iloc[0]['size']
        }
        
        for i in range(1, min(len(orders_df), 10)):  # Ограничиваем для скорости
            current_price = orders_df.iloc[i]['price']
            current_size = orders_df.iloc[i]['size']
            
            if abs(current_price - current_cluster['end_price']) <= price_threshold:
                current_cluster['end_price'] = current_price
                current_cluster['total_size'] += current_size
            else:
                if current_cluster['total_size'] > self.config.min_liquidity_threshold:
                    clusters.append(current_cluster)
                
                current_cluster = {
                    'start_price': current_price,
                    'end_price': current_price,
                    'total_size': current_size
                }
        
        if current_cluster['total_size'] > self.config.min_liquidity_threshold:
            clusters.append(current_cluster)
        
        return clusters
    
    def identify_fast_support_levels(self, bids_df: pd.DataFrame, bid_clusters: List[Dict]) -> List[Dict]:
        """Быстрое определение уровней поддержки"""
        support_levels = []
        
        for cluster in bid_clusters:
            if cluster['total_size'] > self.config.min_liquidity_threshold * 1.2:
                support_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'support'
                })
        
        return support_levels
    
    def identify_fast_resistance_levels(self, asks_df: pd.DataFrame, ask_clusters: List[Dict]) -> List[Dict]:
        """Быстрое определение уровней сопротивления"""
        resistance_levels = []
        
        for cluster in ask_clusters:
            if cluster['total_size'] > self.config.min_liquidity_threshold * 1.2:
                resistance_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'resistance'
                })
        
        return resistance_levels
    
    def analyze_price_movement(self, symbol: str, current_price: float) -> str:
        """Анализирует движение цены"""
        if symbol not in self.price_history:
            self.price_history[symbol] = []
        
        self.price_history[symbol].append(current_price)
        
        # Храним только последние 5 цен для скорости
        if len(self.price_history[symbol]) > 5:
            self.price_history[symbol].pop(0)
        
        if len(self.price_history[symbol]) < 2:
            return 'sideways'
        
        prices = self.price_history[symbol]
        price_change = (prices[-1] - prices[0]) / prices[0]
        
        if abs(price_change) < self.config.price_movement_threshold:
            return 'sideways'
        elif price_change > self.config.price_movement_threshold:
            return 'up'
        else:
            return 'down'

# ===== КЛАСС ДЛЯ ПРИНЯТИЯ ТОРГОВЫХ РЕШЕНИЙ =====
class RealtimeDOMTradingStrategy:
    """Класс для принятия торговых решений в реальном времени"""
    
    def __init__(self, config: RealtimeDOMConfig):
        self.config = config
        self.dom_analyzer = RealtimeDOMAnalyzer(config)
        self.time_analyzer = TradingTimeAnalyzer(config)
        
    def generate_realtime_signal(self, dom_analysis: Dict) -> Optional[Dict]:
        """Генерирует торговый сигнал в реальном времени"""
        try:
            if not dom_analysis:
                return None
            
            symbol = dom_analysis.get('symbol')
            bid_ask_ratio = dom_analysis.get('bid_ask_ratio', 0)
            support_levels = dom_analysis.get('support_levels', [])
            resistance_levels = dom_analysis.get('resistance_levels', [])
            current_price = dom_analysis.get('current_price', 0)
            price_movement = dom_analysis.get('price_movement', 'sideways')
            
            # Проверяем минимальную ликвидность
            if dom_analysis.get('bid_liquidity', 0) < self.config.min_liquidity_threshold or \
               dom_analysis.get('ask_liquidity', 0) < self.config.min_liquidity_threshold:
                return None
            
            # Проверяем активность торговых сессий
            if not self.time_analyzer.is_major_session_active():
                return None
            
            # LONG сигнал (упрощенный для реального времени)
            if bid_ask_ratio > self.config.min_bid_ask_ratio and price_movement in ['up', 'sideways']:
                # Проверяем близость к поддержке
                near_support = False
                if len(support_levels) > 0:
                    for support in support_levels:
                        price_diff = abs(current_price - support['price']) / current_price
                        if price_diff < 0.008:  # В пределах 0.8%
                            near_support = True
                            break
                
                # Если нет близкой поддержки, но bid доминирует сильно - торгуем
                if near_support or bid_ask_ratio > self.config.min_bid_ask_ratio * 1.3:
                    confidence = min(bid_ask_ratio / self.config.min_bid_ask_ratio, 2.5)
                    return {
                        'symbol': symbol,
                        'direction': 'Buy',
                        'confidence': confidence,
                        'entry_price': current_price,
                        'reason': f'Realtime bid dominance (ratio: {bid_ask_ratio:.2f})'
                    }
            
            # SHORT сигнал (упрощенный для реального времени)
            if bid_ask_ratio < (1 / self.config.min_bid_ask_ratio) and price_movement in ['down', 'sideways']:
                # Проверяем близость к сопротивлению
                near_resistance = False
                if len(resistance_levels) > 0:
                    for resistance in resistance_levels:
                        price_diff = abs(current_price - resistance['price']) / current_price
                        if price_diff < 0.008:  # В пределах 0.8%
                            near_resistance = True
                            break
                
                # Если нет близкого сопротивления, но ask доминирует сильно - торгуем
                if near_resistance or bid_ask_ratio < (1 / self.config.min_bid_ask_ratio) * 0.8:
                    confidence = min((1 / bid_ask_ratio) / self.config.min_bid_ask_ratio, 2.5)
                    return {
                        'symbol': symbol,
                        'direction': 'Sell',
                        'confidence': confidence,
                        'entry_price': current_price,
                        'reason': f'Realtime ask dominance (ratio: {bid_ask_ratio:.2f})'
                    }
            
            return None
            
        except Exception as e:
            logging.error(f"Ошибка генерации реального времени сигнала: {str(e)}")
            return None

# ===== КЛАСС РЕАЛЬНОГО ВРЕМЕНИ ТОРГОВОГО БОТА =====
class RealtimeDOMTradingBot:
    """Основной класс торгового бота в реальном времени"""
    
    def __init__(self, config: RealtimeDOMConfig):
        self.config = config
        self.session = HTTP(
            api_key=config.api_key,
            api_secret=config.api_secret,
            testnet=config.testnet
        )
        self.strategy = RealtimeDOMTradingStrategy(config)
        self.open_positions = []
        self.orderbook_queue = queue.Queue()
        self.running = False
        
        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("realtime_dom_trading_bot.log"),
                logging.StreamHandler()
            ]
        )
    
    def get_portfolio_balance(self) -> float:
        """Получает баланс портфеля"""
        try:
            response = self.session.get_wallet_balance(accountType="UNIFIED")
            if response.get('retCode') == 0:
                for wallet in response['result']['list']:
                    for coin in wallet.get('coin', []):
                        if coin.get('coin') == 'USDT':
                            return float(coin['walletBalance'])
            return 0
        except Exception as e:
            logging.error(f"Ошибка получения баланса: {str(e)}")
            return 0
    
    def calculate_position_size(self, balance: float, confidence: float) -> float:
        """Рассчитывает размер позиции"""
        base_size = balance * (self.config.risk_per_trade / 100)
        adjusted_size = base_size * min(confidence, 1.8)  # Ограничиваем для реального времени
        return adjusted_size
    
    def place_order(self, symbol: str, side: str, size: float, entry_price: float) -> bool:
        """Размещает ордер"""
        try:
            logging.info(f"[REALTIME] Размещение ордера: {side} {size:.4f} {symbol} по цене {entry_price}")
            
            # Рассчитываем TP/SL
            if side == 'Buy':
                take_profit = entry_price * (1 + self.config.take_profit_percent)
                stop_loss = entry_price * (1 - self.config.stop_loss_percent)
            else:
                take_profit = entry_price * (1 - self.config.take_profit_percent)
                stop_loss = entry_price * (1 + self.config.stop_loss_percent)
            
            # Размещаем основной ордер
            order_response = self.session.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=str(size),
                timeInForce="GTC"
            )
            
            if order_response.get('retCode') != 0:
                logging.error(f"Ошибка размещения ордера: {order_response.get('retMsg')}")
                return False
            
            logging.info(f"✅ Реальный ордер размещен: {order_response['result']}")
            
            # Сохраняем информацию о позиции
            position_info = {
                'symbol': symbol,
                'side': side,
                'size': size,
                'entry_price': entry_price,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'entry_time': datetime.now().isoformat()
            }
            
            self.open_positions.append(position_info)
            self.save_position_info(position_info)
            
            return True
            
        except Exception as e:
            logging.error(f"Ошибка размещения ордера для {symbol}: {str(e)}")
            return False
    
    def save_position_info(self, position_info: Dict):
        """Сохраняет информацию о позиции"""
        try:
            with open('realtime_dom_positions.json', 'w') as f:
                json.dump(position_info, f, indent=2)
        except Exception as e:
            logging.error(f"Ошибка сохранения информации о позиции: {str(e)}")
    
    def check_and_close_positions(self):
        """Проверяет и закрывает позиции по условиям"""
        try:
            # Получаем открытые позиции
            response = self.session.get_positions(category="linear")
            if response.get('retCode') != 0:
                return
            
            current_time = datetime.now(timezone.utc)
            
            for position in response['result']['list']:
                if float(position['size']) > 0:
                    symbol = position['symbol']
                    side = position['side']
                    size = float(position['size'])
                    entry_price = float(position['avgPrice'])
                    # createdTime приходит в миллисекундах epoch; приводим к UTC datetime
                    try:
                        created_ms = int(position.get('createdTime'))
                        entry_time = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
                    except Exception:
                        entry_time = current_time
                    
                    # Проверяем время удержания
                    hold_time = (current_time - entry_time).total_seconds()
                    if hold_time > self.config.max_hold_time:
                        # Закрываем позицию по времени
                        close_side = 'Sell' if side == 'Buy' else 'Buy'
                        close_response = self.session.place_order(
                            category="linear",
                            symbol=symbol,
                            side=close_side,
                            orderType="Market",
                            qty=str(size),
                            timeInForce="GTC"
                        )
                        
                        if close_response.get('retCode') == 0:
                            logging.info(f"Позиция {symbol} закрыта по времени")
                        else:
                            logging.error(f"Ошибка закрытия позиции {symbol}: {close_response.get('retMsg')}")
            
        except Exception as e:
            logging.error(f"Ошибка проверки позиций: {str(e)}")
    
    def websocket_orderbook_handler(self, message):
        """Обработчик WebSocket сообщений стакана заявок"""
        try:
            if 'topic' in message and 'orderbook' in message['topic']:
                self.orderbook_queue.put(message)
        except Exception as e:
            logging.error(f"Ошибка обработки WebSocket сообщения: {str(e)}")
    
    def process_orderbook_data(self):
        """Обрабатывает данные стакана заявок"""
        try:
            while not self.orderbook_queue.empty():
                message = self.orderbook_queue.get_nowait()
                
                # Извлекаем символ из топика
                topic = message.get('topic', '')
                if 'orderbook' in topic:
                    symbol = topic.split('.')[-1]  # Последняя часть топика - символ
                    
                    # Анализируем DOM
                    dom_analysis = self.strategy.dom_analyzer.analyze_realtime_dom(symbol, message)
                    
                    if dom_analysis:
                        # Генерируем сигнал
                        signal = self.strategy.generate_realtime_signal(dom_analysis)
                        
                        if signal:
                            logging.info(f"🚀 {signal['symbol']}: {signal['direction']} сигнал (уверенность: {signal['confidence']:.2f})")
                            logging.info(f"   Причина: {signal['reason']}")
                            logging.info(f"   Цена: {signal['entry_price']}")
                            
                            # Рассчитываем размер позиции
                            balance = self.get_portfolio_balance()
                            position_size = self.calculate_position_size(balance, signal['confidence'])
                            
                            # Размещаем ордер
                            if self.place_order(signal['symbol'], signal['direction'], position_size, signal['entry_price']):
                                logging.info(f"✅ Реальный ордер размещен для {signal['symbol']}")
                            else:
                                logging.error(f"❌ Ошибка размещения ордера для {signal['symbol']}")
                
        except Exception as e:
            logging.error(f"Ошибка обработки данных стакана: {str(e)}")
    
    def start_websocket_connections(self):
        """Запускает WebSocket соединения для всех пар"""
        try:
            # Создаем WebSocket клиент
            self.ws = WebSocket(
                testnet=self.config.testnet,
                channel_type="linear"
            )
            
            # Подписываемся на стакан заявок для всех пар
            for symbol in self.config.pairs:
                self.ws.orderbook_stream(
                    symbol=symbol,
                    depth=self.config.dom_levels,
                    callback=self.websocket_orderbook_handler
                )
                logging.info(f"📡 Подписка на стакан заявок: {symbol}")
            
            logging.info("🌐 WebSocket соединения установлены")
            
        except Exception as e:
            logging.error(f"Ошибка запуска WebSocket соединений: {str(e)}")
    
    def run(self):
        """Запускает торговый бот в реальном времени"""
        logging.info("🚀 Запуск реального времени DOM торгового бота...")
        
        # Получаем баланс
        balance = self.get_portfolio_balance()
        logging.info(f"💰 Баланс портфеля: {balance:.2f} USDT")
        
        logging.info(f"📊 Отслеживаем {len(self.config.pairs)} торговых пар в реальном времени")
        
        # Запускаем WebSocket соединения
        self.start_websocket_connections()
        
        self.running = True
        cycle_count = 0
        
        while self.running:
            try:
                cycle_count += 1
                
                # Обрабатываем данные стакана заявок
                self.process_orderbook_data()
                
                # Проверяем и закрываем старые позиции (устойчивый расчёт интервала)
                check_every = int(round(self.config.position_check_interval / self.config.dom_update_interval))
                if check_every > 0 and (cycle_count % check_every == 0):
                    self.check_and_close_positions()
                
                # Пауза между циклами
                time.sleep(self.config.dom_update_interval)
                
            except KeyboardInterrupt:
                logging.info("🛑 Бот остановлен пользователем")
                self.running = False
                break
            except Exception as e:
                logging.error(f"Критическая ошибка: {str(e)}")
                time.sleep(5)

# ===== ГЛАВНАЯ ФУНКЦИЯ =====
def main():
    """Главная функция торгового бота"""
    # Создаем конфигурацию
    config = RealtimeDOMConfig()
    
    # Создаем и запускаем бота
    bot = RealtimeDOMTradingBot(config)
    bot.run()

if __name__ == "__main__":
    main()