# -*- coding: utf-8 -*-
import time
import pandas as pd
import logging
import talib
from pybit.unified_trading import HTTP
import json
from datetime import datetime
import numpy as np
from typing import Dict, List, Tuple, Optional

# ===== НАСТРОЙКИ =====
API_KEY = "YMpyspOSx3m52Zx3va"
API_SECRET = "MGo33EXT6BiFhcc0gdxWATcKgttKOC5bAl5f"
    
# Параметры торговли
PAIRS = [
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

# Настройки DOM анализа
LEVERAGE = 2
MAX_OPEN_POSITIONS = 8
RISK_PER_TRADE = 1

# Параметры DOM анализа
DOM_LEVELS = 25  # Количество уровней для анализа
MIN_BID_ASK_RATIO = 1.5  # Минимальное соотношение bid/ask для сигнала
MIN_LIQUIDITY_THRESHOLD = 10000  # Минимальная ликвидность
DOM_UPDATE_INTERVAL = 2  # Интервал обновления DOM (секунды)
PRICE_MOVEMENT_THRESHOLD = 0.001  # Порог движения цены для подтверждения

# Параметры торговли
TAKE_PROFIT_PERCENT = 0.02  # 2% Take Profit
STOP_LOSS_PERCENT = 0.01    # 1% Stop Loss
MAX_HOLD_TIME = 300  # 5 минут максимальное время удержания

# Настройка логов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dom_trading_bot.log"),
        logging.StreamHandler()
    ]
)
    
# ===== ИНИЦИАЛИЗАЦИЯ =====
session = HTTP(
    api_key=API_KEY,
    api_secret=API_SECRET,
)

# ===== КЛАСС ДЛЯ АНАЛИЗА DOM =====
class DOMAnalyzer:
    """Класс для анализа Depth of Market"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.dom_history = []
        self.support_levels = []
        self.resistance_levels = []
        
    def get_orderbook(self) -> Optional[Dict]:
        """Получает данные стакана заявок"""
        try:
            response = session.get_orderbook(
                category="linear",
                symbol=self.symbol,
                limit=DOM_LEVELS
            )
            
            if response.get('retCode') != 0:
                logging.error(f"Ошибка получения DOM для {self.symbol}: {response.get('retMsg')}")
                return None
                
            return response['result']
            
        except Exception as e:
            logging.error(f"Ошибка получения DOM для {self.symbol}: {str(e)}")
            return None
    
    def analyze_dom_structure(self, orderbook: Dict) -> Dict:
        """Анализирует структуру DOM"""
        try:
            bids = orderbook.get('b', [])  # Заявки на покупку
            asks = orderbook.get('a', [])  # Заявки на продажу
            
            if not bids or not asks:
                return {}
            
            # Преобразуем в DataFrame для удобства анализа
            bids_df = pd.DataFrame(bids, columns=['price', 'size'])
            asks_df = pd.DataFrame(asks, columns=['price', 'size'])
            
            bids_df['price'] = bids_df['price'].astype(float)
            bids_df['size'] = bids_df['size'].astype(float)
            asks_df['price'] = asks_df['price'].astype(float)
            asks_df['size'] = asks_df['size'].astype(float)
            
            # Анализ скоплений заявок
            bid_clusters = self.find_order_clusters(bids_df, 'bid')
            ask_clusters = self.find_order_clusters(asks_df, 'ask')
            
            # Анализ ликвидности
            bid_liquidity = bids_df['size'].sum()
            ask_liquidity = asks_df['size'].sum()
            
            # Соотношение bid/ask
            bid_ask_ratio = bid_liquidity / ask_liquidity if ask_liquidity > 0 else 0
            
            # Определение уровней поддержки и сопротивления
            support_levels = self.identify_support_levels(bids_df, bid_clusters)
            resistance_levels = self.identify_resistance_levels(asks_df, ask_clusters)
            
            # Анализ спреда
            spread = float(asks_df.iloc[0]['price']) - float(bids_df.iloc[0]['price'])
            spread_percent = (spread / float(bids_df.iloc[0]['price'])) * 100
            
            return {
                'bid_liquidity': bid_liquidity,
                'ask_liquidity': ask_liquidity,
                'bid_ask_ratio': bid_ask_ratio,
                'spread': spread,
                'spread_percent': spread_percent,
                'support_levels': support_levels,
                'resistance_levels': resistance_levels,
                'bid_clusters': bid_clusters,
                'ask_clusters': ask_clusters,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logging.error(f"Ошибка анализа DOM для {self.symbol}: {str(e)}")
            return {}
    
    def find_order_clusters(self, orders_df: pd.DataFrame, order_type: str) -> List[Dict]:
        """Находит скопления заявок"""
        clusters = []
        
        if len(orders_df) < 2:
            return clusters
        
        # Группируем заявки по близким ценам
        price_threshold = orders_df['price'].std() * 0.1  # 10% от стандартного отклонения
        
        current_cluster = {
            'start_price': orders_df.iloc[0]['price'],
            'end_price': orders_df.iloc[0]['price'],
            'total_size': orders_df.iloc[0]['size'],
            'orders': [orders_df.iloc[0].to_dict()]
        }
        
        for i in range(1, len(orders_df)):
            current_price = orders_df.iloc[i]['price']
            current_size = orders_df.iloc[i]['size']
            
            # Проверяем, принадлежит ли заявка к текущему кластеру
            if abs(current_price - current_cluster['end_price']) <= price_threshold:
                current_cluster['end_price'] = current_price
                current_cluster['total_size'] += current_size
                current_cluster['orders'].append(orders_df.iloc[i].to_dict())
            else:
                # Сохраняем текущий кластер и начинаем новый
                if current_cluster['total_size'] > MIN_LIQUIDITY_THRESHOLD:
                    clusters.append(current_cluster)
                
                current_cluster = {
                    'start_price': current_price,
                    'end_price': current_price,
                    'total_size': current_size,
                    'orders': [orders_df.iloc[i].to_dict()]
                }
        
        # Добавляем последний кластер
        if current_cluster['total_size'] > MIN_LIQUIDITY_THRESHOLD:
            clusters.append(current_cluster)
        
        return clusters
    
    def identify_support_levels(self, bids_df: pd.DataFrame, bid_clusters: List[Dict]) -> List[Dict]:
        """Определяет уровни поддержки"""
        support_levels = []
        
        for cluster in bid_clusters:
            if cluster['total_size'] > MIN_LIQUIDITY_THRESHOLD * 2:  # Сильная поддержка
                support_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'strong_support'
                })
            elif cluster['total_size'] > MIN_LIQUIDITY_THRESHOLD:
                support_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'weak_support'
                })
        
        return support_levels
    
    def identify_resistance_levels(self, asks_df: pd.DataFrame, ask_clusters: List[Dict]) -> List[Dict]:
        """Определяет уровни сопротивления"""
        resistance_levels = []
        
        for cluster in ask_clusters:
            if cluster['total_size'] > MIN_LIQUIDITY_THRESHOLD * 2:  # Сильное сопротивление
                resistance_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'strong_resistance'
                })
            elif cluster['total_size'] > MIN_LIQUIDITY_THRESHOLD:
                resistance_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'weak_resistance'
                })
        
        return resistance_levels
    
    def detect_price_movement(self, current_price: float, previous_price: float) -> str:
        """Определяет направление движения цены"""
        price_change = (current_price - previous_price) / previous_price
        
        if abs(price_change) < PRICE_MOVEMENT_THRESHOLD:
            return 'sideways'
        elif price_change > PRICE_MOVEMENT_THRESHOLD:
            return 'up'
        else:
            return 'down'

# ===== КЛАСС ДЛЯ ПРИНЯТИЯ ТОРГОВЫХ РЕШЕНИЙ =====
class DOMTradingStrategy:
    """Класс для принятия торговых решений на основе DOM"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.dom_analyzer = DOMAnalyzer(symbol)
        self.previous_price = None
        self.dom_history = []
        
    def get_current_price(self) -> Optional[float]:
        """Получает текущую цену"""
        try:
            response = session.get_tickers(category="linear", symbol=self.symbol)
            if response.get('retCode') == 0:
                return float(response['result']['list'][0]['lastPrice'])
            return None
        except Exception as e:
            logging.error(f"Ошибка получения цены для {self.symbol}: {str(e)}")
            return None
    
    def analyze_trading_opportunity(self) -> Optional[Dict]:
        """Анализирует торговую возможность на основе DOM"""
        try:
            # Получаем данные DOM
            orderbook = self.dom_analyzer.get_orderbook()
            if not orderbook:
                return None
            
            # Анализируем структуру DOM
            dom_analysis = self.dom_analyzer.analyze_dom_structure(orderbook)
            if not dom_analysis:
                return None
            
            # Получаем текущую цену
            current_price = self.get_current_price()
            if not current_price:
                return None
            
            # Определяем движение цены
            price_movement = 'sideways'
            if self.previous_price:
                price_movement = self.dom_analyzer.detect_price_movement(current_price, self.previous_price)
            
            self.previous_price = current_price
            
            # Сохраняем историю DOM
            self.dom_history.append(dom_analysis)
            if len(self.dom_history) > 10:  # Храним последние 10 анализов
                self.dom_history.pop(0)
            
            # Анализируем торговые сигналы
            signal = self.generate_trading_signal(dom_analysis, current_price, price_movement)
            
            if signal:
                return {
                    'symbol': self.symbol,
                    'signal': signal['direction'],
                    'confidence': signal['confidence'],
                    'entry_price': current_price,
                    'dom_analysis': dom_analysis,
                    'price_movement': price_movement,
                    'timestamp': datetime.now()
                }
            
            return None
            
        except Exception as e:
            logging.error(f"Ошибка анализа торговой возможности для {self.symbol}: {str(e)}")
            return None
    
    def generate_trading_signal(self, dom_analysis: Dict, current_price: float, price_movement: str) -> Optional[Dict]:
        """Генерирует торговый сигнал на основе DOM"""
        try:
            bid_ask_ratio = dom_analysis.get('bid_ask_ratio', 0)
            support_levels = dom_analysis.get('support_levels', [])
            resistance_levels = dom_analysis.get('resistance_levels', [])
            
            # Проверяем минимальную ликвидность
            if dom_analysis.get('bid_liquidity', 0) < MIN_LIQUIDITY_THRESHOLD or \
               dom_analysis.get('ask_liquidity', 0) < MIN_LIQUIDITY_THRESHOLD:
                return None
            
            # LONG сигнал
            long_conditions = [
                bid_ask_ratio > MIN_BID_ASK_RATIO,  # Преобладание заявок на покупку
                len(support_levels) > 0,  # Есть уровни поддержки
                price_movement in ['up', 'sideways']  # Цена движется вверх или боком
            ]
            
            # Дополнительные условия для LONG
            if all(long_conditions):
                # Проверяем близость к сильной поддержке
                near_support = False
                for support in support_levels:
                    if support['type'] == 'strong_support':
                        price_diff = abs(current_price - support['price']) / current_price
                        if price_diff < 0.005:  # В пределах 0.5% от поддержки
                            near_support = True
                            break
                
                if near_support:
                    confidence = min(bid_ask_ratio / MIN_BID_ASK_RATIO, 3.0)  # Ограничиваем уверенность
                    return {
                        'direction': 'Buy',
                        'confidence': confidence,
                        'reason': 'Strong bid dominance near support level'
                    }
            
            # SHORT сигнал
            short_conditions = [
                bid_ask_ratio < (1 / MIN_BID_ASK_RATIO),  # Преобладание заявок на продажу
                len(resistance_levels) > 0,  # Есть уровни сопротивления
                price_movement in ['down', 'sideways']  # Цена движется вниз или боком
            ]
            
            # Дополнительные условия для SHORT
            if all(short_conditions):
                # Проверяем близость к сильному сопротивлению
                near_resistance = False
                for resistance in resistance_levels:
                    if resistance['type'] == 'strong_resistance':
                        price_diff = abs(current_price - resistance['price']) / current_price
                        if price_diff < 0.005:  # В пределах 0.5% от сопротивления
                            near_resistance = True
                            break
                
                if near_resistance:
                    confidence = min((1 / bid_ask_ratio) / MIN_BID_ASK_RATIO, 3.0)  # Ограничиваем уверенность
                    return {
                        'direction': 'Sell',
                        'confidence': confidence,
                        'reason': 'Strong ask dominance near resistance level'
                    }
            
            return None
            
        except Exception as e:
            logging.error(f"Ошибка генерации торгового сигнала для {self.symbol}: {str(e)}")
            return None

# ===== ФУНКЦИИ ТОРГОВЛИ =====
def get_portfolio_balance() -> float:
    """Получает баланс портфеля"""
    try:
        response = session.get_wallet_balance(accountType="UNIFIED")
        if response.get('retCode') == 0:
            for wallet in response['result']['list']:
                for coin in wallet.get('coin', []):
                    if coin.get('coin') == 'USDT':
                        return float(coin['walletBalance'])
        return 0
    except Exception as e:
        logging.error(f"Ошибка получения баланса: {str(e)}")
        return 0

def calculate_position_size(balance: float, confidence: float) -> float:
    """Рассчитывает размер позиции"""
    base_size = balance * (RISK_PER_TRADE / 100)
    # Увеличиваем размер позиции в зависимости от уверенности
    adjusted_size = base_size * min(confidence, 2.0)  # Максимум 2x от базового размера
    return adjusted_size

def place_dom_order(symbol: str, side: str, size: float, entry_price: float) -> bool:
    """Размещает ордер на основе DOM анализа"""
    try:
        logging.info(f"[DOM] Размещение ордера: {side} {size:.4f} {symbol} по цене {entry_price}")
        
        # Рассчитываем TP/SL
        if side == 'Buy':
            take_profit = entry_price * (1 + TAKE_PROFIT_PERCENT)
            stop_loss = entry_price * (1 - STOP_LOSS_PERCENT)
        else:
            take_profit = entry_price * (1 - TAKE_PROFIT_PERCENT)
            stop_loss = entry_price * (1 + STOP_LOSS_PERCENT)
        
        # Размещаем основной ордер
        order_response = session.place_order(
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
        
        logging.info(f"✅ Ордер размещен успешно: {order_response['result']}")
        
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
        
        # Сохраняем в файл
        save_position_info(position_info)
        
        return True
        
    except Exception as e:
        logging.error(f"Ошибка размещения ордера для {symbol}: {str(e)}")
        return False

def save_position_info(position_info: Dict):
    """Сохраняет информацию о позиции"""
    try:
        with open('dom_positions.json', 'w') as f:
            json.dump(position_info, f, indent=2)
    except Exception as e:
        logging.error(f"Ошибка сохранения информации о позиции: {str(e)}")

def check_and_close_positions():
    """Проверяет и закрывает позиции по условиям"""
    try:
        # Получаем открытые позиции
        response = session.get_positions(category="linear")
        if response.get('retCode') != 0:
            return
        
        current_time = datetime.now()
        
        for position in response['result']['list']:
            if float(position['size']) > 0:
                symbol = position['symbol']
                side = position['side']
                size = float(position['size'])
                entry_price = float(position['avgPrice'])
                entry_time = datetime.fromisoformat(position.get('createdTime', current_time.isoformat()))
                
                # Проверяем время удержания
                hold_time = (current_time - entry_time).total_seconds()
                if hold_time > MAX_HOLD_TIME:
                    # Закрываем позицию по времени
                    close_side = 'Sell' if side == 'Buy' else 'Buy'
                    close_response = session.place_order(
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

# ===== ГЛАВНАЯ ФУНКЦИЯ =====
def main():
    """Главная функция торгового бота"""
    logging.info("🚀 Запуск DOM торгового бота...")
    
    # Получаем баланс
    balance = get_portfolio_balance()
    logging.info(f"💰 Баланс портфеля: {balance:.2f} USDT")
    
    # Создаем стратегии для каждой пары
    strategies = {}
    for pair in PAIRS:
        strategies[pair] = DOMTradingStrategy(pair)
    
    logging.info(f"📊 Отслеживаем {len(PAIRS)} торговых пар")
    
    while True:
        try:
            # Проверяем и закрываем старые позиции
            check_and_close_positions()
            
            # Анализируем каждую пару
            for pair, strategy in strategies.items():
                try:
                    # Анализируем торговую возможность
                    opportunity = strategy.analyze_trading_opportunity()
                    
                    if opportunity:
                        signal = opportunity['signal']
                        confidence = opportunity['confidence']
                        entry_price = opportunity['entry_price']
                        
                        logging.info(f"📈 {pair}: {signal} сигнал (уверенность: {confidence:.2f})")
                        logging.info(f"   Причина: {opportunity.get('reason', 'N/A')}")
                        logging.info(f"   Цена: {entry_price}")
                        
                        # Рассчитываем размер позиции
                        position_size = calculate_position_size(balance, confidence)
                        
                        # Размещаем ордер
                        if place_dom_order(pair, signal, position_size, entry_price):
                            logging.info(f"✅ Ордер размещен для {pair}")
                        else:
                            logging.error(f"❌ Ошибка размещения ордера для {pair}")
                    
                except Exception as e:
                    logging.error(f"Ошибка анализа {pair}: {str(e)}")
                    continue
            
            # Пауза между циклами
            time.sleep(DOM_UPDATE_INTERVAL)
            
        except KeyboardInterrupt:
            logging.info("🛑 Бот остановлен пользователем")
            break
        except Exception as e:
            logging.error(f"Критическая ошибка: {str(e)}")
            time.sleep(10)

if __name__ == "__main__":
    main()