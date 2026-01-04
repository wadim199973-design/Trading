# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import logging
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
try:
    from pybit.unified_trading import WebSocket
except ImportError:
    from pybit import WebSocket
import warnings
warnings.filterwarnings('ignore')
import queue

# ===== НАСТРОЙКИ =====
@dataclass
class DOMBacktestConfig:
    """Конфигурация для DOM бектеста с прямой загрузкой данных"""
    # API ключи для Bybit
    api_key: str = "YMpyspOSx3m52Zx3va"
    api_secret: str = "MGo33EXT6BiFhcc0gdxWATcKgttKOC5bAl5f"
    testnet: bool = False
    
    # Параметры DOM анализа
    dom_levels: int = 25  # Количество уровней для анализа
    min_bid_ask_ratio: float = 1.2  # Минимальное соотношение bid/ask для сигнала
    min_liquidity_threshold: float = 10000  # Минимальная ликвидность
    price_movement_threshold: float = 0.001  # Порог движения цены для подтверждения
    
    # Параметры торговли
    take_profit_percent: float = 0.03  # 3% Take Profit
    stop_loss_percent: float = 0.01    # 1% Stop Loss
    max_hold_time: int = 300  # 5 минут максимальное время удержания
    leverage: int = 2  # Кредитное плечо
    risk_per_trade: float = 1.0  # Риск на сделку (%)
    max_open_positions: int = 10  # Максимум открытых позиций
    
    # Параметры данных (только realtime)
    pairs: List[str] = None  # Список пар для мониторинга в реальном времени
    
    # Режимы
    realtime_mode: bool = True  # Запуск через WebSocket в реальном времени (только realtime)
    report_dir: str = "reports"  # Директория для отчётов
    report_time_utc: str = "23:59"  # Время формирования отчёта UTC (HH:MM)
    channel_type: str = "linear"  # тип WS-канала: linear/spot/inverse
    ws_queue_maxsize: int = 1000   # максимальный размер очереди сообщений
    
    # Параметры API
    max_requests_per_minute: int = 20
    max_requests_per_hour: int = 1000
    request_delay: float = 3.0
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 5.0
    
    # Параметры симуляции DOM
    dom_simulation: bool = True  # Симуляция DOM на основе исторических данных
    dom_update_interval: int = 2  # Интервал обновления DOM (секунды)
    
    # Фильтры по времени торговли
    trade_only_major_sessions: bool = True  # Торговать только в основные сессии
    new_york_session: bool = True  # Нью-Йорк (14:30-21:00 UTC)
    london_session: bool = True    # Лондон (08:00-16:00 UTC)
    tokyo_session: bool = True     # Токио (00:00-09:00 UTC)
    
    # Дополнительные фильтры
    max_spread_percent: float = 0.5  # Максимальный спред для торговли (%)
    min_volume_threshold: float = 10000  # Минимальный объем для торговли (снижен в 10 раз)
    volatility_filter: bool = True  # Фильтр по волатильности
    max_volatility_percent: float = 10.0  # Максимальная волатильность (%)
    
    def __post_init__(self):
        if self.pairs is None:
            self.pairs = [
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

# ===== КЛАСС ДЛЯ АНАЛИЗА ВРЕМЕНИ ТОРГОВЛИ =====
class TradingTimeAnalyzer:
    """Класс для анализа времени торговли и определения активных сессий"""
    
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
        
    def is_major_session_active(self, timestamp: datetime) -> bool:
        """Проверяет, активна ли одна из основных торговых сессий"""
        if not self.config.trade_only_major_sessions:
            return True
            
        utc_time = timestamp.replace(tzinfo=timezone.utc)
        hour = utc_time.hour
        minute = utc_time.minute
        
        # Нью-Йорк: 14:30-21:00 UTC (9:30 AM - 4:00 PM EST)
        if self.config.new_york_session:
            if hour > 14 and hour < 21:
                return True
            elif hour == 14 and minute >= 30:
                return True
            elif hour == 21 and minute == 0:
                return True
            
        # Лондон: 08:00-16:00 UTC (9:00 AM - 5:00 PM GMT)
        if self.config.london_session and 8 <= hour < 16:
            return True
            
        # Токио: 00:00-09:00 UTC (9:00 AM - 6:00 PM JST)
        if self.config.tokyo_session and (0 <= hour < 9):
            return True
            
        return False
    
    def get_session_info(self, timestamp: datetime) -> Dict:
        """Возвращает информацию о текущей торговой сессии"""
        utc_time = timestamp.replace(tzinfo=timezone.utc)
        hour = utc_time.hour
        minute = utc_time.minute
        
        sessions = []
        
        # Нью-Йорк: 14:30-21:00 UTC
        if self.config.new_york_session and ((hour > 14 and hour < 21) or (hour == 14 and minute >= 30) or (hour == 21 and minute == 0)):
            sessions.append("New York")
        
        # Лондон: 08:00-16:00 UTC
        if self.config.london_session and (8 <= hour < 16):
            sessions.append("London")
        
        # Токио: 00:00-09:00 UTC
        if self.config.tokyo_session and (0 <= hour < 9):
            sessions.append("Tokyo")
        
        return {
            'active_sessions': sessions,
            'is_major_session': len(sessions) > 0,
            'utc_hour': hour,
            'timestamp': timestamp
        }
    
    def calculate_session_liquidity_multiplier(self, timestamp: datetime) -> float:
        """Рассчитывает множитель ликвидности на основе активных сессий"""
        session_info = self.get_session_info(timestamp)
        
        if not session_info['is_major_session']:
            return 0.3  # Низкая ликвидность вне основных сессий
        
        # Высокая ликвидность в перекрывающихся сессиях
        if len(session_info['active_sessions']) >= 2:
            return 2.0
        elif len(session_info['active_sessions']) == 1:
            return 1.5
            
        return 1.0

# Удалён исторический режим и генераторы симулированного DOM; остаётся только realtime через WebSocket

# ===== КЛАСС ДЛЯ АНАЛИЗА DOM =====
class DOMAnalyzer:
    """Класс для анализа Depth of Market"""
    
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
        
    def analyze_dom_structure(self, orderbook: Dict) -> Dict:
        """Анализирует структуру DOM"""
        try:
            bids = orderbook.get('b', [])
            asks = orderbook.get('a', [])
            
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
            
            # Поиск кластеров
            bid_clusters = self.find_order_clusters(bids_df, 'bid')
            ask_clusters = self.find_order_clusters(asks_df, 'ask')
            
            # Определение уровней
            support_levels = self.identify_support_levels(bids_df, bid_clusters)
            resistance_levels = self.identify_resistance_levels(asks_df, ask_clusters)
            
            return {
                'bid_liquidity': bid_liquidity,
                'ask_liquidity': ask_liquidity,
                'bid_ask_ratio': bid_ask_ratio,
                'spread': spread,
                'spread_percent': spread_percent,
                'support_levels': support_levels,
                'resistance_levels': resistance_levels,
                'bid_clusters': bid_clusters,
                'ask_clusters': ask_clusters
            }
            
        except Exception as e:
            logging.error(f"Ошибка анализа DOM: {str(e)}")
            return {}
    
    def find_order_clusters(self, orders_df: pd.DataFrame, order_type: str) -> List[Dict]:
        """Находит скопления заявок"""
        clusters = []
        
        if len(orders_df) < 2:
            return clusters
        
        # Группируем заявки по близким ценам
        price_threshold = orders_df['price'].std() * 0.1
        
        current_cluster = {
            'start_price': orders_df.iloc[0]['price'],
            'end_price': orders_df.iloc[0]['price'],
            'total_size': orders_df.iloc[0]['size'],
            'orders': [orders_df.iloc[0].to_dict()]
        }
        
        for i in range(1, len(orders_df)):
            current_price = orders_df.iloc[i]['price']
            current_size = orders_df.iloc[i]['size']
            
            if abs(current_price - current_cluster['end_price']) <= price_threshold:
                current_cluster['end_price'] = current_price
                current_cluster['total_size'] += current_size
                current_cluster['orders'].append(orders_df.iloc[i].to_dict())
            else:
                if current_cluster['total_size'] > self.config.min_liquidity_threshold:
                    clusters.append(current_cluster)
                
                current_cluster = {
                    'start_price': current_price,
                    'end_price': current_price,
                    'total_size': current_size,
                    'orders': [orders_df.iloc[i].to_dict()]
                }
        
        if current_cluster['total_size'] > self.config.min_liquidity_threshold:
            clusters.append(current_cluster)
        
        return clusters
    
    def identify_support_levels(self, bids_df: pd.DataFrame, bid_clusters: List[Dict]) -> List[Dict]:
        """Определяет уровни поддержки"""
        support_levels = []
        
        for cluster in bid_clusters:
            if cluster['total_size'] > self.config.min_liquidity_threshold * 1.5:  # Снижено с 2 до 1.5
                support_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'strong_support'
                })
            elif cluster['total_size'] > self.config.min_liquidity_threshold * 0.5:  # Добавлен слабый уровень
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
            if cluster['total_size'] > self.config.min_liquidity_threshold * 1.5:  # Снижено с 2 до 1.5
                resistance_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'strong_resistance'
                })
            elif cluster['total_size'] > self.config.min_liquidity_threshold * 0.5:  # Добавлен слабый уровень
                resistance_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'weak_resistance'
                })
        
        return resistance_levels

# ===== КЛАСС ДЛЯ ТОРГОВОЙ СТРАТЕГИИ =====
class DOMTradingStrategy:
    """Класс для принятия торговых решений на основе DOM"""
    
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
        self.dom_analyzer = DOMAnalyzer(config)
        self.time_analyzer = TradingTimeAnalyzer(config)
        self.previous_price = None
        
    def detect_price_movement(self, current_price: float, previous_price: float) -> str:
        """Определяет направление движения цены"""
        if not previous_price:
            return 'sideways'
        
        price_change = (current_price - previous_price) / previous_price
        
        if abs(price_change) < self.config.price_movement_threshold:
            return 'sideways'
        elif price_change > self.config.price_movement_threshold:
            return 'up'
        else:
            return 'down'
    
    
    def generate_trading_signal(self, dom_analysis: Dict, current_price: float, 
                              price_movement: str, current_time: datetime,
                              current_volume: float) -> Optional[Dict]:
        """Генерирует торговый сигнал на основе DOM"""
        try:
            
            bid_ask_ratio = dom_analysis.get('bid_ask_ratio', 0)
            support_levels = dom_analysis.get('support_levels', [])
            resistance_levels = dom_analysis.get('resistance_levels', [])
            
            # Проверяем минимальную ликвидность
            if dom_analysis.get('bid_liquidity', 0) < self.config.min_liquidity_threshold or \
               dom_analysis.get('ask_liquidity', 0) < self.config.min_liquidity_threshold:
                return None
            
            # LONG сигнал (упрощенный)
            long_conditions = [
                bid_ask_ratio > self.config.min_bid_ask_ratio,  # bid доминирует
                price_movement in ['up', 'sideways']
            ]
            
            if all(long_conditions):
                # Проверяем близость к любой поддержке (не только сильной)
                near_support = False
                if len(support_levels) > 0:
                    for support in support_levels:
                        price_diff = abs(current_price - support['price']) / current_price
                        if price_diff < 0.01:  # В пределах 1% от поддержки (увеличено)
                            near_support = True
                            break
                
                # Если нет близкой поддержки, но bid доминирует сильно - все равно торгуем
                if near_support or bid_ask_ratio > self.config.min_bid_ask_ratio * 1.5:
                    confidence = min(bid_ask_ratio / self.config.min_bid_ask_ratio, 3.0)
                    return {
                        'direction': 'Buy',
                        'confidence': confidence,
                        'reason': f'Bid dominance (ratio: {bid_ask_ratio:.2f})'
                    }
            
            # SHORT сигнал (упрощенный)
            short_conditions = [
                bid_ask_ratio < (1 / self.config.min_bid_ask_ratio),  # ask доминирует (bid < ask)
                price_movement in ['down', 'sideways']
            ]
            
            if all(short_conditions):
                # Проверяем близость к любому сопротивлению (не только сильному)
                near_resistance = False
                if len(resistance_levels) > 0:
                    for resistance in resistance_levels:
                        price_diff = abs(current_price - resistance['price']) / current_price
                        if price_diff < 0.01:  # В пределах 1% от сопротивления (увеличено)
                            near_resistance = True
                            break
                
                # Если нет близкого сопротивления, но ask доминирует сильно - все равно торгуем
                if near_resistance or bid_ask_ratio < (1 / self.config.min_bid_ask_ratio) * 0.7:
                    confidence = min((1 / bid_ask_ratio) / self.config.min_bid_ask_ratio, 3.0)
                    return {
                        'direction': 'Sell',
                        'confidence': confidence,
                        'reason': f'Ask dominance (ratio: {bid_ask_ratio:.2f})'
                    }
            
            return None
            
        except Exception as e:
            logging.error(f"Ошибка генерации торгового сигнала: {str(e)}")
            return None

# ===== КЛАСС ДЛЯ УПРАВЛЕНИЯ ЛИМИТАМИ API =====
class APIRateLimiter:
    """Заглушка (исторические REST-запросы удалены)."""
    def __init__(self, *args, **kwargs):
        pass

# ===== КЛАСС ДЛЯ ЗАГРУЗКИ ДАННЫХ =====
class DataFetcher:
    """Заглушка для совместимости (исторические данные удалены)."""
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
    def get_available_pairs(self) -> List[str]:
        return list(self.config.pairs or [])

# ===== КЛАСС ДЛЯ БЕКТЕСТИНГА =====
class DOMBacktester:
    """Класс для бектестинга DOM стратегии"""
    
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
        self.data_fetcher = DataFetcher(config)
        self.strategy = DOMTradingStrategy(config)
        
        # Результаты бектеста
        self.trades = []
        self.equity_curve = []
        self.initial_balance = 1000  # Начальный баланс
        self.current_balance = self.initial_balance
        self.open_positions = []
        
    def run_backtest(self) -> Dict:
        """Запускает бектест"""
        logging.info(" Запуск DOM бектеста...")
        logging.info(" Режим: реальное время (WebSocket)")
        return self._run_realtime()

    # ===== Реальный режим через WebSocket =====
    def _run_realtime(self) -> Dict:
        """Запуск в реальном времени: подписка на стаканы, генерация сигналов и дневные отчёты"""
        self.ws = WebSocket(testnet=self.config.testnet, channel_type=self.config.channel_type)
        self.orderbook_queue = queue.Queue(maxsize=self.config.ws_queue_maxsize)
        self.trades = []
        self.open_positions = []
        self.equity_curve = []
        self.current_balance = self.initial_balance
        self._last_report_date = None
        self._report_time_minutes = self._parse_report_time(self.config.report_time_utc)

        # Подписки
        for symbol in self.data_fetcher.get_available_pairs():
            try:
                self.ws.orderbook_stream(
                    symbol=symbol,
                    depth=self.config.dom_levels,
                    callback=lambda msg, s=symbol: self._on_orderbook_msg(s, msg)
                )
                logging.info(f"📡 WS подписка: {symbol}")
            except Exception as e:
                logging.error(f"❌ Не удалось подписаться на {symbol}: {e}")

        logging.info("🌐 WS запущен. Начинаем цикл обработки...")
        try:
            while True:
                self._process_orderbook_queue()
                self._maybe_close_positions_by_time()
                self._maybe_write_daily_report()
                # обновляем equity curve раз в цикл
                self._update_equity_curve(datetime.now(timezone.utc))
                time.sleep(0.5)
        except KeyboardInterrupt:
            logging.info("Остановка по Ctrl+C")
        except Exception as e:
            logging.error(f"Критическая ошибка realtime: {e}")
        finally:
            # Финальный дневной отчёт
            try:
                self._write_daily_report(datetime.now(timezone.utc).date())
            except Exception:
                pass
            return self._analyze_results()

    def _on_orderbook_msg(self, symbol: str, message: Dict):
        try:
            if 'topic' in message and 'orderbook' in message['topic']:
                try:
                    self.orderbook_queue.put_nowait((symbol, message))
                except queue.Full:
                    # сбрасываем самое старое сообщение, чтобы не копить лаг
                    try:
                        _ = self.orderbook_queue.get_nowait()
                    except Exception:
                        pass
                    try:
                        self.orderbook_queue.put_nowait((symbol, message))
                    except Exception:
                        pass
        except Exception as e:
            logging.error(f"Ошибка WS сообщения: {e}")

    def _process_orderbook_queue(self):
        while not self.orderbook_queue.empty():
            symbol, message = self.orderbook_queue.get_nowait()
            dom_analysis = self.strategy.dom_analyzer.analyze_dom_structure(message.get('data', {}))
            if not dom_analysis:
                continue
            # Текущее время и цена
            now = datetime.now(timezone.utc)
            current_price = None
            try:
                data = message.get('data', {})
                bids = data.get('b', [])
                asks = data.get('a', [])
                if bids and asks:
                    bb = float(bids[0][0] if isinstance(bids[0], (list, tuple)) else bids[0]['price'])
                    ba = float(asks[0][0] if isinstance(asks[0], (list, tuple)) else asks[0]['price'])
                    best_bid = bb
                    best_ask = ba
                    current_price = (best_bid + best_ask) / 2
            except Exception:
                pass
            if current_price is None:
                continue

            # фильтр торговых сессий
            if not self.strategy.time_analyzer.is_major_session_active(now):
                continue

            # простой трекер: детект по предыдущей цене
            if not hasattr(self, '_last_price'):
                self._last_price = {}
            prev_price = self._last_price.get(symbol)
            price_movement = self.strategy.detect_price_movement(current_price, prev_price) if prev_price else 'sideways'
            self._last_price[symbol] = current_price

            # контроль спреда
            try:
                spread_pct = abs(best_ask - best_bid) / current_price * 100
                if spread_pct > self.config.max_spread_percent:
                    continue
            except Exception:
                pass

            signal = self.strategy.generate_trading_signal(dom_analysis, current_price, price_movement, now, 0)
            if signal and len(self.open_positions) < self.config.max_open_positions:
                self._execute_trade(symbol, signal, current_price, now, dom_analysis)

    def _maybe_close_positions_by_time(self):
        now = datetime.now(timezone.utc)
        for i in reversed(range(len(self.open_positions))):
            pos = self.open_positions[i]
            hold = (now - pos['entry_time']).total_seconds()
            if hold > self.config.max_hold_time:
                self._close_position(i, pos['entry_price'], now)

    def _parse_report_time(self, hhmm: str) -> int:
        try:
            hh, mm = hhmm.split(':')
            return int(hh) * 60 + int(mm)
        except Exception:
            return 23 * 60 + 59

    def _maybe_write_daily_report(self):
        now = datetime.now(timezone.utc)
        today_minutes = now.hour * 60 + now.minute
        if self._last_report_date == now.date():
            return
        if today_minutes >= self._report_time_minutes:
            self._write_daily_report(now.date())
            self._last_report_date = now.date()

    def _write_daily_report(self, report_date):
        try:
            os.makedirs(self.config.report_dir, exist_ok=True)
            day_trades = [t for t in self.trades if datetime.fromisoformat(str(t['exit_time'])).date() == report_date or datetime.fromisoformat(str(t['entry_time'])).date() == report_date]
            by_symbol: Dict[str, Dict] = {}
            for t in day_trades:
                s = t['symbol']
                d = by_symbol.setdefault(s, {'trades': 0, 'pnl': 0.0, 'wins': 0})
                d['trades'] += 1
                d['pnl'] += t['pnl']
                if t['pnl'] > 0:
                    d['wins'] += 1
            report = {
                'date': str(report_date),
                'summary': self._analyze_results().get('summary', {}),
                'by_symbol': by_symbol,
            }
            path = os.path.join(self.config.report_dir, f"report_{report_date}.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            logging.info(f"📄 Отчёт за {report_date} сохранён: {path}")
        except Exception as e:
            logging.error(f"Ошибка записи отчёта: {e}")
        
    # Удалён исторический цикл _backtest_pair; realtime обрабатывается в WS-цикле
    
    def _execute_trade(self, symbol: str, signal: Dict, price: float, 
                      timestamp: datetime, dom_analysis: Dict):
        """Выполняет торговую операцию"""
        try:
            # Рассчитываем размер позиции
            position_size = self._calculate_position_size(signal['confidence'])
            
            # Рассчитываем TP/SL
            if signal['direction'] == 'Buy':
                take_profit = price * (1 + self.config.take_profit_percent)
                stop_loss = price * (1 - self.config.stop_loss_percent)
            else:
                take_profit = price * (1 - self.config.take_profit_percent)
                stop_loss = price * (1 + self.config.stop_loss_percent)
            
            # Создаем позицию
            position = {
                'symbol': symbol,
                'direction': signal['direction'],
                'entry_price': price,
                'size': position_size,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'entry_time': timestamp,
                'confidence': signal['confidence'],
                'reason': signal['reason']
            }
            
            self.open_positions.append(position)
            
            logging.info(f" Открыта позиция: {signal['direction']} {symbol} по {price:.6f}")
            
        except Exception as e:
            logging.error(f"Ошибка выполнения сделки: {str(e)}")
    
    def _update_positions(self, symbol: str, current_price: float, current_time: datetime):
        """Обновляет открытые позиции"""
        positions_to_close = []
        
        for i, position in enumerate(self.open_positions):
            if position['symbol'] != symbol:
                continue
            
            # Проверяем TP/SL
            if position['direction'] == 'Buy':
                if current_price >= position['take_profit'] or current_price <= position['stop_loss']:
                    positions_to_close.append(i)
            else:  # Sell
                if current_price <= position['take_profit'] or current_price >= position['stop_loss']:
                    positions_to_close.append(i)
            
            # Проверяем время удержания
            hold_time = (current_time - position['entry_time']).total_seconds()
            if hold_time > self.config.max_hold_time:
                positions_to_close.append(i)
        
        # Закрываем позиции
        for i in reversed(positions_to_close):
            self._close_position(i, current_price, current_time)
    
    def _close_position(self, position_index: int, exit_price: float, exit_time: datetime):
        """Закрывает позицию"""
        try:
            position = self.open_positions[position_index]
            
            # Рассчитываем P&L
            if position['direction'] == 'Buy':
                pnl = (exit_price - position['entry_price']) / position['entry_price']
            else:
                pnl = (position['entry_price'] - exit_price) / position['entry_price']
            
            pnl_amount = pnl * position['size'] * self.config.leverage
            
            # Обновляем баланс
            self.current_balance += pnl_amount
            
            # Создаем запись о сделке
            trade = {
                'symbol': position['symbol'],
                'direction': position['direction'],
                'entry_price': position['entry_price'],
                'exit_price': exit_price,
                'entry_time': position['entry_time'],
                'exit_time': exit_time,
                'size': position['size'],
                'pnl': pnl_amount,
                'pnl_percent': pnl * 100,
                'confidence': position['confidence'],
                'reason': position['reason'],
                'hold_time': (exit_time - position['entry_time']).total_seconds()
            }
            
            self.trades.append(trade)
            
            # Удаляем позицию
            del self.open_positions[position_index]
            
            logging.info(f" Закрыта позиция: {position['direction']} {position['symbol']} "
                        f"P&L: {pnl_amount:.2f} ({pnl*100:.2f}%)")
            
        except Exception as e:
            logging.error(f"Ошибка закрытия позиции: {str(e)}")
    
    def _close_all_positions(self, symbol: str, exit_price: float, exit_time: datetime):
        """Закрывает все позиции по символу"""
        positions_to_close = [i for i, pos in enumerate(self.open_positions) 
                            if pos['symbol'] == symbol]
        
        for i in reversed(positions_to_close):
            self._close_position(i, exit_price, exit_time)
    
    def _calculate_position_size(self, confidence: float) -> float:
        """Рассчитывает размер позиции"""
        base_size = self.current_balance * (self.config.risk_per_trade / 100)
        adjusted_size = base_size * min(confidence, 2.0)
        return adjusted_size
    
    def _update_equity_curve(self, timestamp: datetime):
        """Обновляет кривую доходности"""
        self.equity_curve.append({
            'timestamp': timestamp,
            'balance': self.current_balance,
            'open_positions': len(self.open_positions)
        })
    
    def _analyze_results(self) -> Dict:
        """Анализирует результаты бектеста"""
        if not self.trades:
            return {'error': 'Нет сделок для анализа'}
        
        # Базовые метрики
        total_trades = len(self.trades)
        winning_trades = len([t for t in self.trades if t['pnl'] > 0])
        losing_trades = len([t for t in self.trades if t['pnl'] < 0])
        
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        # P&L метрики
        total_pnl = sum(t['pnl'] for t in self.trades)
        total_pnl_percent = (total_pnl / self.initial_balance) * 100
        
        avg_win = np.mean([t['pnl'] for t in self.trades if t['pnl'] > 0]) if winning_trades > 0 else 0
        avg_loss = np.mean([t['pnl'] for t in self.trades if t['pnl'] < 0]) if losing_trades > 0 else 0
        
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
        
        # Максимальная просадка
        equity_values = [e['balance'] for e in self.equity_curve]
        max_drawdown = self._calculate_max_drawdown(equity_values)
        
        # Время удержания
        avg_hold_time = np.mean([t['hold_time'] for t in self.trades])
        
        # Анализ по символам
        symbol_analysis = {}
        for trade in self.trades:
            symbol = trade['symbol']
            if symbol not in symbol_analysis:
                symbol_analysis[symbol] = {'trades': 0, 'pnl': 0, 'wins': 0}
            
            symbol_analysis[symbol]['trades'] += 1
            symbol_analysis[symbol]['pnl'] += trade['pnl']
            if trade['pnl'] > 0:
                symbol_analysis[symbol]['wins'] += 1
        
        return {
            'summary': {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'total_pnl': total_pnl,
                'total_pnl_percent': total_pnl_percent,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'max_drawdown': max_drawdown,
                'avg_hold_time': avg_hold_time,
                'initial_balance': self.initial_balance,
                'final_balance': self.current_balance
            },
            'symbol_analysis': symbol_analysis,
            'trades': self.trades,
            'equity_curve': self.equity_curve
        }
    
    def _calculate_max_drawdown(self, equity_values: List[float]) -> float:
        """Рассчитывает максимальную просадку"""
        if not equity_values:
            return 0
        
        peak = equity_values[0]
        max_dd = 0
        
        for value in equity_values:
            if value > peak:
                peak = value
            dd = (peak - value) / peak
            if dd > max_dd:
                max_dd = dd
        
        return max_dd * 100

# ===== ГЛАВНАЯ ФУНКЦИЯ =====
def main():
    """Главная функция бектеста"""
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("dom_backtest_direct.log"),
            logging.StreamHandler()
        ]
    )
    
    # Создаем конфигурацию (только realtime)
    config = DOMBacktestConfig(
        pairs=[
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
        ],
        min_bid_ask_ratio=1.5,
        min_liquidity_threshold=10000,
        take_profit_percent=0.03,
        stop_loss_percent=0.01,
        max_hold_time=300,
        risk_per_trade=1.0,
        # Торговые окна
        trade_only_major_sessions=True,
        new_york_session=True,
        london_session=True,
        tokyo_session=True,
        max_spread_percent=0.5,
        min_volume_threshold=10000,
        volatility_filter=True,
        max_volatility_percent=10.0
    )
    
    # Создаем и запускаем бектестер
    backtester = DOMBacktester(config)
    results = backtester.run_backtest()
    
    # Выводим результаты
    if 'error' in results:
        logging.error(f"Ошибка бектеста: {results['error']}")
        return
    
    summary = results['summary']
    
    print("\n" + "="*60)
    print(" РЕЗУЛЬТАТЫ DOM БЕКТЕСТА (ПРЯМАЯ ЗАГРУЗКА)")
    print("="*60)
    print(f" Начальный баланс: ${summary['initial_balance']:,.2f}")
    print(f" Конечный баланс: ${summary['final_balance']:,.2f}")
    print(f" Общий P&L: ${summary['total_pnl']:,.2f} ({summary['total_pnl_percent']:.2f}%)")
    print(f" Всего сделок: {summary['total_trades']}")
    print(f" Прибыльных: {summary['winning_trades']}")
    print(f" Убыточных: {summary['losing_trades']}")
    print(f" Винрейт: {summary['win_rate']:.2f}%")
    print(f" Средний выигрыш: ${summary['avg_win']:,.2f}")
    print(f" Средний проигрыш: ${summary['avg_loss']:,.2f}")
    print(f"  Profit Factor: {summary['profit_factor']:.2f}")
    print(f" Максимальная просадка: {summary['max_drawdown']:.2f}%")
    print(f"⏱  Среднее время удержания: {summary['avg_hold_time']:.0f} сек")
    print("="*60)
    
    # Анализ по символам
    print("\n АНАЛИЗ ПО СИМВОЛАМ:")
    print("-"*40)
    for symbol, analysis in results['symbol_analysis'].items():
        win_rate = (analysis['wins'] / analysis['trades']) * 100
        print(f"{symbol}: {analysis['trades']} сделок, "
              f"P&L: ${analysis['pnl']:,.2f}, "
              f"Винрейт: {win_rate:.1f}%")
    
    # Сохраняем результаты
    with open('dom_backtest_direct_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n Результаты сохранены в dom_backtest_direct_results.json")

if __name__ == "__main__":
    main()
