# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import logging
import json
import os
import time
import requests
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from binance import ThreadedWebsocketManager

import queue
import threading
from collections import deque, defaultdict

# Исправление кодировки для Windows
if sys.platform == 'win32':
    try:
        import codecs
        if hasattr(sys.stdout, 'buffer'):
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'replace')
        if hasattr(sys.stderr, 'buffer'):
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'replace')
    except:
        pass  # Если не получилось - продолжаем без исправления
    
    # Исправление для asyncio на Windows (aiodns проблема)
    try:
        import asyncio
        if asyncio.get_event_loop_policy().__class__.__name__ != 'WindowsSelectorEventLoopPolicy':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except:
        pass

# ===== ФУНКЦИИ ДЛЯ ПОЛУЧЕНИЯ СПИСКА ПАР =====
def get_all_binance_pairs(min_volume_usdt: float = 100000.0, 
                          only_usdt: bool = True,
                          exclude_pairs: List[str] = None) -> List[str]:
    """
    Получает список всех торговых пар с Binance
    
    Параметры:
    - min_volume_usdt: минимальный объём за 24ч в USDT
    - only_usdt: только USDT пары
    - exclude_pairs: список пар для исключения
    """
    try:
        logging.info("🌐 Получение списка всех пар с Binance...")
        
        # Получаем данные 24h ticker для всех пар
        url = "https://api.binance.com/api/v3/ticker/24hr"
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            logging.error(f"❌ Ошибка получения данных: {response.status_code}")
            return []
        
        tickers = response.json()
        
        # Фильтруем пары
        filtered_pairs = []
        exclude_list = exclude_pairs if exclude_pairs else []
        
        for ticker in tickers:
            symbol = ticker['symbol']
            
            # Только USDT пары
            if only_usdt and not symbol.endswith('USDT'):
                continue
            
            # Исключаем пары из списка
            if symbol in exclude_list:
                continue
            
            # Фильтруем по объёму
            try:
                volume_usdt = float(ticker.get('quoteVolume', 0))
                if volume_usdt < min_volume_usdt:
                    continue
            except:
                continue
            
            filtered_pairs.append(symbol)
        
        # Сортируем по объёму (от большего к меньшему)
        pairs_with_volume = []
        for ticker in tickers:
            if ticker['symbol'] in filtered_pairs:
                try:
                    volume = float(ticker.get('quoteVolume', 0))
                    pairs_with_volume.append((ticker['symbol'], volume))
                except:
                    pass
        
        pairs_with_volume.sort(key=lambda x: x[1], reverse=True)
        sorted_pairs = [p[0] for p in pairs_with_volume]
        
        logging.info(f"✅ Найдено {len(sorted_pairs)} пар с объёмом ≥ ${min_volume_usdt:,.0f}")
        logging.info(f"📊 Топ-10 пар по объёму:")
        for i, (symbol, volume) in enumerate(pairs_with_volume[:10], 1):
            logging.info(f"   {i}. {symbol}: ${volume:,.0f}")
        
        return sorted_pairs
        
    except Exception as e:
        logging.error(f"❌ Ошибка получения списка пар: {e}")
        return []

# ===== НАСТРОЙКИ =====
@dataclass
class DOMBacktestConfig:
    """Конфигурация для DOM бектеста с Binance"""
    # API ключи для Binance (заполните свои ключи)
    api_key: str = "XTbZuOgp8wJb1v8oSm7rPM1JCs8mxCkb1N3gp0q38XL8TTv9Y3nPelhcjidUT3kZ"
    api_secret: str = "9oKf8Ztm1n0EplXyTr8IN6JmFiypjqvU3Jd0TJ4YNeG44vmlDjn60RwFdo66mllX"
    testnet: bool = False  # True для testnet, False для mainnet
    
    # Параметры DOM анализа
    dom_levels: int = 15  # Количество уровней для анализа
    min_bid_ask_ratio: float = 1.8 # Минимальное соотношение bid/ask 
    min_liquidity_threshold: float = 5000.0  # Минимальная ликвидность в USDT 
    price_movement_threshold: float = 0.001  # Порог движения цены для подтверждения
    
    # Параметры торговли
    take_profit_percent: float = 0.03  # 3% Take Profit - ИСПРАВЛЕНО!
    stop_loss_percent: float = 0.01    # 1% Stop Loss
    max_hold_time: int = 300  # 5 минут максимальное время удержания
    leverage: int = 5  # Кредитное плечо (для фьючерсов)
    risk_per_trade: float = 1.0  # Риск на сделку (%)
    max_open_positions: int = 10  # Максимум открытых позиций
    commission_rate: float = 0.001  # Комиссия биржи на сделку (0.1% в одну сторону)
    
    # Параметры данных
    pairs: List[str] = None  # Список пар для мониторинга
    use_all_pairs: bool = True  # Использовать ВСЕ пары с Binance
    min_24h_volume_usdt: float = 1000000.0  # Минимальный объём за 24ч ($1M USD)
    only_usdt_pairs: bool = True  # Только USDT пары
    exclude_pairs: List[str] = None  # Пары для исключения
    
    # Режимы
    realtime_mode: bool = True  # Этот флаг не используется, можно игнорировать
    replay_mode: bool = False  # Режим воспроизведения из JSON файлов
    record_mode: bool = True  # ← ОБЯЗАТЕЛЬНО True для торговли!
    trade_and_record: bool = True  # Торговать И записывать одновременно
    data_dir: str = "data"  # Директория для JSON данных
    report_dir: str = "reports"  # Директория для отчётов
    report_time_utc: str = "22:00"  # Время формирования отчёта UTC (HH:MM)
    ws_queue_maxsize: int = 1000   # максимальный размер очереди сообщений
    record_duration: int = 86400  # Длительность записи в секундах (86400 = 24 часа)
    
    # Фильтры по времени торговли
    trade_only_major_sessions: bool = False  # Торговать только в основные сессии
    new_york_session: bool = True  # Нью-Йорк (14:30-21:00 UTC)
    london_session: bool = True    # Лондон (08:00-16:00 UTC)
    tokyo_session: bool = True     # Токио (00:00-09:00 UTC)
    
    # Дополнительные фильтры
    max_spread_percent: float = 0.25  # Максимальный спред для торговли (%) — ужесточено
    min_volume_threshold: float = 1000  # Минимальный объем для торговли
    volatility_filter: bool = True  # Фильтр по волатильности (отключен)
    max_volatility_percent: float = 10.0  # Максимальная волатильность (%)
    confirm_volume_multiplier: float = 2  # Множитель для подтверждения объёмом
    sr_distance_percent: float = 0.1  # Процентная дистанция до уровня S/R для подтверждения
    # Устойчивость сигнала
    persistence_ticks: int = 3  # минимальное число последовательных тиков в сторону сигнала
    min_delta_liquidity_usdt: float = 20000.0  # минимальная суммарная дельта ликвидности за N тиков
    # Минимальная ликвидность в топ-уровнях стакана (сумма top-2 по обеим сторонам)
    min_top_levels_liquidity_usdt: float = 50000.0
    # Адаптивные пороги от волатильности
    adaptive_thresholds: bool = True
    vol_ewma_alpha: float = 0.2  # скорость сглаживания EWMA волатильности
    vol_low_percent: float = 1.0  # нижняя граница низкой волатильности (%)
    vol_high_percent: float = 5.0  # верхняя граница высокой волатильности (%)
    ratio_high_multiplier: float = 1.5  # ужесточить порог ratio при высокой волатильности
    ratio_low_multiplier: float = 0.85  # смягчить порог ratio при низкой волатильности
    liq_high_multiplier: float = 1.2  # повысить требование к ликвидности при высокой волатильности
    liq_low_multiplier: float = 0.8   # снизить при низкой
    significant_imbalance_ratio: float = 2.5  # Требуемый сильный перевес для входа
    volume_spike_multiplier: float = 5.0  # Множитель для подтверждения объема
    tick_event_window_seconds: int = 3  # окно анализа событий стакана
    tick_event_threshold: int = 12  # минимальное число событий за окно
    trade_session_filter_enabled: bool = True  # фильтр часовых окон
    allowed_trading_windows: Optional[List[Tuple[str, str]]] = field(default_factory=lambda: [("07:00", "09:00"), ("14:00", "16:00")])
    restricted_trading_windows: Optional[List[Tuple[str, str]]] = field(default_factory=lambda: [("21:00", "07:00")])
    spoof_track_levels: int = 5  # число отслеживаемых уровней стакана
    spoof_min_notional_usdt: float = 50000.0  # минимальный объем для подозрения спуферства
    spoof_cancel_seconds: int = 3  # окно отмены подозрительного ордера
    spoof_cooldown_seconds: int = 30  # время блокировки торговли после спуфа
    atr_period: int = 30  # период ATR по мид-прайсам
    atr_stop_multiplier: float = 1.5  # множитель ATR для стоп-лосса
    atr_trailing_multiplier: float = 1.0  # множитель ATR для трейлинга
    min_stop_loss_percent: float = 0.002  # минимальный процент стопа (0.2%)
    risk_reward_ratio: float = 2.0  # базовое соотношение риск/прибыль
    min_rr_ratio: float = 1.5  # минимальное допустимое соотношение
    # Трейлинг-стоп
    trailing_stop_enabled: bool = True
    trailing_activation_percent: float = 0.01  # активация трейлинга после +1% движения
    trailing_percent: float = 0.005  # расстояние трейлинга 0.5%
    # Динамическое время удержания
    dynamic_hold_enabled: bool = True
    hold_time_trend_boost: float = 1.5   # во сколько раз дольше держать при сильном тренде
    hold_time_high_vol_cut: float = 0.5  # во сколько раз короче держать при высокой волатильности
    hold_time_low_vol_boost: float = 1.2 # немного дольше держать при низкой волатильности
    strong_trend_confidence: float = 1.3 # порог уверенности для признания тренда сильным
    
    # Отладка
    debug_signals: bool = False  # Детальное логирование сигналов (замедляет работу)
    
    def __post_init__(self):
        if self.use_all_pairs:
            # Получаем ВСЕ пары с Binance
            logging.info("🌐 Режим: ВСЕ ПАРЫ BINANCE")
            self.pairs = get_all_binance_pairs(
                min_volume_usdt=self.min_24h_volume_usdt,
                only_usdt=self.only_usdt_pairs,
                exclude_pairs=self.exclude_pairs
            )
            if not self.pairs:
                logging.warning("⚠️ Не удалось получить список пар, используем дефолтный")
                self.pairs = self._get_default_pairs()
        elif self.pairs is None:
            # Используем стандартный список
            self.pairs = self._get_default_pairs()
    
    def _get_default_pairs(self) -> List[str]:
        """Возвращает стандартный список пар"""
        return [
                "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
                "DOGEUSDT", "ADAUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT",
                "DOTUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
                "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
                "SUIUSDT", "WLDUSDT", "THETAUSDT", "FILUSDT", "ICPUSDT"
            ]

# ===== КЛАСС ДЛЯ АНАЛИЗА ВРЕМЕНИ ТОРГОВЛИ =====
class TradingTimeAnalyzer:
    """Класс для анализа времени торговли и определения активных сессий"""
    
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
        self.allowed_windows = self._prepare_windows(self.config.allowed_trading_windows or [])
        self.restricted_windows = self._prepare_windows(self.config.restricted_trading_windows or [])
        
    def is_major_session_active(self, timestamp: datetime) -> bool:
        """Проверяет, активна ли одна из основных торговых сессий"""
        if not self.config.trade_only_major_sessions:
            return True
            
        utc_time = timestamp.replace(tzinfo=timezone.utc)
        hour = utc_time.hour
        minute = utc_time.minute
        
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

    def is_allowed_time(self, timestamp: datetime) -> bool:
        """Проверяет попадание во временные окна торговли"""
        if not self.config.trade_session_filter_enabled:
            return True
        
        utc_time = timestamp.replace(tzinfo=timezone.utc)
        minutes = utc_time.hour * 60 + utc_time.minute
        
        if self.allowed_windows:
            if not any(self._in_window(minutes, start, end) for start, end in self.allowed_windows):
                return False
        
        if self.restricted_windows:
            if any(self._in_window(minutes, start, end) for start, end in self.restricted_windows):
                return False
        
        return True

    def _prepare_windows(self, windows: List[Tuple[str, str]]) -> List[Tuple[int, int]]:
        parsed = []
        for start, end in windows:
            try:
                s_minutes = self._hhmm_to_minutes(start)
                e_minutes = self._hhmm_to_minutes(end)
                parsed.append((s_minutes, e_minutes))
            except Exception:
                continue
        return parsed

    def _hhmm_to_minutes(self, hhmm: str) -> int:
        hh, mm = hhmm.split(':')
        return int(hh) * 60 + int(mm)

    def _in_window(self, minutes: int, start: int, end: int) -> bool:
        if start <= end:
            return start <= minutes < end
        return minutes >= start or minutes < end

# ===== КЛАСС ДЛЯ АНАЛИЗА DOM =====
class DOMAnalyzer:
    """Класс для анализа Depth of Market"""
    
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
        
    def analyze_dom_structure(self, orderbook: Dict) -> Dict:
        """Анализирует структуру DOM для Binance"""
        try:
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])
            
            if not bids or not asks:
                return {}
            
            # Преобразуем в DataFrame
            bids_df = pd.DataFrame(bids, columns=['price', 'size'])
            asks_df = pd.DataFrame(asks, columns=['price', 'size'])
            
            bids_df['price'] = bids_df['price'].astype(float)
            bids_df['size'] = bids_df['size'].astype(float)
            asks_df['price'] = asks_df['price'].astype(float)
            asks_df['size'] = asks_df['size'].astype(float)
            
            # Анализ ликвидности В USDT (цена × размер)
            bids_df['value_usdt'] = bids_df['price'] * bids_df['size']
            asks_df['value_usdt'] = asks_df['price'] * asks_df['size']
            
            bid_liquidity = bids_df['value_usdt'].sum()  # В USDT
            ask_liquidity = asks_df['value_usdt'].sum()  # В USDT
            bid_ask_ratio = bid_liquidity / ask_liquidity if ask_liquidity > 1e-10 else 0
            
            # Анализ спреда
            best_bid_price = float(bids_df.iloc[0]['price'])
            best_bid_size = float(bids_df.iloc[0]['size'])
            best_ask_price = float(asks_df.iloc[0]['price'])
            best_ask_size = float(asks_df.iloc[0]['size'])
            spread = best_ask_price - best_bid_price
            spread_percent = (spread / best_bid_price) * 100 if best_bid_price else 0.0
            mid_price = (best_bid_price + best_ask_price) / 2 if (best_bid_price and best_ask_price) else None

            # Ликвидность в топ-уровнях (top-2) по каждой стороне
            bid_top2_usdt = bids_df.head(2)['value_usdt'].sum() if len(bids_df) > 0 else 0.0
            ask_top2_usdt = asks_df.head(2)['value_usdt'].sum() if len(asks_df) > 0 else 0.0
            
            # Поиск кластеров
            bid_clusters = self.find_order_clusters(bids_df, 'bid')
            ask_clusters = self.find_order_clusters(asks_df, 'ask')
            
            # Определение уровней
            support_levels = self.identify_support_levels(bids_df, bid_clusters)
            resistance_levels = self.identify_resistance_levels(asks_df, ask_clusters)
            bid_levels = bids_df[['price', 'size', 'value_usdt']].head(self.config.dom_levels).to_dict('records')
            ask_levels = asks_df[['price', 'size', 'value_usdt']].head(self.config.dom_levels).to_dict('records')
            
            return {
                'bid_liquidity': bid_liquidity,
                'ask_liquidity': ask_liquidity,
                'bid_ask_ratio': bid_ask_ratio,
                'spread': spread,
                'spread_percent': spread_percent,
                'bid_top2_usdt': bid_top2_usdt,
                'ask_top2_usdt': ask_top2_usdt,
                'best_bid_price': best_bid_price,
                'best_bid_size': best_bid_size,
                'best_ask_price': best_ask_price,
                'best_ask_size': best_ask_size,
                'mid_price': mid_price,
                'support_levels': support_levels,
                'resistance_levels': resistance_levels,
                'bid_clusters': bid_clusters,
                'ask_clusters': ask_clusters,
                'bid_levels': bid_levels,
                'ask_levels': ask_levels
            }
            
        except Exception as e:
            logging.error(f"Ошибка анализа DOM: {str(e)}")
            return {}
    
    def find_order_clusters(self, orders_df: pd.DataFrame, order_type: str) -> List[Dict]:
        """Находит скопления заявок"""
        clusters = []
        
        if len(orders_df) < 2:
            return clusters
        
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
            if cluster['total_size'] > self.config.min_liquidity_threshold * 1.5:
                support_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'strong_support'
                })
            elif cluster['total_size'] > self.config.min_liquidity_threshold * 0.5:
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
            if cluster['total_size'] > self.config.min_liquidity_threshold * 1.5:
                resistance_levels.append({
                    'price': cluster['start_price'],
                    'strength': cluster['total_size'],
                    'type': 'strong_resistance'
                })
            elif cluster['total_size'] > self.config.min_liquidity_threshold * 0.5:
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
        self._avg_volume: Dict[str, float] = {}  # EWMA ликвидности по символам
        self._ewma_vol_percent = None  # EWMA волатильности в процентах
        # Истории для устойчивости сигнала
        self._hist_ratio: Dict[str, deque] = {}
        self._hist_bid_liq: Dict[str, deque] = {}
        self._hist_ask_liq: Dict[str, deque] = {}
        # Волатильность и события
        self._mid_price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.config.atr_period + 1))
        self._tr_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.config.atr_period))
        self._tick_events: Dict[str, deque] = defaultdict(deque)
        # EWMA ликвидности и анти-спуфинг
        self._spoof_tracker: Dict[str, Dict[str, Dict[str, Dict]]] = defaultdict(lambda: {'bid': {}, 'ask': {}})
        self._spoof_block_until: Dict[str, datetime] = {}
        
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

    def is_time_allowed(self, timestamp: datetime) -> bool:
        """Комбинированная проверка временных фильтров"""
        return self.time_analyzer.is_major_session_active(timestamp) and \
            self.time_analyzer.is_allowed_time(timestamp)

    def update_symbol_state(self, symbol: str, dom_analysis: Dict, mid_price: float, current_time: datetime):
        """Обновляет состояние символа (волатильность, ликвидность, анти-спуфинг)"""
        if mid_price:
            self._update_volatility(symbol, mid_price)
        self._update_liquidity(symbol, dom_analysis)
        self._register_tick_event(symbol, current_time)
        self._evaluate_spoofing(symbol, dom_analysis, current_time)

    def is_symbol_blocked(self, symbol: str, current_time: datetime) -> bool:
        blocked_until = self._spoof_block_until.get(symbol)
        if not blocked_until:
            return False
        if current_time >= blocked_until:
            self._spoof_block_until.pop(symbol, None)
            return False
        return True

    def get_tick_intensity(self, symbol: str, current_time: datetime) -> int:
        window = self.config.tick_event_window_seconds
        events = self._tick_events.get(symbol)
        if not events:
            return 0
        return sum(1 for ts in events if (current_time - ts).total_seconds() <= window)

    def get_atr_value(self, symbol: str) -> float:
        tr_values = self._tr_history.get(symbol)
        if not tr_values:
            return 0.0
        if len(tr_values) == 0:
            return 0.0
        return float(np.mean(tr_values))

    def get_liquidity_ewma(self, symbol: str) -> float:
        return self._avg_volume.get(symbol, 0.0)

    def get_dynamic_trailing_percent(self, symbol: str, price: float) -> float:
        atr = self.get_atr_value(symbol)
        if price <= 0:
            return self.config.trailing_percent
        dynamic = (atr / price) * self.config.atr_trailing_multiplier if atr else 0.0
        return max(self.config.trailing_percent, dynamic)

    def _update_volatility(self, symbol: str, price: float):
        history = self._mid_price_history[symbol]
        if history:
            prev_price = history[-1]
            if prev_price:
                tr = abs(price - prev_price)
                tr_deque = self._tr_history[symbol]
                tr_deque.append(tr)
        history.append(price)

    def _update_liquidity(self, symbol: str, dom_analysis: Dict):
        liquidity = (dom_analysis.get('bid_top2_usdt', 0.0) +
                     dom_analysis.get('ask_top2_usdt', 0.0))
        if liquidity <= 0:
            return
        prev = self._avg_volume.get(symbol)
        if prev is None:
            self._avg_volume[symbol] = liquidity
        else:
            self._avg_volume[symbol] = 0.9 * prev + 0.1 * liquidity

    def _register_tick_event(self, symbol: str, current_time: datetime):
        events = self._tick_events[symbol]
        events.append(current_time)
        window = self.config.tick_event_window_seconds
        while events and (current_time - events[0]).total_seconds() > window:
            events.popleft()

    def _evaluate_spoofing(self, symbol: str, dom_analysis: Dict, timestamp: datetime):
        threshold = self.config.spoof_min_notional_usdt
        if threshold <= 0:
            return
        tracker = self._spoof_tracker[symbol]
        suspicious = False
        for side_key, levels_key in [('bid', 'bid_levels'), ('ask', 'ask_levels')]:
            levels = dom_analysis.get(levels_key, [])[:self.config.spoof_track_levels]
            side_store = tracker[side_key]
            current_keys = set()
            for lvl in levels:
                price = float(lvl.get('price', 0.0))
                size = float(lvl.get('size', 0.0))
                notional = float(lvl.get('value_usdt', price * size))
                key = f"{price:.8f}"
                current_keys.add(key)
                if notional >= threshold:
                    entry = side_store.get(key)
                    if entry is None:
                        side_store[key] = {
                            'notional': notional,
                            'timestamp': timestamp
                        }
                    else:
                        entry['notional'] = notional
                        entry['last_seen'] = timestamp
                else:
                    if key in side_store:
                        side_store[key]['notional'] = notional
                        side_store[key]['last_seen'] = timestamp
            to_remove = []
            for key, entry in list(side_store.items()):
                last_seen = entry.get('last_seen', entry['timestamp'])
                notional = entry.get('notional', 0.0)
                still_present = key in current_keys and notional >= threshold
                elapsed = (timestamp - entry['timestamp']).total_seconds()
                if not still_present:
                    if entry.get('notional', 0.0) >= threshold and elapsed <= self.config.spoof_cancel_seconds:
                        suspicious = True
                    to_remove.append(key)
                elif elapsed > self.config.spoof_cancel_seconds * 3:
                    to_remove.append(key)
            for key in to_remove:
                side_store.pop(key, None)
        if suspicious:
            self._spoof_block_until[symbol] = timestamp + timedelta(seconds=self.config.spoof_cooldown_seconds)
    
    def generate_trading_signal(self, symbol: str, dom_analysis: Dict, current_price: float, 
                              price_movement: str, current_time: datetime,
                              current_volume: float) -> Optional[Dict]:
        """Генерирует торговый сигнал на основе DOM"""
        try:
            bid_ask_ratio = dom_analysis.get('bid_ask_ratio', 0)
            support_levels = dom_analysis.get('support_levels', [])
            resistance_levels = dom_analysis.get('resistance_levels', [])
            bid_liquidity = dom_analysis.get('bid_liquidity', 0)
            ask_liquidity = dom_analysis.get('ask_liquidity', 0)
            spread_percent = dom_analysis.get('spread_percent')
            bid_top2_usdt = dom_analysis.get('bid_top2_usdt', 0.0)
            ask_top2_usdt = dom_analysis.get('ask_top2_usdt', 0.0)
            total_top_liq = float(current_volume) if current_volume else (bid_top2_usdt + ask_top2_usdt)
            liq_ewma = self.get_liquidity_ewma(symbol)
            tick_intensity = self.get_tick_intensity(symbol, current_time)
            has_tick_spike = tick_intensity >= self.config.tick_event_threshold

            # Обновляем EWMA волатильности на основе абсолютной доходности
            if self.previous_price and current_price:
                ret = abs((current_price - self.previous_price) / self.previous_price) * 100.0
                if self._ewma_vol_percent is None:
                    self._ewma_vol_percent = ret
                else:
                    a = self.config.vol_ewma_alpha
                    self._ewma_vol_percent = a * ret + (1 - a) * self._ewma_vol_percent
            self.previous_price = current_price

            # Рассчитываем адаптивные пороги
            adaptive_liq = self.config.min_liquidity_threshold
            adaptive_ratio = self.config.min_bid_ask_ratio
            if self.config.adaptive_thresholds and self._ewma_vol_percent is not None:
                v = self._ewma_vol_percent
                if v > self.config.vol_high_percent:
                    adaptive_ratio = self.config.min_bid_ask_ratio * self.config.ratio_high_multiplier
                    adaptive_liq = self.config.min_liquidity_threshold * self.config.liq_high_multiplier
                elif v < self.config.vol_low_percent:
                    adaptive_ratio = self.config.min_bid_ask_ratio * self.config.ratio_low_multiplier
                    adaptive_liq = self.config.min_liquidity_threshold * self.config.liq_low_multiplier
            if adaptive_ratio <= 0:
                adaptive_ratio = self.config.min_bid_ask_ratio if self.config.min_bid_ask_ratio > 0 else 1.0
            long_ratio_threshold = max(adaptive_ratio, self.config.significant_imbalance_ratio)
            inv_ratio_threshold = 1 / long_ratio_threshold if long_ratio_threshold > 0 else 0.0
            
            # Счетчик для отладки
            if not hasattr(self, '_total_checks'):
                self._total_checks = 0
                self._failed_liquidity = 0
                self._failed_long = 0
                self._failed_short = 0
            
            self._total_checks += 1
            
            # Отладочное логирование каждые 100 проверок
            if self._total_checks % 100 == 0:
                print(f"\n🔍 АНАЛИЗ СИГНАЛОВ (проверено {self._total_checks}):")
                print(f"   📊 Bid/Ask ratio: {bid_ask_ratio:.3f} (порог: {self.config.min_bid_ask_ratio})")
                print(f"   💰 Bid liquidity: ${bid_liquidity:,.0f} USDT")
                print(f"   💰 Ask liquidity: ${ask_liquidity:,.0f} USDT")
                print(f"   📏 Порог ликв:    ${self.config.min_liquidity_threshold:,.0f} USDT")
                print(f"   📈 Движение:      {price_movement}")
                print(f"   ❌ Фильтров ликв: {self._failed_liquidity}")
                print(f"   ❌ Не прошли LONG: {self._failed_long}")
                print(f"   ❌ Не прошли SHORT: {self._failed_short}\n")
            
            # ПРОВЕРКА: Минимальная ликвидность (в USDT)
            if bid_liquidity < adaptive_liq or ask_liquidity < adaptive_liq:
                self._failed_liquidity += 1
                return None
            
            # ПРОВЕРКА: Минимальная ликвидность в топ-уровнях стакана
            if bid_top2_usdt < self.config.min_top_levels_liquidity_usdt or \
               ask_top2_usdt < self.config.min_top_levels_liquidity_usdt:
                return None
            
            # Обновляем скользящее среднее ликвидности (EWMA)
            if total_top_liq > 0:
                prev_liq = self._avg_volume.get(symbol)
                if prev_liq is None:
                    self._avg_volume[symbol] = total_top_liq
                else:
                    self._avg_volume[symbol] = 0.9 * prev_liq + 0.1 * total_top_liq

            # Подтверждения: тренд/объём/время/SR
            is_good_time = self.is_time_allowed(current_time)
            has_volume_confirmation = False
            if liq_ewma and total_top_liq:
                has_volume_confirmation = total_top_liq >= liq_ewma * self.config.volume_spike_multiplier

            # Близость к поддержке/сопротивлению
            def _near_level(levels: List[Dict]) -> bool:
                if not levels:
                    return False
                for lvl in levels:
                    try:
                        if abs(current_price - float(lvl.get('price', current_price))) / max(current_price, 1e-9) * 100 <= self.config.sr_distance_percent:
                            return True
                    except Exception:
                        continue
                return False

            near_support = _near_level(support_levels)
            near_resistance = _near_level(resistance_levels)

            # Доп. фильтр по спреду, если есть
            if spread_percent is not None and spread_percent > self.config.max_spread_percent:
                return None

            # УСТОЙЧИВОСТЬ СИГНАЛА: обновим истории и посчитаем условия
            n = max(1, int(self.config.persistence_ticks))
            if symbol not in self._hist_ratio:
                self._hist_ratio[symbol] = deque(maxlen=n)
                self._hist_bid_liq[symbol] = deque(maxlen=n)
                self._hist_ask_liq[symbol] = deque(maxlen=n)
            self._hist_ratio[symbol].append(bid_ask_ratio)
            self._hist_bid_liq[symbol].append(bid_liquidity)
            self._hist_ask_liq[symbol].append(ask_liquidity)

            def _persistence_ok_long() -> bool:
                if len(self._hist_ratio[symbol]) < n:
                    return False
                if not all(r > long_ratio_threshold for r in self._hist_ratio[symbol]):
                    return False
                # суммарная дельта ликвидности за N тиков в сторону лонга
                delta = sum(b - a for b, a in zip(self._hist_bid_liq[symbol], self._hist_ask_liq[symbol]))
                return delta >= self.config.min_delta_liquidity_usdt

            def _persistence_ok_short() -> bool:
                if len(self._hist_ratio[symbol]) < n:
                    return False
                if inv_ratio_threshold <= 0 or not all(r < inv_ratio_threshold for r in self._hist_ratio[symbol]):
                    return False
                delta = sum(a - b for b, a in zip(self._hist_bid_liq[symbol], self._hist_ask_liq[symbol]))
                return delta >= self.config.min_delta_liquidity_usdt

            confirmations_required = 3
            # LONG
            if bid_ask_ratio > long_ratio_threshold and _persistence_ok_long():
                if not (has_volume_confirmation and has_tick_spike):
                    self._failed_long += 1
                else:
                    confirmations = 0
                    if price_movement in ['up', 'sideways']:
                        confirmations += 1
                    if has_volume_confirmation:
                        confirmations += 1
                    if is_good_time:
                        confirmations += 1
                    if near_support:
                        confirmations += 1
                    if has_tick_spike:
                        confirmations += 1

                    if confirmations >= confirmations_required:
                        base_ratio = long_ratio_threshold if long_ratio_threshold else adaptive_ratio
                        confidence = min(bid_ask_ratio / base_ratio, 3.0)
                        if near_support:
                            confidence = min(confidence * 1.1, 3.0)
                        if has_volume_confirmation:
                            confidence = min(confidence * 1.05, 3.0)
                        if has_tick_spike:
                            confidence = min(confidence * 1.05, 3.0)
                        return {
                            'direction': 'Buy',
                            'confidence': confidence,
                            'reason': (f'Bid dominance (ratio: {bid_ask_ratio:.2f} > {long_ratio_threshold:.2f}); '
                                       f'conf:{confirmations}; ticks:{tick_intensity}; '
                                       f'volat:{(self._ewma_vol_percent or 0):.2f}%@liq>={adaptive_liq:,.0f}')
                        }
                    else:
                        self._failed_long += 1

            # SHORT
            if inv_ratio_threshold > 0 and bid_ask_ratio < inv_ratio_threshold and _persistence_ok_short():
                if not (has_volume_confirmation and has_tick_spike):
                    self._failed_short += 1
                else:
                    confirmations = 0
                    if price_movement in ['down', 'sideways']:
                        confirmations += 1
                    if has_volume_confirmation:
                        confirmations += 1
                    if is_good_time:
                        confirmations += 1
                    if near_resistance:
                        confirmations += 1
                    if has_tick_spike:
                        confirmations += 1

                    if confirmations >= confirmations_required:
                        confidence = min((1 / bid_ask_ratio) / long_ratio_threshold, 3.0) if bid_ask_ratio > 0 else 3.0
                        if near_resistance:
                            confidence = min(confidence * 1.1, 3.0)
                        if has_volume_confirmation:
                            confidence = min(confidence * 1.05, 3.0)
                        if has_tick_spike:
                            confidence = min(confidence * 1.05, 3.0)
                        return {
                            'direction': 'Sell',
                            'confidence': confidence,
                            'reason': (f'Ask dominance (ratio: {bid_ask_ratio:.2f} < {inv_ratio_threshold:.2f}); '
                                       f'conf:{confirmations}; ticks:{tick_intensity}; '
                                       f'volat:{(self._ewma_vol_percent or 0):.2f}%@liq>={adaptive_liq:,.0f}')
                        }
                    else:
                        self._failed_short += 1
            
            return None
            
        except Exception as e:
            logging.error(f"Ошибка генерации торгового сигнала: {str(e)}")
            return None

# ===== КЛАСС ДЛЯ БЕКТЕСТИНГА =====
class DOMBacktester:
    """Класс для бектестинга DOM стратегии на Binance"""
    
    def __init__(self, config: DOMBacktestConfig):
        self.config = config
        self.strategy = DOMTradingStrategy(config)
        
        # Результаты бектеста
        self.trades = []
        self.equity_curve = []
        self.initial_balance = 1000
        self.current_balance = self.initial_balance
        self.open_positions = []
        
        # WebSocket клиент
        self.ws_client = None
        self.orderbook_queue = queue.Queue(maxsize=self.config.ws_queue_maxsize)
        self._last_price = {}
        self._last_report_date = None
        
        # Для записи данных
        self.recorded_data = []
        self.record_start_time = None
        
    def run_backtest(self) -> Dict:
        """Запускает бектест"""
        # Красивый заголовок
        print("\n" + "╔" + "═"*78 + "╗")
        print("║" + " "*20 + "🚀 DOM БЕКТЕСТ BINANCE 🚀" + " "*33 + "║")
        print("╠" + "═"*78 + "╣")
        print(f"║ 📊 Мониторинг пар:      {len(self.config.pairs)} шт." + " "*(52-len(str(len(self.config.pairs)))) + "║")
        print(f"║ 💰 Начальный баланс:    ${self.initial_balance:,.2f}" + " "*(47-len(f"{self.initial_balance:,.2f}")) + "║")
        print(f"║ 🎯 Take Profit:          {self.config.take_profit_percent*100:.1f}%" + " "*(52-len(f"{self.config.take_profit_percent*100:.1f}")) + "║")
        print(f"║ 🛑 Stop Loss:            {self.config.stop_loss_percent*100:.1f}%" + " "*(52-len(f"{self.config.stop_loss_percent*100:.1f}")) + "║")
        print(f"║ 📈 Макс. позиций:       {self.config.max_open_positions}" + " "*(53-len(str(self.config.max_open_positions))) + "║")
        
        if self.config.replay_mode:
            print("║ 🔵 Режим:                ВОСПРОИЗВЕДЕНИЕ (Replay)" + " "*28 + "║")
        elif self.config.record_mode:
            if self.config.trade_and_record:
                print("║ 🔴 Режим:                ЗАПИСЬ + ТОРГОВЛЯ" + " "*35 + "║")
            else:
                print("║ 🔴 Режим:                ЗАПИСЬ ДАННЫХ" + " "*39 + "║")
            print(f"║ ⏱️  Длительность:        {self.config.record_duration/60:.0f} минут ({self.config.record_duration/3600:.1f} часов)" + " "*(29-len(f"{self.config.record_duration/60:.0f}")) + "║")
        else:
            print("║ 🌐 Режим:                РЕАЛЬНОЕ ВРЕМЯ (Realtime)" + " "*27 + "║")
        
        print("╚" + "═"*78 + "╝\n")
        
        logging.info("🚀 Запуск DOM бектеста на Binance...")
        logging.info(f"📊 Мониторинг пар: {len(self.config.pairs)} шт.")
        
        if self.config.replay_mode:
            logging.info("📂 Режим: воспроизведение из JSON файлов")
            return self._run_replay()
        elif self.config.record_mode:
            logging.info("🔴 Режим: запись данных в JSON")
            return self._run_record()
        else:
            logging.info("🌐 Режим: реальное время (WebSocket)")
            return self._run_realtime()

    def _run_realtime(self) -> Dict:
        """Запуск в реальном времени через Binance WebSocket"""
        self._report_time_minutes = self._parse_report_time(self.config.report_time_utc)
        
        # Создаём WebSocket клиент с API ключами
        self.ws_client = ThreadedWebsocketManager(
            api_key=self.config.api_key if self.config.api_key else None,
            api_secret=self.config.api_secret if self.config.api_secret else None
        )
        self.ws_client.start()
        
        logging.info("🌐 Запуск WebSocket соединений...")
        time.sleep(2)  # Даём время на инициализацию
        
        # Подписываемся на orderbook для каждой пары
        for symbol in self.config.pairs:
            try:
                # Подписка на partial book depth (20 уровней)
                self.ws_client.start_depth_socket(
                    callback=self._on_message,
                    symbol=symbol
                )
                logging.info(f"📡 WS подписка Binance: {symbol}")
                time.sleep(0.1)  # Небольшая задержка между подписками
            except Exception as e:
                logging.error(f"❌ Не удалось подписаться на {symbol}: {e}")
        
        logging.info("🌐 Binance WS запущен. Начинаем цикл обработки...")
        
        try:
            while True:
                self._process_orderbook_queue()
                self._maybe_close_positions_by_time()
                self._maybe_write_daily_report()
                self._update_equity_curve(datetime.now(timezone.utc))
                time.sleep(0.5)
        except KeyboardInterrupt:
            logging.info("⏹️ Остановка по Ctrl+C")
        except Exception as e:
            logging.error(f"❌ Критическая ошибка realtime: {e}")
        finally:
            if self.ws_client:
                self.ws_client.stop()
            try:
                self._write_daily_report(datetime.now(timezone.utc).date())
            except Exception:
                pass
            return self._analyze_results()

    def _run_record(self) -> Dict:
        """Режим записи данных в JSON (с опциональной торговлей)"""
        self._report_time_minutes = self._parse_report_time(self.config.report_time_utc)
        self.record_start_time = datetime.now(timezone.utc)
        
        # Создаём директорию для данных
        os.makedirs(self.config.data_dir, exist_ok=True)
        
        # Создаём WebSocket клиент с API ключами
        self.ws_client = ThreadedWebsocketManager(
            api_key=self.config.api_key,
            api_secret=self.config.api_secret
        )
        self.ws_client.start()
        
        logging.info(f"🔴 Запуск WebSocket соединений...")
        time.sleep(2)  # Даём время на инициализацию
        
        # Подписываемся на orderbook
        for symbol in self.config.pairs:
            try:
                self.ws_client.start_depth_socket(
                    callback=self._on_message_record,
                    symbol=symbol
                )
                logging.info(f"🔴 Запись: {symbol}")
                time.sleep(0.1)  # Небольшая задержка между подписками
            except Exception as e:
                logging.error(f"❌ Ошибка подписки на {symbol}: {e}")
        
        if self.config.trade_and_record:
            logging.info(f"🔴 Режим: ЗАПИСЬ + ТОРГОВЛЯ")
        else:
            logging.info(f"🔴 Режим: только ЗАПИСЬ (торговля отключена)")
        
        logging.info(f"⏱️  Длительность: {self.config.record_duration} сек ({self.config.record_duration/3600:.1f} часов)")
        logging.info(f"💾 Автосохранение каждые 10 минут")
        
        last_save = time.time()
        
        try:
            while True:
                elapsed = (datetime.now(timezone.utc) - self.record_start_time).total_seconds()
                
                # ВАЖНО: Обрабатываем очередь для торговли
                if self.config.trade_and_record:
                    self._process_orderbook_queue()
                    self._maybe_close_positions_by_time()
                    self._maybe_write_daily_report()
                    self._update_equity_curve(datetime.now(timezone.utc))
                
                # Автосохранение каждые 10 минут
                if time.time() - last_save > 600:  # 10 минут
                    if self.config.trade_and_record:
                        logging.info(f"💰 Автосохранение: Баланс ${self.current_balance:.2f} | "
                                   f"Позиций: {len(self.open_positions)} | "
                                   f"Сделок: {len(self.trades)}")
                        self._save_recorded_data_partial()
                    last_save = time.time()
                
                # Проверка завершения
                if elapsed >= self.config.record_duration:
                    logging.info("⏹️ Завершение записи по таймеру")
                    break
                
                # Показываем прогресс каждые 5 минут
                if int(elapsed) % 300 == 0 and int(elapsed) > 0:  # Каждые 5 минут
                    progress = (elapsed / self.config.record_duration) * 100
                    
                    if self.config.trade_and_record:
                        # Подробная статистика в консоль
                        wins = len([t for t in self.trades if t['pnl'] > 0])
                        losses = len([t for t in self.trades if t['pnl'] < 0])
                        total_pnl = sum(t['pnl'] for t in self.trades) if self.trades else 0
                        win_rate = (wins / len(self.trades) * 100) if self.trades else 0
                        
                        print("\n" + "┌" + "─"*78 + "┐")
                        print(f"│ 📊 СТАТИСТИКА ({elapsed/60:.0f} минут работы)")
                        print("├" + "─"*78 + "┤")
                        print(f"│ ⏱️  Прогресс:        {progress:.1f}% ({elapsed/60:.0f}/{self.config.record_duration/60:.0f} мин)")
                        print(f"│ 📊 Всего сделок:     {len(self.trades)}")
                        print(f"│ ✅ Прибыльных:       {wins} ({win_rate:.1f}%)")
                        print(f"│ ❌ Убыточных:        {losses}")
                        print(f"│ 📈 Открыто позиций:  {len(self.open_positions)}")
                        print(f"│ 💰 Текущий баланс:   ${self.current_balance:,.2f}")
                        print(f"│ 💎 Общий P&L:        ${total_pnl:,.2f}")
                        print("└" + "─"*78 + "┘\n")
                
                time.sleep(0.5 if self.config.trade_and_record else 1)
                
        except KeyboardInterrupt:
            logging.info("⏹️ Запись остановлена по Ctrl+C")
        except Exception as e:
            logging.error(f"❌ Ошибка записи: {e}")
        finally:
            if self.ws_client:
                try:
                    self.ws_client.stop()
                except Exception:
                    pass
            try:
                self._write_daily_report(datetime.now(timezone.utc).date())
            except Exception:
                pass
            self._save_recorded_data()
            
            # Возвращаем результаты торговли
            if self.config.trade_and_record:
                return self._analyze_results()
            else:
                return {'info': 'Работа завершена', 'trades': len(self.trades)}

    def _run_replay(self) -> Dict:
        """Режим воспроизведения из JSON файлов"""
        self._report_time_minutes = self._parse_report_time(self.config.report_time_utc)
        
        # Загружаем данные
        data_files = self._load_recorded_data()
        if not data_files:
            logging.error("❌ Нет данных для воспроизведения")
            return {'error': 'Нет данных для воспроизведения'}
        
        logging.info(f"📂 Загружено записей: {len(data_files)}")
        logging.info("▶️ Начинаем воспроизведение...")
        
        try:
            for record in data_files:
                symbol = record.get('symbol')
                message = record.get('data')
                timestamp = record.get('timestamp')
                
                if symbol and message:
                    # Помещаем в очередь как обычное сообщение
                    try:
                        self.orderbook_queue.put_nowait((symbol, message))
                    except queue.Full:
                        pass
                
                # Обрабатываем очередь
                self._process_orderbook_queue()
                self._maybe_close_positions_by_time()
                self._update_equity_curve(datetime.now(timezone.utc))
                
                # Небольшая задержка для симуляции реального времени
                time.sleep(0.01)
                
        except KeyboardInterrupt:
            logging.info("⏹️ Воспроизведение остановлено")
        finally:
            return self._analyze_results()

    def _on_message_record(self, message):
        """Обработчик для записи данных - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            # Обрабатываем разные типы сообщений
            if isinstance(message, str):
                data = json.loads(message)
            elif isinstance(message, dict):
                data = message
            else:
                return
            
            # ДИАГНОСТИКА: Показываем первое сообщение
            if not hasattr(self, '_first_message_shown'):
                self._first_message_shown = True
                print(f"\n{'='*80}")
                print(f"🔍 ПЕРВОЕ СООБЩЕНИЕ ОТ BINANCE:")
                print(f"{'='*80}")
                print(f"Тип: {type(data)}")
                print(f"Ключи: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
                print(f"Содержимое: {json.dumps(data, indent=2)[:500]}")
                print(f"{'='*80}\n")
            
            # Обрабатываем ОБА формата: полный orderbook И depth updates
            symbol = None
            bids = None
            asks = None
            
            # ФОРМАТ 1: Полный orderbook snapshot (ключи 'bids', 'asks')
            if 'bids' in data and 'asks' in data:
                symbol = data.get('s', data.get('symbol', '')).upper()
                bids = data['bids']
                asks = data['asks']
                
                if len(self.recorded_data) < 3:
                    print(f"✅ ФОРМАТ 1: Полный orderbook для {symbol}")
            
            # ФОРМАТ 2: Depth update events (ключи 'b', 'a')
            elif 'b' in data and 'a' in data:
                symbol = data.get('s', '').upper()
                bids = data['b']
                asks = data['a']
                
                if len(self.recorded_data) < 3:
                    print(f"✅ ФОРМАТ 2: Depth update для {symbol}")
                
                # Преобразуем в стандартный формат
                data['bids'] = bids
                data['asks'] = asks
            
            # ФОРМАТ 3: Другие события (игнорируем)
            else:
                if len(self.recorded_data) < 3:
                    print(f"⚪ Пропущено: {data.get('e', 'неизвестное событие')}")
                return
            
            # Проверяем символ
            if not symbol:
                return
            
                if symbol in self.config.pairs:
                # НЕ записываем сырые orderbook данные (экономим место и память)
                # self.recorded_data.append(...)  ← ОТКЛЮЧЕНО
                
                # Добавляем в очередь для торговли
                    try:
                        self.orderbook_queue.put_nowait((symbol, data))
                    except queue.Full:
                        pass
                
                # Логируем первые 3
                if self.orderbook_queue.qsize() <= 3:
                    print(f"✅ В очередь: {symbol} (размер: {self.orderbook_queue.qsize()})")
                
        except Exception as e:
            logging.error(f"Ошибка обработчика: {e}")

    def _save_recorded_data(self):
        """Сохраняет ТОЛЬКО торговые данные (сделки + баланс)"""
        try:
            # НЕ сохраняем сырые orderbook данные (экономим место)
            # Сохраняем только результаты торговли
            
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            
            # Сохраняем сделки (всегда, даже если пустой список)
            trades_file = f"trades_{timestamp}.json"
            with open(trades_file, 'w', encoding='utf-8') as f:
                json.dump(self.trades, f, indent=2, ensure_ascii=False, default=str)
            logging.info(f"💾 Сделки сохранены: {trades_file} ({len(self.trades)} шт.)")
            
            # Сохраняем изменения баланса (всегда, даже если пустая)
            balance_history = [
                {
                    'timestamp': e['timestamp'].isoformat() if isinstance(e['timestamp'], datetime) else str(e['timestamp']),
                    'balance': e['balance'],
                    'open_positions': e['open_positions']
                }
                for e in self.equity_curve
            ]
            balance_file = f"balance_history_{timestamp}.json"
            with open(balance_file, 'w', encoding='utf-8') as f:
                json.dump(balance_history, f, indent=2, ensure_ascii=False)
            logging.info(f"💾 История баланса сохранена: {balance_file} ({len(balance_history)} точек)")
            
        except Exception as e:
            logging.error(f"❌ Ошибка сохранения данных: {e}")
    
    def _save_recorded_data_partial(self):
        """Промежуточное сохранение ТОЛЬКО торговых данных"""
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            
            # Сохраняем только сделки (компактно)
            if self.trades:
                trades_file = f"trades_partial_{timestamp}.json"
                with open(trades_file, 'w', encoding='utf-8') as f:
                    json.dump(self.trades, f, indent=2, ensure_ascii=False, default=str)
                logging.info(f"💾 Промежуточно: сделок {len(self.trades)}, баланс ${self.current_balance:,.2f}")
            
        except Exception as e:
            logging.error(f"❌ Ошибка промежуточного сохранения: {e}")

    def _load_recorded_data(self) -> List[Dict]:
        """Загружает записанные данные из JSON"""
        try:
            # Ищем самый новый файл с данными
            data_files = []
            if os.path.exists(self.config.data_dir):
                for filename in os.listdir(self.config.data_dir):
                    if filename.startswith('orderbook_data_') and filename.endswith('.json'):
                        filepath = os.path.join(self.config.data_dir, filename)
                        data_files.append(filepath)
            
            if not data_files:
                return []
            
            # Берём самый новый файл
            latest_file = max(data_files, key=os.path.getmtime)
            logging.info(f"📂 Загрузка данных из: {latest_file}")
            
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return data
        except Exception as e:
            logging.error(f"❌ Ошибка загрузки данных: {e}")
            return []

    def _on_message(self, message):
        """Обработчик WebSocket сообщений от Binance - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        try:
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            # Обрабатываем ОБА формата
            symbol = None
            
            # ФОРМАТ 1: Полный orderbook (ключи 'bids', 'asks')
            if 'bids' in data and 'asks' in data:
                symbol = data.get('s', data.get('symbol', '')).upper()
            
            # ФОРМАТ 2: Depth update events (ключи 'b', 'a')
            elif 'b' in data and 'a' in data:
                symbol = data.get('s', '').upper()
                # Преобразуем в стандартный формат
                data['bids'] = data['b']
                data['asks'] = data['a']
            
            # Если символ найден и в списке пар
            if symbol and symbol in self.config.pairs:
                    try:
                        self.orderbook_queue.put_nowait((symbol, data))
                    except queue.Full:
                        try:
                            _ = self.orderbook_queue.get_nowait()
                            self.orderbook_queue.put_nowait((symbol, data))
                        except Exception:
                            pass
                        
        except Exception as e:
            logging.error(f"Ошибка обработки WS сообщения: {e}")

    def _process_orderbook_queue(self):
        """Обработка очереди orderbook данных"""
        processed_count = 0
        skipped_dom = 0
        skipped_price = 0
        skipped_session = 0
        skipped_spread = 0
        skipped_no_signal = 0
        skipped_max_positions = 0
        skipped_spoof = 0
        
        # ДИАГНОСТИКА: Логируем первый вызов функции
        if not hasattr(self, '_process_queue_called'):
            self._process_queue_called = True
            print(f"\n🔍 PROCESS_QUEUE: Функция вызвана ВПЕРВЫЕ!")
            print(f"   Размер очереди: {self.orderbook_queue.qsize()}")
            print(f"   Открыто позиций: {len(self.open_positions)}")
            print(f"   Макс позиций: {self.config.max_open_positions}\n")
        
        # Логируем если очередь не пуста
        queue_size = self.orderbook_queue.qsize()
        if queue_size > 0 and processed_count == 0:
            print(f"\n🔍 PROCESS_QUEUE: Начинаем обработку {queue_size} сообщений...")
        
        while not self.orderbook_queue.empty():
            symbol, message = self.orderbook_queue.get_nowait()
            processed_count += 1
            
            # ПРОВЕРКА 1: DOM анализ
            dom_analysis = self.strategy.dom_analyzer.analyze_dom_structure(message)
            if not dom_analysis:
                skipped_dom += 1
                continue
            
            # ПРОВЕРКА 2: Текущее время и цена
            now = datetime.now(timezone.utc)
            current_price = dom_analysis.get('mid_price')
            best_bid = dom_analysis.get('best_bid_price')
            best_ask = dom_analysis.get('best_ask_price')
            
            if current_price is None:
                try:
                    bids = message.get('bids', [])
                    asks = message.get('asks', [])
                    if bids and asks:
                        best_bid = float(bids[0][0])
                        best_ask = float(asks[0][0])
                        current_price = (best_bid + best_ask) / 2
                except Exception:
                    skipped_price += 1
                    continue
            
            if current_price is None:
                skipped_price += 1
                continue
            
            # Обновляем состояние символа (волатильность, анти-спуф)
            self.strategy.update_symbol_state(symbol, dom_analysis, current_price, now)
            if self.strategy.is_symbol_blocked(symbol, now):
                skipped_spoof += 1
                continue
            
            # ПРОВЕРКА 3: Фильтр торговых сессий
            if not self.strategy.is_time_allowed(now):
                skipped_session += 1
                continue
            
            # Определение движения цены
            prev_price = self._last_price.get(symbol)
            price_movement = self.strategy.detect_price_movement(current_price, prev_price) if prev_price else 'sideways'
            self._last_price[symbol] = current_price
            
            # ПРОВЕРКА 4: Контроль спреда
            try:
                spread_pct = abs(best_ask - best_bid) / current_price * 100 if (best_ask and best_bid) else dom_analysis.get('spread_percent')
                if spread_pct and spread_pct > self.config.max_spread_percent:
                    skipped_spread += 1
                    continue
            except Exception:
                pass
            
            # Проверка TP/SL для открытых позиций
            self._update_positions(symbol, current_price, now)
            
            # ПРОВЕРКА 5: Генерация сигнала (с учётом устойчивости по символу)
            total_top_liq = dom_analysis.get('bid_top2_usdt', 0) + dom_analysis.get('ask_top2_usdt', 0)
            signal = self.strategy.generate_trading_signal(symbol, dom_analysis, current_price, price_movement, now, total_top_liq)
            
            if not signal:
                skipped_no_signal += 1
            elif len(self.open_positions) >= self.config.max_open_positions:
                skipped_max_positions += 1
            else:
                # ✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ - ОТКРЫВАЕМ СДЕЛКУ!
                self._execute_trade(symbol, signal, current_price, now, dom_analysis)
            
            # ДИАГНОСТИКА: Статистика обработки каждые 500 сообщений
            if processed_count % 500 == 0:
                print(f"\n🔍 ДИАГНОСТИКА (обработано {processed_count} сообщений):")
                print(f"   ❌ Пропущено DOM пустой:     {skipped_dom}")
                print(f"   ❌ Пропущено нет цены:       {skipped_price}")
                print(f"   ❌ Пропущено вне сессии:     {skipped_session}")
                print(f"   ❌ Пропущено большой спред:  {skipped_spread}")
                print(f"   ❌ Пропущено нет сигнала:    {skipped_no_signal}")
                print(f"   ❌ Пропущено макс. позиций:  {skipped_max_positions}")
                print(f"   ❌ Пропущено спуф-фильтр:    {skipped_spoof}")
                print(f"   ✅ Открыто позиций:          {len(self.open_positions)}")
                print(f"   ✅ Всего сделок:             {len(self.trades)}\n")
                
                logging.info(f"📊 Обработано: {processed_count} | Сделок: {len(self.trades)} | "
                           f"Фильтры: DOM={skipped_dom}, цена={skipped_price}, сессия={skipped_session}, "
                           f"спред={skipped_spread}, сигнал={skipped_no_signal}, позиции={skipped_max_positions}")

    def _update_positions(self, symbol: str, current_price: float, current_time: datetime):
        """Обновляет открытые позиции и проверяет TP/SL"""
        positions_to_close = []
        
        for i, position in enumerate(self.open_positions):
            if position['symbol'] != symbol:
                continue
            
            # Трейлинг-стоп: обновляем максимум/минимум и подтягиваем SL
            if self.config.trailing_stop_enabled:
                if position['direction'] == 'Buy':
                    # обновляем максимум цены
                    if position.get('peak_price') is None or current_price > position['peak_price']:
                        position['peak_price'] = current_price
                    # активируем трейлинг после заданного хода цены
                    if not position.get('trailing_active', False):
                        if current_price >= position['entry_price'] * (1 + self.config.trailing_activation_percent):
                            position['trailing_active'] = True
                    # подтягиваем стоп, если активирован
                    if position.get('trailing_active', False):
                        dynamic_trailing = position.get('trailing_percent', self.config.trailing_percent)
                        new_sl = position['peak_price'] * (1 - dynamic_trailing)
                        if new_sl > position['stop_loss']:
                            position['stop_loss'] = new_sl
                else:  # Sell
                    # обновляем минимум цены
                    if position.get('trough_price') is None or current_price < position['trough_price']:
                        position['trough_price'] = current_price
                    if not position.get('trailing_active', False):
                        if current_price <= position['entry_price'] * (1 - self.config.trailing_activation_percent):
                            position['trailing_active'] = True
                    if position.get('trailing_active', False) and position.get('trough_price') is not None:
                        dynamic_trailing = position.get('trailing_percent', self.config.trailing_percent)
                        new_sl = position['trough_price'] * (1 + dynamic_trailing)
                        if new_sl < position['stop_loss']:
                            position['stop_loss'] = new_sl
            
            # Проверяем TP/SL
            if position['direction'] == 'Buy':
                if current_price >= position['take_profit'] or current_price <= position['stop_loss']:
                    positions_to_close.append(i)
            else:  # Sell
                if current_price <= position['take_profit'] or current_price >= position['stop_loss']:
                    positions_to_close.append(i)
        
        # Закрываем позиции
        for i in reversed(positions_to_close):
            self._close_position(i, current_price, current_time)

    def _maybe_close_positions_by_time(self):
        """Закрытие позиций по времени"""
        now = datetime.now(timezone.utc)
        for i in reversed(range(len(self.open_positions))):
            pos = self.open_positions[i]
            hold = (now - pos['entry_time']).total_seconds()
            max_hold = pos.get('max_hold_time', self.config.max_hold_time)
            if hold > max_hold:
                self._close_position(i, pos['entry_price'], now)

    def _parse_report_time(self, hhmm: str) -> int:
        """Парсинг времени отчёта"""
        try:
            hh, mm = hhmm.split(':')
            return int(hh) * 60 + int(mm)
        except Exception:
            return 23 * 60 + 59

    def _maybe_write_daily_report(self):
        """Проверка и запись дневного отчёта"""
        now = datetime.now(timezone.utc)
        today_minutes = now.hour * 60 + now.minute
        if self._last_report_date == now.date():
            return
        if today_minutes >= self._report_time_minutes:
            self._write_daily_report(now.date())
            self._last_report_date = now.date()

    def _write_daily_report(self, report_date):
        """Запись дневного отчёта"""
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
                'exchange': 'Binance',
                'summary': self._analyze_results().get('summary', {}),
                'by_symbol': by_symbol,
            }
            path = os.path.join(self.config.report_dir, f"binance_report_{report_date}.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            logging.info(f"📄 Отчёт Binance за {report_date} сохранён: {path}")
        except Exception as e:
            logging.error(f"Ошибка записи отчёта: {e}")
    
    def _execute_trade(self, symbol: str, signal: Dict, price: float, 
                      timestamp: datetime, dom_analysis: Dict):
        """Выполняет торговую операцию"""
        try:
            atr_value = self.strategy.get_atr_value(symbol)
            min_stop_distance = price * self.config.min_stop_loss_percent
            atr_stop_distance = atr_value * self.config.atr_stop_multiplier if atr_value else 0.0
            stop_distance = max(min_stop_distance, atr_stop_distance)
            if stop_distance <= 0:
                stop_distance = min_stop_distance if min_stop_distance > 0 else price * 0.005
            rr_ratio = max(self.config.risk_reward_ratio, self.config.min_rr_ratio)
            
            if signal['direction'] == 'Buy':
                stop_loss = max(price - stop_distance, 0.0)
                take_profit = price + stop_distance * rr_ratio
            else:
                stop_loss = price + stop_distance
                take_profit = price - stop_distance * rr_ratio
                if take_profit < 0:
                    take_profit = 0.0
            
            position_size = self._calculate_position_size(price, stop_loss, signal['confidence'])
            trailing_percent = self.strategy.get_dynamic_trailing_percent(symbol, price) if self.config.trailing_stop_enabled else None
            if position_size <= 0:
                logging.info(f"⚠️ Пропуск сделки {symbol}: размер позиции нулевой после расчёта риска")
                return
            
            # Динамическое время удержания
            dynamic_max_hold = self.config.max_hold_time
            if self.config.dynamic_hold_enabled:
                # Оценим силу тренда по уверенности и направлению движения
                trend_multiplier = 1.0
                if signal.get('confidence', 1.0) >= self.config.strong_trend_confidence:
                    trend_multiplier = self.config.hold_time_trend_boost
                
                # Волатильность из стратегии (EWMA в %)
                vol = getattr(self.strategy, '_ewma_vol_percent', None)
                vol_multiplier = 1.0
                if isinstance(vol, (int, float)):
                    if vol > self.config.vol_high_percent:
                        vol_multiplier = self.config.hold_time_high_vol_cut
                    elif vol < self.config.vol_low_percent:
                        vol_multiplier = self.config.hold_time_low_vol_boost
                
                dynamic_max_hold = int(self.config.max_hold_time * trend_multiplier * vol_multiplier)
            
            position = {
                'symbol': symbol,
                'direction': signal['direction'],
                'entry_price': price,
                'size': position_size,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'initial_stop_loss': stop_loss,
                'entry_time': timestamp,
                'confidence': signal['confidence'],
                'reason': signal['reason'],
                # Трейлинг-поля
                'trailing_active': False,
                'peak_price': price if signal['direction'] == 'Buy' else None,
                'trough_price': price if signal['direction'] == 'Sell' else None,
                'trailing_percent': trailing_percent if trailing_percent is not None else self.config.trailing_percent,
                # Динамическое удержание
                'max_hold_time': dynamic_max_hold,
            }
            
            self.open_positions.append(position)
            
            # Красивый вывод в консоль
            print("\n" + "="*80)
            print(f"🟢 ОТКРЫТА ПОЗИЦИЯ #{len(self.trades) + len(self.open_positions)}")
            print("="*80)
            print(f"📊 Символ:       {symbol}")
            print(f"📈 Направление:  {signal['direction']}")
            print(f"💵 Цена входа:   ${price:,.6f}")
            print(f"📦 Размер:       ${position_size:,.2f}")
            print(f"🎯 Take Profit:  ${take_profit:,.6f} (+{self.config.take_profit_percent*100:.1f}%)")
            print(f"🛑 Stop Loss:    ${stop_loss:,.6f} (-{self.config.stop_loss_percent*100:.1f}%)")
            print(f"⭐ Уверенность:  {signal['confidence']:.2f}")
            print(f"💡 Причина:      {signal['reason']}")
            print(f"⏰ Время:        {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            if self.config.dynamic_hold_enabled:
                print(f"⏳ Макс удерж.:  {dynamic_max_hold} сек")
            print(f"📊 Открыто позиций: {len(self.open_positions)}")
            print(f"💰 Текущий баланс: ${self.current_balance:,.2f}")
            print("="*80 + "\n")
            
            logging.info(f"✅ ОТКРЫТА: {signal['direction']} {symbol} @ ${price:,.6f} | TP: ${take_profit:,.6f} | SL: ${stop_loss:,.6f}")
            
        except Exception as e:
            logging.error(f"Ошибка выполнения сделки: {str(e)}")
    
    def _close_position(self, position_index: int, exit_price: float, exit_time: datetime):
        """Закрывает позицию"""
        try:
            position = self.open_positions[position_index]
            
            if position['direction'] == 'Buy':
                pnl = (exit_price - position['entry_price']) / position['entry_price']
            else:
                pnl = (position['entry_price'] - exit_price) / position['entry_price']
            
            # Расчет PnL в деньгах
            pnl_amount = pnl * position['size'] * self.config.leverage

            # Учет комиссий (вход + выход)
            commission_cost = position['size'] * self.config.commission_rate * 2
            pnl_amount_after_fee = pnl_amount - commission_cost
            self.current_balance += pnl_amount_after_fee
            
            hold_seconds = (exit_time - position['entry_time']).total_seconds()
            
            trade = {
                'symbol': position['symbol'],
                'direction': position['direction'],
                'entry_price': position['entry_price'],
                'exit_price': exit_price,
                'entry_time': position['entry_time'],
                'exit_time': exit_time,
                'size': position['size'],
                'pnl': pnl_amount_after_fee,
                'pnl_percent': pnl * 100,
                'commission': commission_cost,
                'confidence': position['confidence'],
                'reason': position['reason'],
                'hold_time': hold_seconds
            }
            
            self.trades.append(trade)
            del self.open_positions[position_index]
            
            # Определяем результат
            if pnl_amount_after_fee > 0:
                result_emoji = "🟢"
                result_text = "ПРИБЫЛЬ"
                color = "+"
            else:
                result_emoji = "🔴"
                result_text = "УБЫТОК"
                color = ""
            
            # Красивый вывод в консоль
            print("\n" + "="*80)
            print(f"{result_emoji} ЗАКРЫТА ПОЗИЦИЯ #{len(self.trades)} - {result_text}")
            print("="*80)
            print(f"📊 Символ:       {position['symbol']}")
            print(f"📈 Направление:  {position['direction']}")
            print(f"💵 Вход:         ${position['entry_price']:,.6f}")
            print(f"💵 Выход:        ${exit_price:,.6f}")
            print(f"📦 Размер:       ${position['size']:,.2f}")
            print(f"💰 P&L:          {color}${pnl_amount_after_fee:,.2f} ({color}{pnl*100:.2f}%)  (комиссии: ${commission_cost:,.2f})")
            print(f"⏱️  Удержание:    {hold_seconds:.0f} сек ({hold_seconds/60:.1f} мин)")
            print(f"💵 Новый баланс: ${self.current_balance:,.2f}")
            print(f"📊 Всего сделок: {len(self.trades)}")
            print(f"✅ Прибыльных:   {len([t for t in self.trades if t['pnl'] > 0])}")
            print(f"❌ Убыточных:    {len([t for t in self.trades if t['pnl'] < 0])}")
            if len(self.trades) > 0:
                total_pnl = sum(t['pnl'] for t in self.trades)
                print(f"💎 Общий P&L:    ${total_pnl:,.2f}")
            print("="*80 + "\n")
            
            logging.info(f"🔄 ЗАКРЫТА: {position['direction']} {position['symbol']} @ ${exit_price:,.6f} | "
                        f"P&L: ${pnl_amount_after_fee:,.2f} ({pnl*100:.2f}%) | Комиссии: ${commission_cost:,.2f} | Удержание: {hold_seconds:.0f}с")
            
        except Exception as e:
            logging.error(f"Ошибка закрытия позиции: {str(e)}")
    
    def _calculate_position_size(self, entry_price: float, stop_loss: float, confidence: float) -> float:
        """Рассчитывает размер позиции на основе риска"""
        if entry_price <= 0:
            return 0.0
        risk_fraction = max(self.config.risk_per_trade, 0) / 100
        base_risk = self.current_balance * risk_fraction
        confidence_multiplier = min(max(confidence, 0.5), 2.0)
        risk_amount = base_risk * confidence_multiplier
        stop_distance = abs(entry_price - stop_loss)
        if stop_distance <= 0:
            return min(risk_amount * self.config.leverage, self.current_balance * self.config.leverage)
        stop_pct = stop_distance / entry_price
        if stop_pct <= 1e-6:
            stop_pct = self.config.min_stop_loss_percent
        position_notional = risk_amount / stop_pct
        max_notional = self.current_balance * self.config.leverage
        return max(0.0, min(position_notional, max_notional))
    
    def _update_equity_curve(self, timestamp: datetime):
        """Обновляет кривую доходности"""
        self.equity_curve.append({
            'timestamp': timestamp,
            'balance': self.current_balance,
            'open_positions': len(self.open_positions)
        })
        # Ограничиваем размер
        if len(self.equity_curve) > 10000:
            self.equity_curve = self.equity_curve[-10000:]
    
    def _analyze_results(self) -> Dict:
        """Анализирует результаты бектеста"""
        if not self.trades:
            return {'error': 'Нет сделок для анализа'}
        
        total_trades = len(self.trades)
        winning_trades = len([t for t in self.trades if t['pnl'] > 0])
        losing_trades = len([t for t in self.trades if t['pnl'] < 0])
        
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        total_pnl = sum(t['pnl'] for t in self.trades)
        total_pnl_percent = (total_pnl / self.initial_balance) * 100
        
        avg_win = np.mean([t['pnl'] for t in self.trades if t['pnl'] > 0]) if winning_trades > 0 else 0
        avg_loss = np.mean([t['pnl'] for t in self.trades if t['pnl'] < 0]) if losing_trades > 0 else 0
        
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
        
        equity_values = [e['balance'] for e in self.equity_curve]
        max_drawdown = self._calculate_max_drawdown(equity_values)
        
        avg_hold_time = np.mean([t['hold_time'] for t in self.trades])
        
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
                'exchange': 'Binance',
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

# ===== ФУНКЦИЯ СОХРАНЕНИЯ РЕЗУЛЬТАТОВ =====
def _save_results_always(results: Dict):
    """Сохраняет результаты бектеста в файлы (всегда вызывается)"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    saved_files = []
    
    try:
        # 1. Сделки (сохраняем всегда, даже если пустой список)
        trades = results.get('trades', [])
        trades_file = f'trades_{timestamp}.json'
        with open(trades_file, 'w', encoding='utf-8') as f:
            json.dump(trades, f, indent=2, default=str, ensure_ascii=False)
        saved_files.append(trades_file)
        print(f"\n💾 Сделки: {trades_file} ({len(trades)} шт.)")
        logging.info(f"💾 Сделки сохранены: {trades_file} ({len(trades)} шт.)")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения сделок: {e}")
        print(f"❌ Ошибка сохранения сделок: {e}")
    
    try:
        # 2. Статистика (сохраняем если есть)
        if 'summary' in results and results['summary']:
            summary_file = f'summary_{timestamp}.json'
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(results['summary'], f, indent=2, default=str, ensure_ascii=False)
            saved_files.append(summary_file)
            print(f"💾 Статистика: {summary_file}")
            logging.info(f"💾 Статистика сохранена: {summary_file}")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения статистики: {e}")
        print(f"❌ Ошибка сохранения статистики: {e}")
    
    try:
        # 3. История баланса (сохраняем всегда, даже если пустая)
        equity_curve = results.get('equity_curve', [])
        balance_file = f'balance_{timestamp}.json'
        
        # Берём каждую 100-ю точку для экономии места
        equity_sample = equity_curve[::100] if len(equity_curve) > 100 else equity_curve
        
        balance_data = [
            {
                'timestamp': e['timestamp'].isoformat() if isinstance(e['timestamp'], datetime) else str(e['timestamp']),
                'balance': e['balance'],
                'open_positions': e['open_positions']
            }
            for e in equity_sample
        ]
        
        with open(balance_file, 'w', encoding='utf-8') as f:
            json.dump(balance_data, f, indent=2, ensure_ascii=False)
        saved_files.append(balance_file)
        print(f"💾 История баланса: {balance_file} ({len(balance_data)} точек)")
        logging.info(f"💾 История баланса сохранена: {balance_file} ({len(balance_data)} точек)")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения истории баланса: {e}")
        print(f"❌ Ошибка сохранения истории баланса: {e}")
    
    # Итоговое сообщение
    if saved_files:
        print(f"\n✅ Сохранено файлов: {len(saved_files)}")
        for f in saved_files:
            print(f"   📄 {f}")
        logging.info(f"✅ Сохранено {len(saved_files)} файлов результатов")
    else:
        print(f"\n⚠️ ВНИМАНИЕ: Файлы не были сохранены!")
        logging.warning("⚠️ Результаты не были сохранены в файлы")

# ===== ГЛАВНАЯ ФУНКЦИЯ =====
def main():
    """Главная функция бектеста на Binance"""
    # Настройка логирования с UTF-8
    file_handler = logging.FileHandler("dom_backtest_binance.log", encoding='utf-8')
    stream_handler = logging.StreamHandler(sys.stdout)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[file_handler, stream_handler]
    )
    
    # Создаем конфигурацию для Binance
    config = DOMBacktestConfig(
        # ===== ВЫБОР ПАР =====
        # Вариант 1: ВСЕ пары Binance (автоматически)
        use_all_pairs=True,  # ← True для ВСЕХ пар
        min_24h_volume_usdt=1000000.0,  # Минимум $1M объёма за 24ч (фильтр ликвидности)
        only_usdt_pairs=True,  # Только USDT пары
        exclude_pairs=[],  # Исключить пары (например: ["BTCUSDT", "ETHUSDT"])
        
        # Вариант 2: Выбранные пары вручную (если use_all_pairs=False)
        pairs=[
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
            "DOGEUSDT", "ADAUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT",
            "DOTUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
            "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT"
        ],
        
        # Режимы работы
        record_mode=True,  # ← ВКЛЮЧИТЬ для торговли + записи
        replay_mode=False,  # True - воспроизвести из JSON
        trade_and_record=True,  # True - торговать И записывать (работает если record_mode=True)
        record_duration=86400,  # ← 24 ЧАСА (86400 секунд)
        data_dir="data",  # Папка для сохранения JSON файлов
        
        # Торговые параметры
        min_bid_ask_ratio=1.8,  # Минимальное соотношение bid/ask (обновлено: было 1.05)
        min_liquidity_threshold=5000.0,  # Минимальная ликвидность в USDT (соответствует классу)
        take_profit_percent=0.03,  # 3% прибыли
        stop_loss_percent=0.01,  # 1% убытка
        max_hold_time=300,  # 5 минут
        risk_per_trade=1.0,
        trade_only_major_sessions=False,  # Торговать 24/7
        new_york_session=True,
        london_session=True,
        tokyo_session=True,
        max_spread_percent=0.25,  # Максимальный спред (соответствует классу)
        min_volume_threshold=1000,  # Минимальный объем (соответствует классу)
        volatility_filter=True,  # Фильтр по волатильности (соответствует классу)
        max_volatility_percent=10.0,
        debug_signals=False  # Отключаем для скорости
    )
    
    # Создаем и запускаем бектестер
    backtester = DOMBacktester(config)
    results = backtester.run_backtest()
    
    # Выводим результаты
    if 'error' in results:
        logging.error(f"Ошибка бектеста: {results['error']}")
        # Сохраняем результаты даже при ошибке
        _save_results_always(results)
        return
    
    # Если режим без торговли
    if 'info' in results and 'summary' not in results:
        print("\n" + "="*60)
        print("⏹️ РАБОТА ЗАВЕРШЕНА")
        print("="*60)
        print(f"📊 Сделок: {results.get('trades', 0)}")
        print("="*60)
        # Сохраняем результаты даже в режиме без торговли
        _save_results_always(results)
        return
    
    # Если есть торговые результаты
    summary = results.get('summary', {})
    
    if summary:
        # Определяем результат
        pnl_emoji = "🟢" if summary['total_pnl'] > 0 else "🔴"
        
        print("\n" + "╔" + "═"*88 + "╗")
        print("║" + " "*25 + "💰 РЕЗУЛЬТАТЫ DOM БЕКТЕСТА НА BINANCE 💰" + " "*24 + "║")
        print("╠" + "═"*88 + "╣")
        print("║" + " "*88 + "║")
        print(f"║  💵 Начальный баланс:        ${summary['initial_balance']:,.2f}" + " "*(63-len(f"{summary['initial_balance']:,.2f}")) + "║")
        print(f"║  💰 Конечный баланс:         ${summary['final_balance']:,.2f}" + " "*(63-len(f"{summary['final_balance']:,.2f}")) + "║")
        print(f"║  {pnl_emoji} Общий P&L:               ${summary['total_pnl']:,.2f} ({summary['total_pnl_percent']:.2f}%)" + " "*(50-len(f"{summary['total_pnl']:,.2f} ({summary['total_pnl_percent']:.2f}%)")) + "║")
        print("║" + " "*88 + "║")
        print("╠" + "─"*88 + "╣")
        print("║" + " "*88 + "║")
        print(f"║  📊 Всего сделок:            {summary['total_trades']}" + " "*(64-len(str(summary['total_trades']))) + "║")
        print(f"║  ✅ Прибыльных:              {summary['winning_trades']} ({summary['win_rate']:.1f}%)" + " "*(54-len(f"{summary['winning_trades']} ({summary['win_rate']:.1f}%)")) + "║")
        print(f"║  ❌ Убыточных:               {summary['losing_trades']} ({100-summary['win_rate']:.1f}%)" + " "*(54-len(f"{summary['losing_trades']} ({100-summary['win_rate']:.1f}%)")) + "║")
        print("║" + " "*88 + "║")
        print("╠" + "─"*88 + "╣")
        print("║" + " "*88 + "║")
        print(f"║  💚 Средний выигрыш:         ${summary['avg_win']:,.2f}" + " "*(63-len(f"{summary['avg_win']:,.2f}")) + "║")
        print(f"║  💔 Средний проигрыш:        ${abs(summary['avg_loss']):,.2f}" + " "*(63-len(f"{abs(summary['avg_loss']):,.2f}")) + "║")
        print(f"║  📊 Profit Factor:           {summary['profit_factor']:.2f}" + " "*(64-len(f"{summary['profit_factor']:.2f}")) + "║")
        print(f"║  📉 Максимальная просадка:   {summary['max_drawdown']:.2f}%" + " "*(63-len(f"{summary['max_drawdown']:.2f}")) + "║")
        print(f"║  ⏱️  Среднее время удержания: {summary['avg_hold_time']:.0f} сек ({summary['avg_hold_time']/60:.1f} мин)" + " "*(43-len(f"{summary['avg_hold_time']:.0f} ({summary['avg_hold_time']/60:.1f})")) + "║")
        print("║" + " "*88 + "║")
        print("╚" + "═"*88 + "╝\n")
        
        # Таблица последних 10 сделок
        if 'trades' in results and len(results['trades']) > 0:
            print("\n" + "╔" + "═"*88 + "╗")
            print("║" + " "*30 + "📝 ПОСЛЕДНИЕ 10 СДЕЛОК" + " "*36 + "║")
            print("╠" + "═"*88 + "╣")
            print("║ № │ Символ     │ Направ. │ Вход      │ Выход     │ P&L       │ P&L%   │ Время  ║")
            print("╠" + "═"*88 + "╣")
            
            for i, trade in enumerate(results['trades'][-10:], 1):
                pnl_emoji = "🟢" if trade['pnl'] > 0 else "🔴"
                symbol = trade['symbol'][:10].ljust(10)
                direction = trade['direction'][:7].ljust(7)
                entry = f"${trade['entry_price']:,.2f}"[:9].ljust(9)
                exit = f"${trade['exit_price']:,.2f}"[:9].ljust(9)
                pnl = f"${trade['pnl']:,.2f}"[:9].ljust(9)
                pnl_pct = f"{trade['pnl_percent']:,.1f}%"[:6].ljust(6)
                hold = f"{int(trade['hold_time']/60)}м"[:6].ljust(6)
                
                print(f"║ {i:2} │ {symbol} │ {direction} │ {entry} │ {exit} │ {pnl_emoji} {pnl} │ {pnl_pct} │ {hold} ║")
            
            print("╚" + "═"*88 + "╝\n")
    
    # Анализ по символам (только если есть торговые данные)
    if 'symbol_analysis' in results and results['symbol_analysis']:
        print("\n📊 АНАЛИЗ ПО СИМВОЛАМ:")
        print("-"*40)
        for symbol, analysis in results['symbol_analysis'].items():
            win_rate = (analysis['wins'] / analysis['trades']) * 100 if analysis['trades'] > 0 else 0
            print(f"{symbol}: {analysis['trades']} сделок, "
                  f"P&L: ${analysis['pnl']:,.2f}, "
                  f"Винрейт: {win_rate:.1f}%")
    
    # Сохраняем результаты в любом случае
    _save_results_always(results)

if __name__ == "__main__":
    main()

