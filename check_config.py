# -*- coding: utf-8 -*-
"""
Проверка текущей конфигурации и диагностика
"""

import sys

# Исправление кодировки для Windows
if sys.platform == 'win32':
    try:
        import codecs
        if hasattr(sys.stdout, 'buffer'):
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'replace')
        if hasattr(sys.stderr, 'buffer'):
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'replace')
    except:
        pass
    
    # Исправление для asyncio на Windows
    try:
        import asyncio
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except:
        pass

sys.path.insert(0, '.')

from backtest_dom_binance import DOMBacktestConfig

print("="*80)
print("🔍 ПРОВЕРКА ТЕКУЩЕЙ КОНФИГУРАЦИИ")
print("="*80)

# Создаём конфигурацию по умолчанию (из класса)
config_default = DOMBacktestConfig()

print("\n📋 НАСТРОЙКИ ИЗ КЛАССА (дефолтные):")
print(f"   use_all_pairs: {config_default.use_all_pairs}")
print(f"   realtime_mode: {config_default.realtime_mode}")
print(f"   record_mode: {config_default.record_mode}")
print(f"   replay_mode: {config_default.replay_mode}")
print(f"   trade_and_record: {config_default.trade_and_record}")
print(f"   min_liquidity_threshold: ${config_default.min_liquidity_threshold:,.0f}")
print(f"   min_bid_ask_ratio: {config_default.min_bid_ask_ratio}")
print(f"   max_spread_percent: {config_default.max_spread_percent}%")
print(f"   record_duration: {config_default.record_duration} сек ({config_default.record_duration/3600:.1f} часов)")
print(f"   Количество пар: {len(config_default.pairs)}")

print("\n" + "="*80)
print("🎯 КАКОЙ РЕЖИМ ЗАПУСТИТСЯ:")
print("="*80)

if config_default.replay_mode:
    print("📂 REPLAY MODE - воспроизведение из JSON")
elif config_default.record_mode:
    if config_default.trade_and_record:
        print("🟢 RECORD + TRADE MODE - торговля + запись")
        print("   ✅ Сделки БУДУТ открываться")
        print("   ✅ Данные БУДУТ записываться")
    else:
        print("🔴 RECORD MODE - только запись")
        print("   ❌ Сделки НЕ будут открываться")
        print("   ✅ Данные БУДУТ записываться")
else:
    print("🌐 REALTIME MODE - только торговля")
    print("   ✅ Сделки БУДУТ открываться")
    print("   ❌ Данные НЕ будут записываться")

print("\n" + "="*80)
print("⚠️  ПРОВЕРКА ПАРАМЕТРОВ:")
print("="*80)

# Проверка потенциальных проблем
problems = []

if config_default.min_liquidity_threshold > 5000:
    problems.append(f"❌ min_liquidity_threshold слишком высокий: ${config_default.min_liquidity_threshold:,.0f}")
    problems.append(f"   Рекомендуется: $3,000 - $5,000 USDT")

if config_default.min_bid_ask_ratio > 1.05:
    problems.append(f"❌ min_bid_ask_ratio слишком высокий: {config_default.min_bid_ask_ratio}")
    problems.append(f"   Рекомендуется: 1.01 - 1.03")

if config_default.max_spread_percent < 2.0:
    problems.append(f"⚠️  max_spread_percent может быть строгим: {config_default.max_spread_percent}%")
    problems.append(f"   Рекомендуется: 2.0% - 3.0%")

if not config_default.record_mode and not config_default.replay_mode:
    problems.append(f"⚠️  Режим работы: realtime (без записи)")
    problems.append(f"   Сделки не сохраняются в JSON автоматически")

if config_default.use_all_pairs and len(config_default.pairs) > 200:
    problems.append(f"⚠️  Очень много пар: {len(config_default.pairs)}")
    problems.append(f"   Может быть высокая нагрузка на систему")

if problems:
    print("\n⚠️  НАЙДЕНЫ ПОТЕНЦИАЛЬНЫЕ ПРОБЛЕМЫ:\n")
    for problem in problems:
        print(f"   {problem}")
else:
    print("\n✅ ВСЕ ПАРАМЕТРЫ В ПОРЯДКЕ!")

print("\n" + "="*80)
print("💡 РЕКОМЕНДАЦИИ:")
print("="*80)
print("""
Для МАКСИМУМА сделок используйте:

config = DOMBacktestConfig(
    # Режим
    record_mode=True,           # ← ОБЯЗАТЕЛЬНО!
    trade_and_record=True,      # ← ОБЯЗАТЕЛЬНО!
    
    # Пары
    use_all_pairs=True,         # Все пары Binance
    min_24h_volume_usdt=1000000.0,  # $1M+
    
    # Условия (МЯГКИЕ!)
    min_bid_ask_ratio=1.02,     # ← 2% перевес
    min_liquidity_threshold=3000.0,  # ← $3K (НЕ 10K!)
    max_spread_percent=3.0,     # ← 3%
)
""")

print("="*80)

