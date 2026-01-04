# -*- coding: utf-8 -*-
import os
import sys

print("="*60)
print("🔍 ПРОВЕРКА РАБОЧЕЙ ДИРЕКТОРИИ СКРИПТА")
print("="*60)
print(f"\n📁 Текущая рабочая директория (os.getcwd()):")
print(f"   {os.getcwd()}")
print(f"\n📄 Расположение скрипта (__file__):")
print(f"   {os.path.abspath(__file__)}")
print(f"\n📂 Директория скрипта (dirname):")
print(f"   {os.path.dirname(os.path.abspath(__file__))}")
print(f"\n🐍 Путь к Python (sys.executable):")
print(f"   {sys.executable}")
print(f"\n📋 sys.path (пути для импорта):")
for i, p in enumerate(sys.path[:5], 1):
    print(f"   {i}. {p}")

print("\n" + "="*60)
print("📝 ФАЙЛЫ, КОТОРЫЕ БУДУТ СОЗДАНЫ:")
print("="*60)

# Проверяем относительные пути
log_file = "dom_backtest_direct.log"
result_file = "dom_backtest_direct_results.json"
report_dir = "reports"

print(f"\n1. Лог файл (dom_backtest_direct.log):")
print(f"   → {os.path.join(os.getcwd(), log_file)}")

print(f"\n2. Файл результатов (dom_backtest_direct_results.json):")
print(f"   → {os.path.join(os.getcwd(), result_file)}")

print(f"\n3. Директория отчётов (reports/):")
print(f"   → {os.path.join(os.getcwd(), report_dir)}")

print("\n" + "="*60)
print("⚠️  ВАЖНО!")
print("="*60)
print("""
Скрипт использует ОТНОСИТЕЛЬНЫЕ пути, поэтому все файлы 
будут созданы в ТЕКУЩЕЙ РАБОЧЕЙ ДИРЕКТОРИИ (os.getcwd()),
а НЕ в директории самого скрипта!

Это зависит от того, ОТКУДА вы запускаете скрипт:
- Из E:\\Games\\Trading\\ → файлы там же
- Из E:\\Games\\MOB\\ → файлы будут в E:\\Games\\MOB\\
- Из C:\\Users\\... → файлы будут в C:\\Users\\...
""")
print("="*60)

