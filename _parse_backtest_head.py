# -*- coding: utf-8 -*-
"""Читает summary / symbol_analysis / exit_type_analysis из огромного JSON (до корневого \"trades\")."""
import json
import mmap
import sys
from pathlib import Path


def main():
    path = Path(__file__).resolve().parent / "dom_backtest_direct_results.json"
    if not path.exists():
        print("Файл не найден:", path)
        sys.exit(1)

    needle = b'  "trades": ['
    idx = -1
    head_bytes = b""
    with path.open("rb") as f:
        try:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        except (OSError, ValueError):
            data = f.read()
            idx = data.find(needle)
            head_bytes = data[:idx] if idx != -1 else data
        else:
            try:
                idx = mm.find(needle)
                head_bytes = bytes(mm[:idx]) if idx != -1 else bytes(mm[:])
            finally:
                mm.close()
    if idx == -1:
        print("Не найден корневый фрагмент  '  \"trades\": [' — файл в неожиданном формате.")
        sys.exit(1)
    else:
        head = head_bytes.decode("utf-8").rstrip()
        if head.endswith(","):
            head = head[:-1]
        # После обрезки до \"trades\" корневой объект { ещё не закрыт — всегда добавляем }
        head = head + "\n}"
        try:
            obj = json.loads(head)
        except json.JSONDecodeError as e:
            print("JSON parse error:", e)
            print("Tail (800 chars):", repr(head[-800:]))
            sys.exit(1)

    s = obj.get("summary", {})
    print("=== SUMMARY ===")
    for k in [
        "total_trades",
        "winning_trades",
        "losing_trades",
        "win_rate",
        "total_pnl",
        "total_pnl_percent",
        "total_fees",
        "avg_win",
        "avg_loss",
        "profit_factor",
        "max_drawdown",
        "avg_hold_time",
        "initial_balance",
        "final_balance",
    ]:
        if k in s:
            print(f"{k}: {s[k]}")

    print("\n=== EXIT_TYPE_ANALYSIS ===")
    eta = obj.get("exit_type_analysis", {})
    for k, v in sorted(eta.items(), key=lambda x: -x[1].get("trades", 0)):
        print(
            f"{k}: trades={v.get('trades')} pnl={v.get('pnl', 0):.4f} wins={v.get('wins')}"
        )

    print("\n=== SYMBOLS (worst 15 by pnl) ===")
    sym = obj.get("symbol_analysis", {})
    items = [(k, v.get("pnl", 0), v.get("trades", 0), v.get("wins", 0)) for k, v in sym.items()]
    items.sort(key=lambda x: x[1])
    for k, pnl, tr, w in items[:15]:
        wr = (w / tr * 100) if tr else 0
        print(f"{k}: pnl={pnl:.4f} trades={tr} winrate={wr:.1f}%")

    print("\n=== SYMBOLS (best 10 by pnl) ===")
    for k, pnl, tr, w in sorted(items, key=lambda x: -x[1])[:10]:
        wr = (w / tr * 100) if tr else 0
        print(f"{k}: pnl={pnl:.4f} trades={tr} winrate={wr:.1f}%")


if __name__ == "__main__":
    main()
