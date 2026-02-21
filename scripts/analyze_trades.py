#!/usr/bin/env python3
"""Query backend SQLite `trading.db`, compute simple trade analytics, and save a report.

Usage: python3 scripts/analyze_trades.py
"""
import sqlite3
import json
from pathlib import Path
from datetime import datetime
from statistics import median


ROOT = Path(__file__).resolve().parents[1] / 'backend'
DB_PATH = ROOT / 'data' / 'trading.db'
OUT_DIR = ROOT / 'data'
OUT_DIR.mkdir(parents=True, exist_ok=True)


def parse_iso(s):
    if not s:
        return None
    try:
        # Python 3.11+: fromisoformat handles offsets; this is tolerant for common ISO formats
        return datetime.fromisoformat(s)
    except Exception:
        # Fallback: try to strip Z
        try:
            if s.endswith('Z'):
                return datetime.fromisoformat(s[:-1])
        except Exception:
            return None
    return None


def fetch_trades(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades ORDER BY id ASC")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    trades = [dict(zip(cols, r)) for r in rows]
    return trades


def summarize(trades):
    n = len(trades)
    pnl_values = [t.get('pnl') for t in trades if t.get('pnl') is not None]

    winning = [p for p in pnl_values if p > 0]
    losing = [p for p in pnl_values if p < 0]

    durations = []
    for t in trades:
        et = parse_iso(t.get('entry_time'))
        xt = parse_iso(t.get('exit_time'))
        if et and xt:
            durations.append((xt - et).total_seconds())

    by_type = {}
    by_index = {}
    for t in trades:
        opt = t.get('option_type') or 'UNKNOWN'
        by_type.setdefault(opt, []).append(t)
        idx = t.get('index_name') or 'UNKNOWN'
        by_index.setdefault(idx, []).append(t)

    summary = {
        'total_trades': n,
        'total_pnl': sum(pnl_values) if pnl_values else 0,
        'avg_pnl': (sum(pnl_values) / len(pnl_values)) if pnl_values else 0,
        'winning_trades': len(winning),
        'losing_trades': len(losing),
        'win_rate_pct': (len(winning) / len(pnl_values) * 100) if pnl_values else 0,
        'avg_win': (sum(winning) / len(winning)) if winning else 0,
        'avg_loss': (sum(losing) / len(losing)) if losing else 0,
        'max_profit': max(pnl_values) if pnl_values else 0,
        'max_loss': min(pnl_values) if pnl_values else 0,
        'median_duration_seconds': median(durations) if durations else None,
        'avg_duration_seconds': (sum(durations) / len(durations)) if durations else None,
        'by_option_type': {k: len(v) for k, v in by_type.items()},
        'by_index_name': {k: len(v) for k, v in by_index.items()},
    }
    return summary


def main():
    if not DB_PATH.exists():
        print(f"DB not found at: {DB_PATH}")
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    trades = fetch_trades(conn)
    trades_out = [dict(t) for t in trades]

    # Normalize numeric fields to native types
    for t in trades_out:
        for k in ('pnl', 'entry_price', 'exit_price'):
            if t.get(k) is not None:
                try:
                    t[k] = float(t[k])
                except Exception:
                    pass

    summary = summarize(trades_out)

    report = {'summary': summary, 'count': len(trades_out)}

    out_trades = OUT_DIR / 'trades_export.json'
    out_report = OUT_DIR / 'trades_report.json'
    with open(out_trades, 'w', encoding='utf-8') as f:
        json.dump(trades_out, f, ensure_ascii=False, indent=2, default=str)
    with open(out_report, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print('Exported', len(trades_out), 'trades to', out_trades)
    print('Report saved to', out_report)
    print('\nSummary:')
    for k, v in summary.items():
        print(f" - {k}: {v}")

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
