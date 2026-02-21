#!/usr/bin/env python3
"""Filter trades by specific YYYY-MM-DD dates and compute analytics.

Usage: python3 scripts/analyze_trades_by_dates.py 2026-02-17,2026-02-18,2026-02-19
"""
import sqlite3
import json
import sys
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
        return datetime.fromisoformat(s)
    except Exception:
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
    for t in trades:
        opt = t.get('option_type') or 'UNKNOWN'
        by_type.setdefault(opt, []).append(t)

    return {
        'count': len(trades),
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
    }


def filter_by_dates(trades, dates):
    dateset = set(dates)
    out = []
    for t in trades:
        et = parse_iso(t.get('entry_time'))
        xt = parse_iso(t.get('exit_time'))
        added = False
        for dt in (et, xt):
            if dt is None:
                continue
            if dt.date().isoformat() in dateset:
                out.append(t)
                added = True
                break
        if not added:
            # also check created_at if present
            ca = parse_iso(t.get('created_at'))
            if ca and ca.date().isoformat() in dateset:
                out.append(t)
    return out


def main():
    if len(sys.argv) < 2:
        print('Usage: python3 scripts/analyze_trades_by_dates.py YYYY-MM-DD,YYYY-MM-DD,...')
        return 2

    dates = [d.strip() for d in sys.argv[1].split(',') if d.strip()]
    if not dates:
        print('No dates provided')
        return 2

    if not DB_PATH.exists():
        print(f'DB not found at: {DB_PATH}')
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    trades = fetch_trades(conn)

    filtered = filter_by_dates(trades, dates)
    # Normalize numeric fields
    for t in filtered:
        for k in ('pnl', 'entry_price', 'exit_price'):
            if t.get(k) is not None:
                try:
                    t[k] = float(t[k])
                except Exception:
                    pass

    per_day = {}
    for d in dates:
        per_day[d] = [t for t in filtered if (parse_iso(t.get('entry_time')) and parse_iso(t.get('entry_time')).date().isoformat() == d) or (parse_iso(t.get('exit_time')) and parse_iso(t.get('exit_time')).date().isoformat() == d) or (parse_iso(t.get('created_at')) and parse_iso(t.get('created_at')).date().isoformat() == d)]

    report = {'requested_dates': dates, 'total_filtered_trades': len(filtered), 'per_day': {}, 'combined': {}}
    for d, ts in per_day.items():
        report['per_day'][d] = summarize(ts)

    report['combined'] = summarize(filtered)

    out_trades = OUT_DIR / f"trades_filtered_{'_'.join(dates)}.json"
    out_report = OUT_DIR / f"trades_report_{'_'.join(dates)}.json"
    with open(out_trades, 'w', encoding='utf-8') as f:
        json.dump([dict(t) for t in filtered], f, ensure_ascii=False, indent=2, default=str)
    with open(out_report, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print('Filtered trades:', len(filtered))
    for d in dates:
        pd = report['per_day'].get(d) or {}
        print(f"Date {d}: {pd.get('count',0)} trades, total_pnl={pd.get('total_pnl',0)}")

    print('\nCombined:')
    for k, v in report['combined'].items():
        print(f" - {k}: {v}")

    print('\nSaved:')
    print(' -', out_trades)
    print(' -', out_report)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
