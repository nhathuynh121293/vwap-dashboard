"""
build_percentiles.py — Build / Refresh VWAP(9:00-9:30) Percentile Zones
=========================================================================
Chạy incremental: chỉ fetch ngày mới chưa có, append vào CSV.

Logic:
  1. Fetch 1m CSVs — incremental append (dùng lại fetch_vietstock.py)
  2. Mỗi ngày giao dịch: tính VWAP(9:00-9:30)
  3. Tính max_up% và max_down% từ VWAP suốt phiên (9:31 → 14:45)
     dùng cột high/low để capture đúng biên độ
  4. Percentile p25/p50/p75/p90/p95 trên ROLLING_DAYS ngày gần nhất
  5. Ghi data/percentiles.json

Usage:
    python build_percentiles.py
    python build_percentiles.py --symbols ACB,HPG
    python build_percentiles.py --days 365    ← chỉ dùng khi init lần đầu
    python build_percentiles.py --no-fetch    ← recompute thôi, không gọi API
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone, timedelta, time as dtime
from pathlib import Path

import numpy as np

# ── Import fetch logic từ fetch_vietstock.py ────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
try:
    from fetch_vietstock import (
        process_symbol as _fetch_symbol,
        load_existing,
        VN_TZ,
    )
except ImportError as e:
    print(f"❌ Không import được fetch_vietstock.py: {e}")
    print("   Đảm bảo fetch_vietstock.py nằm cùng thư mục.")
    sys.exit(1)

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_DIR  = Path(__file__).parent / "data"
PERC_FILE = DATA_DIR / "percentiles.json"

# ── HOSE session windows ─────────────────────────────────────────────────────
VWAP_START  = dtime(9, 0)
VWAP_END    = dtime(9, 30)
SESSION_END = dtime(14, 45)

# ── Watchlist ────────────────────────────────────────────────────────────────
WATCHLIST = [
    "ACB", "BID", "CTG", "DGC", "FPT", "GAS", "GVR", "HDB", "HPG", "LPB",
    "MBB", "MSN", "MWG", "PLX", "SAB", "SHB", "SSI", "STB", "TCB",
    "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE",
    "MCH", "TCX", "BSR", "POW", "KBC", "REE", "GEX", "VCI", "GMD", "PVS",
    # Phái sinh
    "VN30F1M",
]

# ── Outlier filter: loại ngày circuit breaker / data lỗi ────────────────────
MAX_MOVE_PCT = 8.0    # % — phù hợp HOSE circuit breaker ±7% (was 15.0)

# ── Rolling window ────────────────────────────────────────────────────────────
ROLLING_DAYS = 252    # Chỉ dùng 252 ngày giao dịch gần nhất để tính percentile


# ── Helpers ──────────────────────────────────────────────────────────────────

def bar_time(ts: int) -> dtime:
    """Unix timestamp → VN local time-of-day."""
    return datetime.fromtimestamp(ts, tz=VN_TZ).time()


def bar_date(ts: int) -> str:
    """Unix timestamp → 'YYYY-MM-DD' (VN timezone)."""
    return datetime.fromtimestamp(ts, tz=VN_TZ).strftime("%Y-%m-%d")


# ── Core computation ──────────────────────────────────────────────────────────

def compute_percentiles_for_symbol(symbol: str) -> dict | None:
    """
    Đọc CSV đã có, tính percentile VWAP zones.

    Returns:
        {
          "up":   {"p25": ..., "p50": ..., "p75": ..., "p90": ..., "p95": ...},
          "down": {"p25": ..., "p50": ..., "p75": ..., "p90": ..., "p95": ...},
          "n_days": int,
        }
        hoặc None nếu không đủ data.
    """
    rows, _ = load_existing(symbol)
    if not rows:
        return None

    # ── Group bars theo ngày ─────────────────────────────────────────────
    days: dict[str, list[dict]] = {}
    for bar in rows:
        days.setdefault(bar_date(bar["ts"]), []).append(bar)

    up_pcts: list[float] = []
    dn_pcts: list[float] = []

    # ── Rolling window: chỉ giữ ROLLING_DAYS ngày giao dịch gần nhất ────
    sorted_day_items = sorted(days.items())
    if len(sorted_day_items) > ROLLING_DAYS:
        sorted_day_items = sorted_day_items[-ROLLING_DAYS:]

    for date_str, bars in sorted_day_items:
        vwap_bars:    list[dict] = []
        session_bars: list[dict] = []

        for bar in bars:
            t = bar_time(bar["ts"])
            if VWAP_START <= t <= VWAP_END:
                vwap_bars.append(bar)
            elif VWAP_END < t <= SESSION_END:
                session_bars.append(bar)

        # Cần đủ cả hai window (≥ 15 bars VWAP để đảm bảo chất lượng)
        if len(vwap_bars) < 15 or not session_bars:
            continue

        # ── VWAP(9:00-9:30): close * volume ──────────────────────────────
        total_vol = sum(b["volume"] for b in vwap_bars)
        if total_vol > 0:
            vwap = sum(b["close"] * b["volume"] for b in vwap_bars) / total_vol
        else:
            vwap = sum(b["close"] for b in vwap_bars) / len(vwap_bars)

        if vwap <= 0:
            continue

        # ── Max up/down từ VWAP trong phiên: dùng high/low ───────────────
        session_high = max(b["high"] for b in session_bars)
        session_low  = min(b["low"]  for b in session_bars)

        up_pct = (session_high - vwap) / vwap * 100
        dn_pct = (vwap - session_low)  / vwap * 100

        # ── Lọc outlier ──────────────────────────────────────────────────
        if up_pct < 0 or dn_pct < 0:
            continue
        if up_pct > MAX_MOVE_PCT or dn_pct > MAX_MOVE_PCT:
            continue

        up_pcts.append(up_pct)
        dn_pcts.append(dn_pct)

    if len(up_pcts) < 20:
        return None

    up = np.array(up_pcts)
    dn = np.array(dn_pcts)

    return {
        "up": {
            "p25": round(float(np.percentile(up, 25)), 4),
            "p50": round(float(np.percentile(up, 50)), 4),
            "p75": round(float(np.percentile(up, 75)), 4),
            "p90": round(float(np.percentile(up, 90)), 4),
            "p95": round(float(np.percentile(up, 95)), 4),
        },
        "down": {
            "p25": round(float(np.percentile(dn, 25)), 4),
            "p50": round(float(np.percentile(dn, 50)), 4),
            "p75": round(float(np.percentile(dn, 75)), 4),
            "p90": round(float(np.percentile(dn, 90)), 4),
            "p95": round(float(np.percentile(dn, 95)), 4),
        },
        "n_days": len(up_pcts),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build VWAP Percentile Zones")
    parser.add_argument(
        "--symbols", type=str, default="",
        help="Comma-separated symbols (default: WATCHLIST)"
    )
    parser.add_argument(
        "--days", type=int, default=365,
        help="Số ngày lịch sử khi init CSV lần đầu (default: 365)"
    )
    parser.add_argument(
        "--no-fetch", action="store_true",
        help="Bỏ qua fetch API, chỉ recompute percentiles từ CSV hiện có"
    )
    args = parser.parse_args()

    symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols else WATCHLIST
    )

    print("=" * 60)
    print("  BUILD PERCENTILES")
    print("=" * 60)
    print(f"  Symbols  : {len(symbols)}")
    print(f"  Rolling  : {ROLLING_DAYS} ngày giao dịch gần nhất")
    print(f"  Init days: {args.days}  (chỉ dùng khi CSV chưa có)")
    print(f"  Max move : {MAX_MOVE_PCT}%  (outlier filter)")
    print(f"  Output   : {PERC_FILE}")
    print("=" * 60)

    # ── Step 1: Cập nhật CSVs (incremental) ─────────────────────────────
    if not args.no_fetch:
        print(f"\n[1/2] Cập nhật 1m CSVs ({len(symbols)} symbols)...\n")
        t0 = time.time()
        fetch_ok = fetch_fail = 0

        for i, sym in enumerate(symbols, 1):
            print(f"[{i:>2}/{len(symbols)}]", end=" ")
            try:
                _fetch_symbol(sym, days=args.days)
                fetch_ok += 1
            except KeyboardInterrupt:
                print("\n  ⚠️  Interrupted.")
                break
            except Exception as e:
                print(f"  ❌ {sym}: {e}")
                fetch_fail += 1

        elapsed = time.time() - t0
        print(f"\n  Fetch xong: {fetch_ok} OK  |  {fetch_fail} lỗi  |  {elapsed:.1f}s")
    else:
        print("\n[1/2] --no-fetch: bỏ qua bước gọi API\n")

    # ── Step 2: Tính percentiles từ CSVs ────────────────────────────────
    print(f"\n[2/2] Tính percentiles ({len(symbols)} symbols)...\n")

    result: dict[str, dict] = {}
    ok = fail = 0

    for sym in symbols:
        perc = compute_percentiles_for_symbol(sym)
        if perc is None:
            print(f"  ⚠️  {sym:<6}  không đủ data (cần ≥ 20 ngày)")
            fail += 1
        else:
            result[sym] = perc
            print(
                f"  ✅ {sym:<6}  {perc['n_days']:>4} ngày  │  "
                f"up  p75={perc['up']['p75']:.2f}%  "
                f"p90={perc['up']['p90']:.2f}%  "
                f"p95={perc['up']['p95']:.2f}%  │  "
                f"dn  p75={perc['down']['p75']:.2f}%  "
                f"p90={perc['down']['p90']:.2f}%  "
                f"p95={perc['down']['p95']:.2f}%"
            )
            ok += 1

    # ── Ghi file ─────────────────────────────────────────────────────────
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(VN_TZ).isoformat(),
        "n_symbols":    ok,
        "symbols":      result,
    }
    with open(PERC_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"  ✅ {ok} symbols  │  ⚠️  {fail} skipped")
    print(f"  Saved → {PERC_FILE}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
