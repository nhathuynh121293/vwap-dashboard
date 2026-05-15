"""
fetch_vietstock.py — Tải 1m OHLCV từ Vietstock API
=====================================================
Chạy: python fetch_vietstock.py                       ← tự lấy VN30 từ SSI
      python fetch_vietstock.py --symbols ACB,HPG,FPT ← chỉ định symbols
      python fetch_vietstock.py --days 365
      python fetch_vietstock.py --symbols ACB --days 30

Logic:
  1. Lấy danh sách VN30 từ SSI iBoard API (trừ khi --symbols được chỉ định)
  2. Với mỗi symbol:
     - File chưa có → backward paginate từ hôm nay về `days` ngày trước
     - File đã có   → append từ max_ts+1m đến hôm nay (incremental)

Output: data/vietstock/{SYMBOL}_1m.csv
"""

import argparse
import csv
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ── Cấu hình ────────────────────────────────────────────────────────────────

# Fallback nếu SSI API lỗi
FALLBACK_SYMBOLS = [
    "ACB", "DGC", "FPT", "HPG", "MBB",
    "MSN", "MWG", "SHB", "STB", "TCB",
    "VHM", "VIB", "VNM", "VPB", "SSI",
]

# SSI iBoard — lấy danh sách VN30
SSI_VN30_URL = "https://iboard-query.ssi.com.vn/stock/group/VN30"
SSI_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi",
    "device-id": "F41381ED-9B57-482E-B983-4E02F1E189C6",
    "origin": "https://iboard.ssi.com.vn",
    "referer": "https://iboard.ssi.com.vn/",
    "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "x-device-name": "Chrome",
    "x-os-name": "Windows",
}

DEFAULT_DAYS      = 365       # Lấy bao nhiêu ngày lịch sử khi init
COUNTBACK         = 2000      # Số bars mỗi request (Vietstock max ~2000)
REQUEST_DELAY     = 0.5       # Giây giữa các request
REQUEST_TIMEOUT   = 20        # Timeout mỗi request
MAX_EMPTY_PAGES   = 3         # Dừng nếu N request liên tiếp trả 0 bars
OUTPUT_DIR        = Path(__file__).parent / "data" / "vietstock"

VN_TZ = timezone(timedelta(hours=7))

API_URL = "https://api.vietstock.vn/tvnew/history"
HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Origin": "https://stockchart.vietstock.vn",
    "Referer": "https://stockchart.vietstock.vn/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

CSV_FIELDS = ["ts", "open", "high", "low", "close", "volume"]


# ── SSI VN30 Symbol Fetcher ───────────────────────────────────────────────────

def fetch_vn30_symbols() -> list[str]:
    """
    Lấy danh sách symbol VN30 từ SSI iBoard API.
    Returns sorted list of stock symbols (bỏ qua index/ETF).
    Fallback về FALLBACK_SYMBOLS nếu API lỗi.
    """
    print("  Đang lấy danh sách VN30 từ SSI iBoard...")
    try:
        r = requests.get(
            SSI_VN30_URL,
            headers=SSI_HEADERS,
            timeout=10,
        )
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}")

        data = r.json()

        # SSI trả về list hoặc dict có key data/items
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("data") or data.get("items") or data.get("result") or []
        else:
            items = []

        symbols = []
        for item in items:
            sym = (
                item.get("stockSymbol")
                or item.get("symbol")
                or item.get("StockSymbol")
                or item.get("Symbol")
                or ""
            )
            sym = sym.strip().upper()
            # Bỏ qua index (VN30, VNI...) và ETF (E1V...) — chỉ lấy stock 3 ký tự
            if sym and len(sym) == 3 and sym.isalpha():
                symbols.append(sym)

        if not symbols:
            raise ValueError("Không parse được symbols từ response")

        symbols = sorted(set(symbols))
        print(f"  ✅ SSI VN30: {len(symbols)} symbols — {', '.join(symbols)}")
        return symbols

    except Exception as e:
        print(f"  ⚠️  SSI API lỗi ({e}) → dùng fallback list ({len(FALLBACK_SYMBOLS)} symbols)")
        return FALLBACK_SYMBOLS


# ── Helpers ──────────────────────────────────────────────────────────────────

def ts_to_vn(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=VN_TZ).strftime("%Y-%m-%d %H:%M")

def now_ts() -> int:
    return int(time.time())

def csv_path(symbol: str) -> Path:
    return OUTPUT_DIR / f"{symbol}_1m.csv"


# ── CSV I/O ──────────────────────────────────────────────────────────────────

def load_existing(symbol: str) -> tuple[list[dict], int | None]:
    """
    Đọc CSV hiện có.
    Returns: (rows, max_ts) hoặc ([], None) nếu chưa có file.
    """
    path = csv_path(symbol)
    if not path.exists():
        return [], None

    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "ts":     int(row["ts"]),
                "open":   float(row["open"]),
                "high":   float(row["high"]),
                "low":    float(row["low"]),
                "close":  float(row["close"]),
                "volume": int(float(row["volume"])),
            })

    if not rows:
        return [], None

    max_ts = max(r["ts"] for r in rows)
    return rows, max_ts


def save_csv(symbol: str, rows: list[dict]):
    """Lưu toàn bộ rows ra CSV (sort by ts, deduplicate)."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = csv_path(symbol)

    # Deduplicate + sort
    seen = {}
    for r in rows:
        seen[r["ts"]] = r
    sorted_rows = sorted(seen.values(), key=lambda x: x["ts"])

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(sorted_rows)

    return sorted_rows


# ── API fetch ────────────────────────────────────────────────────────────────

def fetch_one(symbol: str, from_ts: int, to_ts: int, countback: int) -> list[dict]:
    """
    Gọi 1 request Vietstock API.
    Returns list of bar dicts, empty list nếu lỗi hoặc no_data.
    """
    params = {
        "symbol":     symbol,
        "resolution": "1",
        "from":       from_ts,
        "to":         to_ts,
        "countback":  countback,
    }
    try:
        r = requests.get(
            API_URL, params=params, headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            print(f"    ⚠️  HTTP {r.status_code}")
            return []

        d = r.json()
        if d.get("s") != "ok":
            return []

        t_arr = d.get("t", [])
        o_arr = d.get("o", [])
        h_arr = d.get("h", [])
        l_arr = d.get("l", [])
        c_arr = d.get("c", [])
        v_arr = d.get("v", [])

        if not t_arr:
            return []

        bars = []
        for i in range(len(t_arr)):
            bars.append({
                "ts":     int(t_arr[i]),
                "open":   float(o_arr[i]),
                "high":   float(h_arr[i]),
                "low":    float(l_arr[i]),
                "close":  float(c_arr[i]),
                "volume": int(float(v_arr[i])),
            })
        return bars

    except Exception as e:
        print(f"    ❌ Request error: {e}")
        return []


# ── Fetch strategies ─────────────────────────────────────────────────────────

CHUNK_DAYS = 30  # Vietstock API chỉ trả data đúng khi window <= ~30 ngày


def fetch_backward(symbol: str, target_from_ts: int) -> list[dict]:
    """
    Backward pagination từ now về target_from_ts.
    Chunk theo CHUNK_DAYS để tránh API bỏ qua `from` khi window quá lớn.

    Ví dụ với 365 ngày:
      Chunk 1: from=(now-30d),   to=now
      Chunk 2: from=(now-60d),   to=(now-30d)-60
      Chunk 3: from=(now-90d),   to=(now-60d)-60
      ...
    """
    all_bars: list[dict] = []
    chunk_secs = CHUNK_DAYS * 86400
    to = now_ts()
    empty_streak = 0
    page = 1

    print(f"  Init: backward từ {ts_to_vn(target_from_ts)} → now")
    print(f"  Chunk size: {CHUNK_DAYS} ngày/request")

    while to > target_from_ts:
        # Window mỗi chunk: tối đa CHUNK_DAYS, nhưng không vượt target
        chunk_from = max(to - chunk_secs, target_from_ts)

        bars = fetch_one(symbol, from_ts=chunk_from, to_ts=to, countback=COUNTBACK)

        if not bars:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES:
                print(f"    Dừng: {MAX_EMPTY_PAGES} chunk liên tiếp trống")
                break
            # Lùi thêm dù không có data (có thể khoảng nghỉ lễ dài)
            to = chunk_from - 60
            time.sleep(REQUEST_DELAY)
            continue

        empty_streak = 0
        first_ts = bars[0]["ts"]
        last_ts  = bars[-1]["ts"]

        # Lọc đúng range
        bars = [b for b in bars if b["ts"] >= target_from_ts]
        all_bars.extend(bars)

        print(
            f"  Chunk {page:>3}: {len(bars):>5} bars | "
            f"{ts_to_vn(first_ts)} → {ts_to_vn(last_ts)} | "
            f"Total: {len(all_bars):,}"
        )

        # Bước lui: dùng chunk_from thay vì first_ts (tránh miss khoảng trống)
        to = chunk_from - 60
        page += 1
        time.sleep(REQUEST_DELAY)

    return all_bars


def fetch_forward(symbol: str, from_ts: int) -> list[dict]:
    """
    Forward fill: lấy bars từ from_ts đến now.
    Dùng khi append — thường chỉ vài ngày → ít request.
    """
    to = now_ts()
    if from_ts >= to:
        return []

    # Số bars cần (trading hours ~270 bars/ngày, nhưng lấy thoải mái)
    needed = min((to - from_ts) // 60 + 100, COUNTBACK)

    all_bars: list[dict] = []
    current_from = from_ts
    page = 1

    print(f"  Append: từ {ts_to_vn(from_ts)} → now")

    while current_from < to:
        bars = fetch_one(symbol, from_ts=current_from, to_ts=to, countback=COUNTBACK)

        if not bars:
            break

        # Chỉ lấy bars mới hơn from_ts
        bars = [b for b in bars if b["ts"] > from_ts]
        if not bars:
            break

        all_bars.extend(bars)
        last_ts = bars[-1]["ts"]

        print(
            f"  Page {page:>3}: {len(bars):>5} bars | "
            f"{ts_to_vn(bars[0]['ts'])} → {ts_to_vn(last_ts)} | "
            f"New: {len(all_bars):,}"
        )

        # Nếu đã tới now hoặc ít bars hơn max → xong
        if len(bars) < COUNTBACK or last_ts >= to - 120:
            break

        current_from = last_ts
        page += 1
        time.sleep(REQUEST_DELAY)

    return all_bars


# ── Main ─────────────────────────────────────────────────────────────────────

def process_symbol(symbol: str, days: int):
    print(f"\n{'═'*55}")
    print(f"  {symbol}")
    print(f"{'═'*55}")

    existing_rows, max_ts = load_existing(symbol)

    if max_ts is None:
        # ── INIT: fetch 1 năm backward ──
        target_from = now_ts() - days * 86400
        new_bars = fetch_backward(symbol, target_from_ts=target_from)
        all_rows = existing_rows + new_bars
    else:
        # ── APPEND: fetch từ max_ts đến now ──
        lag_hours = (now_ts() - max_ts) / 3600
        print(f"  Có sẵn đến: {ts_to_vn(max_ts)} (lag: {lag_hours:.1f}h)")

        if lag_hours < 0.1:
            print("  ✅ Đã cập nhật, bỏ qua.")
            return

        new_bars = fetch_forward(symbol, from_ts=max_ts)
        all_rows = existing_rows + new_bars

    if not all_rows:
        print("  ⚠️  Không có data nào để lưu.")
        return

    saved = save_csv(symbol, all_rows)

    # Summary
    first_dt = ts_to_vn(saved[0]["ts"])
    last_dt  = ts_to_vn(saved[-1]["ts"])
    span_days = (saved[-1]["ts"] - saved[0]["ts"]) / 86400
    print(
        f"\n  ✅ Đã lưu {len(saved):,} bars | "
        f"{first_dt} → {last_dt} ({span_days:.0f} ngày)"
    )
    print(f"     → {csv_path(symbol)}")


def main():
    parser = argparse.ArgumentParser(description="Fetch Vietstock 1m OHLCV")
    parser.add_argument(
        "--symbols", type=str, default="",
        help="Comma-separated symbols. Nếu bỏ trống → tự lấy VN30 từ SSI"
    )
    parser.add_argument(
        "--days", type=int, default=DEFAULT_DAYS,
        help=f"Số ngày lịch sử khi init (default: {DEFAULT_DAYS})"
    )
    args = parser.parse_args()

    print("=" * 55)
    print("  VIETSTOCK 1m OHLCV FETCHER")
    print("=" * 55)
    print(f"  Init days : {args.days}")
    print(f"  Output    : {OUTPUT_DIR}/")
    print("=" * 55)

    # Lấy danh sách symbols
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        print(f"  Symbols (manual): {', '.join(symbols)}")
    else:
        print("\n[1/2] Lấy danh sách VN30 từ SSI...")
        symbols = fetch_vn30_symbols()

    if not symbols:
        print("  ❌ Không có symbols nào để fetch. Thoát.")
        return

    print(f"\n[2/2] Fetch 1m data cho {len(symbols)} symbols...\n")
    start = time.time()

    ok, skip, fail = 0, 0, 0
    for i, sym in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}]", end=" ")
        try:
            process_symbol(sym, days=args.days)
            # Check nếu bỏ qua (up-to-date)
            ok += 1
        except KeyboardInterrupt:
            print("\n\n  ⚠️  Đã dừng bởi người dùng.")
            break
        except Exception as e:
            print(f"  ❌ {sym} lỗi: {e}")
            fail += 1

    elapsed = time.time() - start
    print(f"\n{'═'*55}")
    print(f"  Xong. Thời gian: {elapsed:.1f}s")
    print(f"  OK: {ok} | Fail: {fail}")
    print(f"  Data: {OUTPUT_DIR}/")
    print(f"{'═'*55}")


if __name__ == "__main__":
    main()
