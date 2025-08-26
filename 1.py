#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
from datetime import datetime
from dateutil import parser as dtparser
import time
import math
import json
import glob
from typing import List, Dict, Any

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use("Agg")  # 無頭環境繪圖
import matplotlib.pyplot as plt
import mplfinance as mpf
from tqdm import tqdm

# 資料來源
SOURCE_YF = "yfinance"
SOURCE_FINMIND = "finmind"

# 只抓 overlaps 裡這兩個 key
OV_KEYS_TW  = "單日_上市∩ZGB_1470∩ZGB_1650"  # → .TW
OV_KEYS_TWO = "單日_上櫃∩ZGB_1470∩ZGB_1650"  # → .TWO

def parse_args():
    p = argparse.ArgumentParser(
        description="Batch fetch OHLCV & draw K-line for symbols. Supports directory scanning for JSON (overlaps keys)."
    )
    # 來源：symbols / 檔案 / JSON 資料夾
    p.add_argument("--chart", choices=["candle", "line", "both"], default="candle",
               help="Chart type: candle (default), line, or both")
    p.add_argument("--symbols", nargs="*", help="Symbols, e.g., 2330.TW 5483.TWO AAPL")
    p.add_argument("--symbols-file", help="Text file with one symbol per line")
    p.add_argument("--json-dir", help="Directory containing JSON files with 'overlaps' keys")
    p.add_argument("--json-glob", default="*.json", help="Filename pattern under --json-dir (default: *.json)")
    p.add_argument("--json-latest", type=int, default=0,
                   help="Only use the latest N files by mtime from --json-dir (0 = use all)")

    # 抓取選項
    p.add_argument("--out", default="./out", help="Output dir (default: ./out)")
    p.add_argument("--period", default="1y",
                   help="Data period: 1mo,3mo,6mo,1y,2y,5y,max (exclusive with --start/--end)")
    p.add_argument("--interval", default="1d",
                   help="Interval: 1m,5m,15m,30m,60m,90m,1h,1d,1wk,1mo")
    p.add_argument("--start", help="Start date YYYY-MM-DD")
    p.add_argument("--end", help="End date YYYY-MM-DD (inclusive-ish)")
    p.add_argument("--source", choices=[SOURCE_YF, SOURCE_FINMIND], default=SOURCE_YF,
                   help="yfinance (default) or finmind (TW only)")
    p.add_argument("--finmind-token", help="FinMind token (required if --source finmind)")
    p.add_argument("--ma", nargs="*", type=int, default=[5, 20, 60],
                   help="Moving averages (default: 5 20 60)")
    p.add_argument("--retry", type=int, default=3, help="Retry times")
    p.add_argument("--sleep", type=float, default=0.8, help="Sleep seconds between requests")
    p.add_argument("--dpi", type=int, default=140, help="PNG dpi (default: 140)")
    p.add_argument("--style", default="yahoo", help="mplfinance style (default: yahoo)")
    return p.parse_args()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _read_symbols_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def _collect_symbols_from_overlaps_json_file(fp: str) -> List[str]:
    """
    僅讀 overlaps 裡兩個 key：
      OV_KEYS_TW  → .TW
      OV_KEYS_TWO → .TWO
    若 JSON 裡代號已帶 .TW/.TWO 則保留。
    """
    try:
        with open(fp, "r", encoding="utf-8") as f:
            j = json.load(f)
    except Exception as e:
        print(f"[WARN] skip {fp}: {e}", file=sys.stderr)
        return []

    ov = (j or {}).get("overlaps") or {}
    out: List[str] = []

    # 上市 (.TW)
    for it in ov.get(OV_KEYS_TW, []):
        code = str((it or {}).get("代號", "")).strip()
        if not code:
            continue
        if code.endswith((".TW", ".TWO")):
            out.append(code)
        else:
            out.append(f"{code}.TW")

    # 上櫃 (.TWO)
    for it in ov.get(OV_KEYS_TWO, []):
        code = str((it or {}).get("代號", "")).strip()
        if not code:
            continue
        if code.endswith((".TW", ".TWO")):
            out.append(code)
        else:
            out.append(f"{code}.TWO")

    return out

def _collect_symbols_from_json_dir(json_dir: str, pattern: str, latest: int) -> List[str]:
    if not os.path.isdir(json_dir):
        print(f"[WARN] --json-dir not found: {json_dir}", file=sys.stderr)
        return []
    paths = sorted(glob.glob(os.path.join(json_dir, pattern)))
    if not paths:
        return []
    # 取最新 N 個
    if latest and latest > 0:
        paths = sorted(paths, key=lambda p: os.path.getmtime(p), reverse=True)[:latest]

    symbols: List[str] = []
    for fp in paths:
        symbols.extend(_collect_symbols_from_overlaps_json_file(fp))

    # 去重保序
    seen = set()
    deduped = []
    for s in symbols:
        if s and s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped

def load_symbols(args) -> List[str]:
    syms: List[str] = []

    # 1) JSON 資料夾
    if args.json_dir:
        syms.extend(_collect_symbols_from_json_dir(args.json_dir, args.json_glob, args.json_latest))

    # 2) 直接 symbols / 檔案
    if args.symbols:
        syms.extend(args.symbols)
    if args.symbols_file:
        syms.extend(_read_symbols_file(args.symbols_file))

    # 清理 + 去重保序
    syms = [s.strip() for s in syms if s and s.strip()]
    syms = list(dict.fromkeys(syms))
    if not syms:
        print("No symbols found. Provide --json-dir or --symbols/--symbols-file.", file=sys.stderr)
        sys.exit(2)
    return syms

def _fetch_yfinance(symbol, start, end, period, interval, retry=3, sleep=0.8):
    import yfinance as yf
    last_err = None
    for i in range(retry):
        try:
            if start:
                df = yf.download(symbol, start=start, end=end, interval=interval, progress=False, auto_adjust=False)
            else:
                df = yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=False)
            if isinstance(df, pd.DataFrame) and not df.empty:
                df = df.rename(columns=str.title)  # 統一欄位
            return df
        except Exception as e:
            last_err = e
            time.sleep(sleep * (i + 1))
    raise RuntimeError(f"yfinance fetch failed for {symbol}: {last_err}")

def _fetch_finmind(symbol, start, end, period, interval, token, retry=3, sleep=0.8):
    # FinMind 僅示範日K
    if token is None:
        raise ValueError("FinMind requires --finmind-token.")
    if not (symbol.endswith(".TW") or symbol.endswith(".TWO")):
        raise ValueError("FinMind only supports TW/TWO symbols, e.g., 2330.TW / 5483.TWO")
    if interval.lower() != "1d":
        raise ValueError("FinMind demo supports daily interval only (use --interval 1d).")

    from FinMind.data import DataLoader
    dl = DataLoader()
    dl.login_by_token(api_token=token)

    tw_code = symbol.replace(".TW", "").replace(".TWO", "")
    if not start:
        period_map = {
            "1mo": 30, "3mo": 92, "6mo": 183, "1y": 365, "2y": 365*2,
            "3y": 365*3, "5y": 365*5, "max": 365*20
        }
        days = period_map.get(period or "1y", 365)
        start = (datetime.today() - pd.Timedelta(days=days)).strftime("%Y-%m-%d")

    last_err = None
    for i in range(retry):
        try:
            df = dl.taiwan_stock_daily(stock_id=tw_code, start_date=start, end_date=end)
            if not df.empty:
                df = df.rename(columns={
                    "date": "Date",
                    "open": "Open",
                    "max": "High",
                    "min": "Low",
                    "close": "Close",
                    "volume": "Volume"
                })
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.set_index("Date").sort_index()
                return df[["Open", "High", "Low", "Close", "Volume"]]
        except Exception as e:
            last_err = e
            time.sleep(sleep * (i + 1))
    raise RuntimeError(f"FinMind fetch failed for {symbol}: {last_err}")

def fetch_ohlcv(symbol, args) -> pd.DataFrame:
    start = args.start
    end = args.end
    # 參數驗證
    if start:
        try:
            dtparser.parse(start)
        except Exception:
            raise ValueError(f"Invalid --start: {start}")
    if end:
        try:
            dtparser.parse(end)
        except Exception:
            raise ValueError(f"Invalid --end: {end}")
    period = None if start else (args.period or "1y")

    if args.source == SOURCE_YF:
        df = _fetch_yfinance(symbol, start, end, period or "1y", args.interval,
                             retry=args.retry, sleep=args.sleep)
    else:
        df = _fetch_finmind(symbol, start, end, period or "1y", args.interval,
                            token=args.finmind_token, retry=args.retry, sleep=args.sleep)
    if df is None or df.empty:
        raise RuntimeError(f"No data for {symbol}")
    # 清理
    df = df.replace([np.inf, -np.inf], np.nan).dropna(how="any")
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in df.columns:
            raise RuntimeError(f"Missing column {col} for {symbol}")
    return df

def enrich_metrics(df: pd.DataFrame, ma_list=(5,20,60)) -> pd.DataFrame:
    out = df.copy()
    for m in ma_list:
        out[f"MA{m}"] = out["Close"].rolling(m, min_periods=1).mean()
    out["Return_1d"] = out["Close"].pct_change(1)
    return out

def draw_kline(symbol, df: pd.DataFrame, outdir: str, dpi=140, style="yahoo", ma_list=(5,20,60), chart_type="candle"):
    ensure_dir(outdir)

    def _save_line_chart():
        fig = plt.figure(figsize=(14, 6))
        ax1 = fig.add_subplot(211)
        ax1.plot(df.index, df["Close"], label="Close")
        # 疊加均線
        for m in [m for m in ma_list if isinstance(m, int) and m > 0]:
            col = f"MA{m}"
            if col in df.columns:
                ax1.plot(df.index, df[col], label=col)
        ax1.set_title(f"{symbol} Close (Line) + MA")
        ax1.grid(True, linestyle="--", alpha=0.3)
        ax1.legend(loc="best")

        ax2 = fig.add_subplot(212, sharex=ax1)
        ax2.bar(df.index, df["Volume"])
        ax2.set_title("Volume")
        ax2.grid(True, linestyle="--", alpha=0.3)

        fig.tight_layout()
        fig.savefig(os.path.join(outdir, f"{symbol}_line_volume.png"), dpi=dpi)
        plt.close(fig)

    def _save_candle_chart():
        mav = [m for m in ma_list if isinstance(m, int) and m > 0]
        addp = [mpf.make_addplot(df[f"MA{m}"]) for m in mav if f"MA{m}" in df.columns]
        mpf.plot(df,
                 type="candle",
                 style=style,
                 volume=True,
                 addplot=addp if addp else None,
                 figsize=(14, 7),
                 tight_layout=True,
                 title=f"{symbol} K-line with Volume & MA",
                 savefig=dict(fname=os.path.join(outdir, f"{symbol}_kline.png"), dpi=dpi))

    if chart_type in ("line", "both"):
        _save_line_chart()
    if chart_type in ("candle", "both"):
        _save_candle_chart()

    mpf.plot(df,
             type="candle",
             style=style,
             volume=True,
             addplot=addp if addp else None,
             figsize=(14, 7),
             tight_layout=True,
             title=f"{symbol} K-line with Volume & MA",
             savefig=dict(fname=os.path.join(outdir, f"{symbol}_kline.png"), dpi=dpi))

    # 收盤 + 量能
    fig = plt.figure(figsize=(14, 5))
    ax1 = fig.add_subplot(211)
    ax1.plot(df.index, df["Close"])
    ax1.set_title(f"{symbol} Close")
    ax1.grid(True, linestyle="--", alpha=0.3)

    ax2 = fig.add_subplot(212, sharex=ax1)
    ax2.bar(df.index, df["Volume"])
    ax2.set_title("Volume")
    ax2.grid(True, linestyle="--", alpha=0.3)

    fig.tight_layout()
    fig.savefig(os.path.join(outdir, f"{symbol}_close_volume.png"), dpi=dpi)
    plt.close(fig)

def save_csv(symbol, df: pd.DataFrame, outdir: str):
    ensure_dir(outdir)
    df.to_csv(os.path.join(outdir, f"{symbol}.csv"), encoding="utf-8")

def summarize(df: pd.DataFrame) -> dict:
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None
    last_close = float(last["Close"])
    change = float(last_close - float(prev["Close"])) if prev is not None else float("nan")
    pct = float(change / float(prev["Close"]) * 100.0) if prev is not None and prev["Close"] else float("nan")
    vol = int(last["Volume"]) if not math.isnan(last["Volume"]) else None
    return {
        "last_date": df.index[-1].strftime("%Y-%m-%d"),
        "close": last_close,
        "change": change,
        "change_pct": pct,
        "volume": vol
    }

def main():
    args = parse_args()
    symbols = load_symbols(args)
    ensure_dir(args.out)

    results = []
    for sym in tqdm(symbols, desc="Fetching"):
        try:
            df = fetch_ohlcv(sym, args)
            df = enrich_metrics(df, args.ma)
            save_csv(sym, df, args.out)
            draw_kline(sym, df, args.out, dpi=args.dpi, style=args.style, ma_list=args.ma, chart_type=args.chart)
            info = summarize(df)
            info["symbol"] = sym
            results.append(info)
            time.sleep(args.sleep)
        except Exception as e:
            print(f"[ERR] {sym}: {e}", file=sys.stderr)

    if results:
        summary_df = pd.DataFrame(results)[["symbol", "last_date", "close", "change", "change_pct", "volume"]]
        summary_df.to_csv(os.path.join(args.out, "summary_latest.csv"), index=False, encoding="utf-8")
        print(f"\nDone. Files saved in: {os.path.abspath(args.out)}")
        print(summary_df.to_string(index=False))
    else:
        print("No successful results.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
