#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fubon E-broker scraper (強化修正版：保證抓到全部 代號/名稱)
- 單日_上市: https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_0_1.djhtm
- 單日_上櫃: https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_1_1.djhtm
- ZGB_1470/1650 (自動帶入台灣當日 e=f): https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm

策略：
1) 先從所有含 onclick="GenLink2stk('AS####')" 的元素抓代號，元素文字做為名稱（這是最穩的方式，頁面大量使用此連結）
2) 若沒有，再嘗試從 JS 片段/表格回退抽取
3) 存檔前強制只留兩欄 (代號, 名稱)
"""
import argparse
import datetime as dt
import re
from io import StringIO
from typing import List, Tuple, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
import json

DD_URLS = [
    ("單日_上市", "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_0_1.djhtm"),
    ("單日_上櫃", "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_1_1.djhtm"),
]
ZGB_BASE = "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm"
ZGB_CODES = [
    ("ZGB_1470", {"a": "1470", "b": "1470", "c": "B"}),
    ("ZGB_1650", {"a": "1650", "b": "1650", "c": "B"}),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
}

def _tw_today():
    if ZoneInfo is not None:
        tz = ZoneInfo("Asia/Taipei")
        now = dt.datetime.now(tz)
    else:
        now = dt.datetime.now()
    return now.date()

def format_fubon_date(d: dt.date) -> str:
    return f"{d.year}-{d.month}-{d.day}"

def build_zgb_url(params: dict, d: dt.date) -> str:
    e = f = format_fubon_date(d)
    p = {**params, "e": e, "f": f}
    qs = "&".join([f"{k}={v}" for k, v in p.items()])
    return f"{ZGB_BASE}?{qs}"

def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.6,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET", "HEAD"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def fetch_html(url: str, session: requests.Session) -> str:
    r = session.get(url, timeout=20)
    encs = ["big5-hkscs", "big5", "cp950", r.apparent_encoding, r.encoding, "utf-8"]
    for enc in [e for e in encs if e]:
        try:
            r.encoding = enc
            html = r.text
            if "�" not in html and len(html) > 200:
                return html
        except Exception:
            continue
    r.encoding = "big5"
    return r.text

RE_ONCLICK = re.compile(r"GenLink2stk\('AS(\d{4})'")

def extract_from_onclick(html: str) -> Optional[pd.DataFrame]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for node in soup.find_all(onclick=RE_ONCLICK):
        m = RE_ONCLICK.search(node.get("onclick", ""))
        if not m:
            continue
        code = m.group(1)
        name = node.get_text(strip=True)
        # 過濾空值與顯然不是名稱的文字
        if not name:
            continue
        # 名稱若以代號開頭，嘗試去掉代號
        name = re.sub(rf"^{code}\s*", "", name)
        rows.append((code, name))
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["代號", "名稱"]).drop_duplicates()
    return df

RE_CODEJS = re.compile(r"GenLink2stk\('[^']*?(\d{4})'\s*,\s*'([^']+)'\)", re.IGNORECASE)
def extract_from_js(html: str) -> Optional[pd.DataFrame]:
    items = RE_CODEJS.findall(html)
    if not items:
        return None
    rows = [(code, name.strip()) for code, name in items if name.strip()]
    if not rows:
        return None
    return pd.DataFrame(rows, columns=["代號","名稱"]).drop_duplicates()

RE_CODE = re.compile(r"(?<!\d)(\d{4})(?!\d)")
RE_CODE_NAME = re.compile(r"(?<!\d)(?P<code>\d{4})(?!\d)\s*[，,\s]*\s*(?P<name>[\u4e00-\u9fffA-Za-z0-9\-\._]+)")
def extract_from_tables(html: str) -> Optional[pd.DataFrame]:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    candidates: List[pd.DataFrame] = []
    for t in tables:
        try:
            df = pd.read_html(StringIO(str(t)))[0]
            if not df.empty:
                candidates.append(df)
        except Exception:
            continue
    if not candidates:
        return None

    # 1) 有代號/名稱欄位
    for df in candidates:
        cols = [str(c).strip() for c in df.columns]
        has_code = any("代號" in c or "證券代號" in c for c in cols)
        has_name = any("名稱" in c or "股票名稱" in c or "證券名稱" in c for c in cols)
        if has_code and has_name:
            df.columns = cols
            colmap = {}
            for c in cols:
                if "代號" in c or "證券代號" in c: colmap["代號"] = c
                if "名稱" in c or "股票名稱" in c or "證券名稱" in c: colmap["名稱"] = c
            out = df.rename(columns={v:k for k,v in colmap.items()})
            out = out[["代號","名稱"]]
            out["代號"] = out["代號"].astype(str).str.extract(RE_CODE)
            out["名稱"] = out["名稱"].astype(str).str.replace(r"\s+", "", regex=True)
            out = out.dropna().drop_duplicates()
            if len(out)>0:
                return out.reset_index(drop=True)

    # 2) 正則掃描所有欄位文字
    rows = []
    for df in candidates:
        cols = [str(c).strip() for c in df.columns]
        df.columns = cols
        for _, row in df.iterrows():
            text = " ".join(str(v) for v in row.values)
            m = RE_CODE_NAME.search(text)
            if m:
                rows.append((m.group("code"), m.group("name")))
    if rows:
        return pd.DataFrame(rows, columns=["代號","名稱"]).drop_duplicates().reset_index(drop=True)
    return None

def parse_codes(html: str) -> pd.DataFrame:
    for extractor in (extract_from_onclick, extract_from_js, extract_from_tables):
        df = extractor(html)
        if df is not None and len(df)>0:
            return df
    raise ValueError("無法抽出代號/名稱")

def scrape_codes(name: str, url: str, session: requests.Session):
    html = fetch_html(url, session)
    df = parse_codes(html)
    # 最終只留兩欄
    df = df[["代號","名稱"]].copy()
    df["代號"] = df["代號"].astype(str).str.extract(RE_CODE)
    df["名稱"] = df["名稱"].astype(str).str.replace(r"\s+", "", regex=True)
    df = df.dropna().drop_duplicates().reset_index(drop=True)
    return name, df, url

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=str, default=".", help="輸出資料夾")
    args = ap.parse_args()

    session = _requests_session()
    today_tw = _tw_today()

    targets = []
    for n, url in DD_URLS:
        targets.append((n, url))
    for n, params in ZGB_CODES:
        targets.append((n, build_zgb_url(params, today_tw)))

    results = []
    for name, url in targets:
        try:
            n, df, url = scrape_codes(name, url, session)
            results.append((n, df, url))
            print(f"[OK] {n} rows={len(df)} url={url}")
            print(df.head().to_string(index=False))
        except Exception as e:
            print(f"[ERR] {name}: {e} | url={url}")

    # ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    # combined = None
    # for name, df, url in results:
    #     df = df[["代號","名稱"]]
    #     safe = re.sub(r"[^0-9A-Za-z_一-龥]+", "_", name)
    #     csv_path = f"{args.out}/fubon_{safe}_{ts}.csv"
    #     df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    #     print(f"Saved: {csv_path}")
    #     df2 = df.copy()
    #     df2.insert(0, "來源", name)
    #     combined = pd.concat([combined, df2], ignore_index=True) if combined is not None else df2

    output = {}
    for name, df, url in results:
        df = df[["代號","名稱"]]
        records = df.to_dict(orient="records")
        output[name] = records

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = f"{args.out}/fubon_{ts}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Saved JSON: {json_path}")


    if combined is not None:
        combo_path = f"{args.out}/fubon_combined_{ts}.csv"
        combined.to_csv(combo_path, index=False, encoding="utf-8-sig")
        print(f"Saved combined: {combo_path}")

if __name__ == "__main__":
    main()
