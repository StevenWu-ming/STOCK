#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fubon scraper (final)
- DD(單日_上市/上櫃): 抓最新
- ZGB(1470/1650): 只抓左欄『買超』，支援 --date (YYYY-M-D / YYYY-MM-DD 等)

輸出單一 JSON，包含：
- summary: 各組筆數、日期等
- data: 原始四組 [{代號, 名稱}]
- overlaps:
    - 上市∩1470∩1650
    - 上櫃∩1470∩1650
    python test.py --out . --date 2025-08-25
"""

import argparse
import datetime as dt
import json
import re
from io import StringIO
from typing import List, Optional, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
import certifi, urllib3
from urllib3.exceptions import InsecureRequestWarning

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

DD_URLS = [
    ("單日_上市", "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_0_1.djhtm"),
    ("單日_上櫃", "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_1_1.djhtm"),
]

ZGB_BASE = "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm"
ZGB_CODES = [
    ("ZGB_1470", {"a": "1470", "b": "1470", "c": "B"}),  # 金額
    ("ZGB_1650", {"a": "1650", "b": "1650", "c": "B"}),  # 金額
]

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://fubon-ebrokerdj.fbs.com.tw/",
}

RE_ONCLICK = re.compile(r"GenLink2stk\('AS(\d{4})'")
RE_CODEJS = re.compile(r"GenLink2stk\('[^']*?(\d{4})'\s*,\s*'([^']+)'\)", re.IGNORECASE)
RE_CODE = re.compile(r"(?<!\d)(\d{4})(?!\d)")
RE_CODE_NAME = re.compile(r"(?<!\d)(?P<code>\d{4})(?!\d)\s*[，,\s]*\s*(?P<name>[\u4e00-\u9fffA-Za-z0-9\-\._]+)")

def _tw_today() -> dt.date:
    if ZoneInfo is not None:
        return dt.datetime.now(ZoneInfo("Asia/Taipei")).date()
    return dt.datetime.now().date()

def parse_date_arg(s: Optional[str]) -> dt.date:
    if not s:
        return _tw_today()
    s = s.strip().replace("/", "-").replace(".", "-")
    m = re.match(r"^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$", s)
    if not m:
        raise ValueError(f"無法解析日期：{s}")
    y, mo, d = map(int, m.groups())
    return dt.date(y, mo, d)

def build_zgb_url(params: dict, d: dt.date) -> str:
    # e=f=指定日期；Fubon 接受不補零
    y, m, da = d.year, d.month, d.day
    p = {**params, "e": f"{y}-{m}-{da}", "f": f"{y}-{m}-{da}"}
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

def _get(session: requests.Session, url: str, timeout: int = 25) -> requests.Response:
    try:
        session.verify = certifi.where()  # 先用 certifi
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception:
        # Mac 上若遇到憑證錯誤 → fallback
        urllib3.disable_warnings(InsecureRequestWarning)
        r = session.get(url, timeout=timeout, verify=False)
        r.raise_for_status()
        return r

def fetch_html(url: str, session: requests.Session) -> str:
    r = _get(session, url, timeout=25)
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

# ---------- 通用抽取（給 DD 頁面用） ----------
def extract_from_onclick(html: str) -> Optional[pd.DataFrame]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for node in soup.find_all(onclick=RE_ONCLICK):
        m = RE_ONCLICK.search(node.get("onclick", ""))
        if not m:
            continue
        code = m.group(1)
        name = node.get_text(strip=True)
        if not name:
            continue
        name = re.sub(rf"^{code}\s*", "", name)
        rows.append((code, name))
    if not rows:
        return None
    return pd.DataFrame(rows, columns=["代號", "名稱"]).drop_duplicates()

def extract_from_js(html: str) -> Optional[pd.DataFrame]:
    items = RE_CODEJS.findall(html)
    if not items:
        return None
    rows = [(c, n.strip()) for c, n in items if n.strip()]
    if not rows:
        return None
    return pd.DataFrame(rows, columns=["代號", "名稱"]).drop_duplicates()

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

    # 1) 有『代號/名稱』欄位
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
            out = df.rename(columns={v: k for k, v in colmap.items()})[["代號", "名稱"]]
            out["代號"] = out["代號"].astype(str).str.extract(RE_CODE)
            out["名稱"] = out["名稱"].astype(str).str.replace(r"\s+", "", regex=True)
            out = out.dropna().drop_duplicates()
            if len(out) > 0:
                return out.reset_index(drop=True)

    # 2) fallback: 正則掃欄位文字
    rows = []
    for df in candidates:
        for _, row in df.iterrows():
            text = " ".join(str(v) for v in row.values)
            m = RE_CODE_NAME.search(text)
            if m:
                rows.append((m.group("code"), m.group("name")))
    if rows:
        return pd.DataFrame(rows, columns=["代號", "名稱"]).drop_duplicates().reset_index(drop=True)
    return None

def parse_codes_generic(html: str) -> pd.DataFrame:
    for extractor in (extract_from_onclick, extract_from_js, extract_from_tables):
        df = extractor(html)
        if df is not None and len(df) > 0:
            df = df[["代號", "名稱"]].copy()
            df["代號"] = df["代號"].astype(str).str.extract(RE_CODE)
            df["名稱"] = df["名稱"].astype(str).str.replace(r"\s+", "", regex=True)
            return df.dropna().drop_duplicates().reset_index(drop=True)
    raise ValueError("無法抽出代號/名稱 (generic)")

# ---------- ZGB 專用：只抓左欄『買超』 ----------
# 取代舊的 extract_zgb_buy_only
def extract_zgb_buy_only(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")

    # 先找到寫著「買超」的表頭，鎖定那一張表
    hdr = soup.find(lambda tag: tag.name in ("td", "th") and tag.get_text(strip=True) == "買超")
    if not hdr:
        raise ValueError("找不到『買超』表頭")

    # 「買超」所在的整張表
    tbl = hdr.find_parent("table")
    if not tbl:
        raise ValueError("找不到『買超』對應的 table")

    # ✅ 直接在該表的 HTML 內，用 GenLink2stk 解析代號/名稱
    table_html = str(tbl)
    items = re.findall(r"GenLink2stk\('AS(\d{4})','([^']+)'\)", table_html)
    rows = [(code, re.sub(r"\s+", "", name)) for code, name in items]

    # 若還是抓不到，再用第二招：抓 <a> 文字裡的「四碼+名稱」
    if not rows:
        for a in tbl.find_all("a"):
            text = a.get_text(strip=True)
            m = re.match(r"^(\d{4})(.+)$", text)
            if m:
                code, name = m.group(1), re.sub(r"\s+", "", m.group(2))
                rows.append((code, name))

    if not rows:
        # 仍然沒有 → 印出表內前幾行幫助除錯
        snippet = tbl.get_text()[:500]
        raise ValueError(f"買超表沒有解析到任何股票 (snippet={snippet})")

    df = pd.DataFrame(rows, columns=["代號", "名稱"]).drop_duplicates().reset_index(drop=True)
    return df



# ---------- 抓取與整理 ----------
def scrape_group(name: str, url: str, session: requests.Session) -> pd.DataFrame:
    html = fetch_html(url, session)
    if "/z/zg/zgb/zgb0.djhtm" in url.lower():
        df = extract_zgb_buy_only(html)  # 只買超
    else:
        df = parse_codes_generic(html)   # 單日_上市/上櫃
    df = df[["代號", "名稱"]].copy()
    df["代號"] = df["代號"].astype(str)
    df["名稱"] = df["名稱"].astype(str)
    return df.dropna().drop_duplicates().reset_index(drop=True)

def df_to_list(df: pd.DataFrame) -> List[Dict[str, str]]:
    return [{"代號": str(r["代號"]), "名稱": str(r["名稱"])} for _, r in df.iterrows()]

def triple_intersection(a: pd.DataFrame, b: pd.DataFrame, c: pd.DataFrame) -> pd.DataFrame:
    # 以『代號』交集；名稱取最先源 (a) 的名稱，缺則補 b/c
    sa = set(a["代號"]); sb = set(b["代號"]); sc = set(c["代號"])
    codes = sa & sb & sc
    if not codes:
        return pd.DataFrame(columns=["代號", "名稱"])
    # 建索引方便找名稱
    name_map = {}
    for df in (a, b, c):
        for _, r in df.iterrows():
            code = r["代號"]; nm = str(r["名稱"]).strip()
            if code not in name_map and nm:
                name_map[code] = nm
    rows = [(code, name_map.get(code, "")) for code in sorted(codes)]
    return pd.DataFrame(rows, columns=["代號", "名稱"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=str, default=".", help="輸出資料夾")
    ap.add_argument("--date", type=str, default=None, help="ZGB 指定日期 (YYYY-MM-DD / YYYY-M-D)")
    args = ap.parse_args()

    target_date = parse_date_arg(args.date) if args.date else _tw_today()
    session = _requests_session()

    # 蒐集四組
    targets = []
    for n, url in DD_URLS:
        targets.append((n, url))
    for n, params in ZGB_CODES:
        targets.append((n, build_zgb_url(params, target_date)))

    data: Dict[str, pd.DataFrame] = {}
    for name, url in targets:
        try:
            df = scrape_group(name, url, session)
            data[name] = df
            print(f"[OK] {name} rows={len(df)}")
        except Exception as e:
            print(f"[ERR] {name}: {e}")
            data[name] = pd.DataFrame(columns=["代號", "名稱"])

    # 三組交集
    s  = data.get("單日_上市", pd.DataFrame(columns=["代號","名稱"]))
    ot = data.get("單日_上櫃", pd.DataFrame(columns=["代號","名稱"]))
    z1 = data.get("ZGB_1470", pd.DataFrame(columns=["代號","名稱"]))
    z2 = data.get("ZGB_1650", pd.DataFrame(columns=["代號","名稱"]))

    inter_s = triple_intersection(s, z1, z2)
    inter_ot = triple_intersection(ot, z1, z2)

    # 組 JSON
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outpath = f"{args.out.rstrip('/')}/fubon_{ts}.json"

    payload = {
        "summary": {
            "date_for_zgb": target_date.isoformat(),
            "group_counts": {k: int(len(v)) for k, v in data.items()},
            "triple_intersection_counts": {
                "單日_上市∩ZGB_1470∩ZGB_1650": int(len(inter_s)),
                "單日_上櫃∩ZGB_1470∩ZGB_1650": int(len(inter_ot)),
            },
        },
        "data": {
            k: df_to_list(v) for k, v in data.items()
        },
        "overlaps": {
            "單日_上市∩ZGB_1470∩ZGB_1650": df_to_list(inter_s),
            "單日_上櫃∩ZGB_1470∩ZGB_1650": df_to_list(inter_ot),
        }
    }

    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved JSON: {outpath}")

if __name__ == "__main__":
    main()
