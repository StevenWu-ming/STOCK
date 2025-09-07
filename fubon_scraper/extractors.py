# fubon_scraper/extractors.py
import re
from io import StringIO
from typing import List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from .utils import RE_CODE

# 正則表達式
RE_ONCLICK = re.compile(r"GenLink2stk\('AS(\d{4,6})'")
RE_CODEJS  = re.compile(r"GenLink2stk\('[^']*?(\d{4,6})'\s*,\s*'([^']+)'\)", re.IGNORECASE)
RE_CODE_NAME = re.compile(
    r"(?<!\d)(?P<code>\d{4,6})(?!\d)\s*[，,\s]*\s*(?P<name>[\u4e00-\u9fffA-Za-z0-9\-\._]+)"
)


def extract_from_onclick(html: str) -> Optional[pd.DataFrame]:
    """從 onclick=GenLink2stk(...) 抽出代號/名稱"""
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
    """從 JS: GenLink2stk('xxxx','名稱') 抽出"""
    items = RE_CODEJS.findall(html)
    if not items:
        return None
    rows = [(c, n.strip()) for c, n in items if n.strip()]
    if not rows:
        return None
    return pd.DataFrame(rows, columns=["代號", "名稱"]).drop_duplicates()


def extract_from_tables(html: str) -> Optional[pd.DataFrame]:
    """從 <table> 嘗試讀取，找有「代號」「名稱」欄位的表格"""
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

    # 嘗試對應欄位
    for df in candidates:
        cols = [str(c).strip() for c in df.columns]
        has_code = any("代號" in c or "證券代號" in c for c in cols)
        has_name = any("名稱" in c or "股票名稱" in c or "證券名稱" in c for c in cols)
        if has_code and has_name:
            df.columns = cols
            colmap = {}
            for c in cols:
                if "代號" in c or "證券代號" in c:
                    colmap["代號"] = c
                if "名稱" in c or "股票名稱" in c or "證券名稱" in c:
                    colmap["名稱"] = c
            out = df.rename(columns={v: k for k, v in colmap.items()})[["代號", "名稱"]]
            out["代號"] = out["代號"].astype(str).str.extract(RE_CODE)
            out["名稱"] = out["名稱"].astype(str).str.replace(r"\s+", "", regex=True)
            out = out.dropna().drop_duplicates()
            if len(out) > 0:
                return out.reset_index(drop=True)

    # 保底：逐列找 4 碼數字 + 中文名稱
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
    """依序嘗試 onclick / js / table 三種方式抽取代號/名稱"""
    for extractor in (extract_from_onclick, extract_from_js, extract_from_tables):
        df = extractor(html)
        if df is not None and len(df) > 0:
            df = df[["代號", "名稱"]].copy()
            df["代號"] = df["代號"].astype(str).str.extract(RE_CODE)
            df["名稱"] = df["名稱"].astype(str).str.replace(r"\s+", "", regex=True)
            return df.dropna().drop_duplicates().reset_index(drop=True)
    raise ValueError("無法抽出代號/名稱 (generic)")


def extract_zgb_side(html: str, side: str = "買超") -> pd.DataFrame:
    """只在指定 side 區塊（買超/賣超）內抽出代號/名稱"""
    compact = re.sub(r"\s+", " ", html)

    start = compact.find(f">{side}<")
    if start == -1:
        m = re.search(rf"> *{re.escape(side)} *<", compact)
        if m:
            start = m.start()
    if start == -1:
        raise ValueError(f"找不到『{side}』區塊標記")

    other = "賣超" if side == "買超" else "買超"
    end = compact.find(f">{other}<", start + 1)
    segment = compact[start:end] if end != -1 else compact[start:]

    # 先抓 GenLink2stk
    items = re.findall(r"GenLink2stk\('AS(\d{4,6})','([^']+)'\)", segment)
    rows = [(code, re.sub(r"\s+", "", name)) for code, name in items]

    # 保底：從 <a> 文字抓
    if not rows:
        seg_soup = BeautifulSoup(segment, "lxml")
        for a in seg_soup.find_all("a"):
            text = a.get_text(strip=True)
            m = re.match(r"^(\d{4,6})(.+)$", text)
            if m:
                code, name = m.group(1), re.sub(r"\s+", "", m.group(2))
                rows.append((code, name))

    if not rows:
        raise ValueError(f"{side}表沒有解析到任何股票")

    df = pd.DataFrame(rows, columns=["代號", "名稱"]).drop_duplicates().reset_index(drop=True)
    df = df[df["代號"].astype(str).str.fullmatch(r"\d{4,6}")]
    df["名稱"] = df["名稱"].astype(str).str.replace(r"\*", "", regex=True).str.strip()
    return df.reset_index(drop=True)
