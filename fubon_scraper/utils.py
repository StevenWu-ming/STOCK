# fubon_scraper/utils.py
import re
import certifi
import urllib3
import datetime as dt
from typing import Optional, Dict
from urllib.parse import urlparse, parse_qs

import requests
from requests.adapters import HTTPAdapter, Retry

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import config

# 共用正則：4 碼股票代號
RE_CODE = re.compile(r"(?<!\d)(\d{4,6})(?!\d)")


def tw_today() -> dt.date:
    """回傳台灣當地今日日期"""
    if ZoneInfo is not None:
        return dt.datetime.now(ZoneInfo("Asia/Taipei")).date()
    return dt.datetime.now().date()


def parse_date_arg(s: Optional[str]) -> dt.date:
    """解析 CLI/函式傳入的日期字串（YYYY-MM-DD / 允許 1 位月日）"""
    if not s:
        return tw_today()
    s = s.strip().replace("/", "-").replace(".", "-")
    m = re.match(r"^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$", s)
    if not m:
        raise ValueError(f"無法解析日期：{s}")
    y, mo, d = map(int, m.groups())
    return dt.date(y, mo, d)


def requests_session() -> requests.Session:
    """建立帶重試的 requests Session，套用預設 HEADERS"""
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(config.HEADERS)
    return s


def _get(session: requests.Session, url: str, timeout: int = 25) -> requests.Response:
    """優先驗證 SSL；失敗則降級為忽略驗證（避免目標站憑證異常時中斷）"""
    try:
        session.verify = certifi.where()
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = session.get(url, timeout=timeout, verify=False)
        r.raise_for_status()
        return r


def fetch_html(url: str, session: requests.Session) -> str:
    """以多組編碼嘗試解碼，回傳 HTML 文字"""
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


def build_zgb_url(params: Dict[str, str], d: dt.date) -> str:
    """
    Fubon ZGB：若沒有 d（自設區間）才帶 e/f；有 d=1/3/5 則不帶 e/f。
    params: a, b, c(B/S), d(1/3/5) / 可不含 d
    """
    y, m, da = d.year, d.month, d.day
    p = params.copy()
    if not p.get("d"):
        p.update({"e": f"{y}-{m}-{da}", "f": f"{y}-{m}-{da}"})
    qs = "&".join([f"{k}={v}" for k, v in p.items()])
    return f"{config.ZGB_BASE}?{qs}"


def zgb_side_from_url(url: str) -> str:
    """依 URL 查詢參數 c=B/S 判定 '買超' 或 '賣超'（預設買超）"""
    try:
        qs = parse_qs(urlparse(url).query)
        c = (qs.get("c", ["B"])[0] or "B").upper()
        return "買超" if c == "B" else "賣超"
    except Exception:
        return "買超"
