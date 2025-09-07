# notifier/discord.py
import json
from typing import Iterable, Union, List, Dict, Tuple
from urllib.parse import urlparse
from datetime import datetime, timezone

import requests
import config

# ── 外觀（可由 config 覆寫） ─────────────────────────────
BOT_NAME = getattr(config, "DISCORD_BOT_NAME", "Fubon Scraper")
BOT_AVATAR = getattr(config, "DISCORD_BOT_AVATAR", "")
EMBED_COLOR = getattr(config, "DISCORD_EMBED_COLOR", 0x2ECC71)
FOOTER_TEXT = getattr(config, "DISCORD_FOOTER_TEXT", "Fubon eBrokerDJ")


# ---------- 基礎工具 ----------
def _chunk_lines(lines: List[str], max_chars: int = 1000) -> Iterable[str]:
    buf, curr = [], 0
    for ln in lines:
        ln = ln.rstrip()
        need = len(ln) + (1 if buf else 0)
        if curr + need > max_chars and buf:
            yield "\n".join(buf); buf, curr = [], 0
        buf.append(ln); curr += need
    if buf:
        yield "\n".join(buf)


def _normalize_webhooks(w: Union[list, dict, str, None]) -> Union[List[str], Dict[str, str]]:
    """保留使用者型態：list -> list, dict -> dict, str -> list[str]"""
    if isinstance(w, dict):
        items = {}
        for k, v in w.items():
            if isinstance(v, str):
                pu = urlparse(v)
                if pu.scheme in ("http", "https") and pu.netloc:
                    items[str(k)] = v
                else:
                    print(f"⚠️ 忽略非 URL Webhook（{k}）：{v}")
        return items
    elif isinstance(w, (list, tuple, set)):
        out = []
        for u in w:
            if not isinstance(u, str):
                continue
            pu = urlparse(u)
            if pu.scheme in ("http", "https") and pu.netloc:
                out.append(u)
            else:
                print(f"⚠️ 忽略非 URL Webhook：{u}")
        return out
    elif isinstance(w, str):
        pu = urlparse(w)
        return [w] if (pu.scheme in ("http", "https") and pu.netloc) else []
    return []


# ---------- 路由：把 embed 送到哪個 webhook ----------
def _select_webhooks_for_name(name: str) -> List[str]:
    """
    若 DISCORD_WEBHOOKS 是 dict：以「鍵名出現在 name 中」來路由，採最長匹配（更精準）。
    若是 list：回傳整個清單（全部廣播）。
    若沒命中任何鍵，使用 DEFAULT_DISCORD_WEBHOOKS（可空）。
    """
    configured = _normalize_webhooks(getattr(config, "DISCORD_WEBHOOKS", []))
    if isinstance(configured, list):
        return configured[:]  # 廣播
    elif isinstance(configured, dict):
        # 最長鍵名優先
        matches: List[Tuple[int, str]] = []
        for key in configured.keys():
            if key and key in name:
                matches.append((len(key), key))
        if matches:
            matches.sort(reverse=True)  # 長的優先
            key = matches[0][1]
            return [configured[key]]
        # 沒命中 → 回預設
        defaults = _normalize_webhooks(getattr(config, "DEFAULT_DISCORD_WEBHOOKS", []))
        return defaults if isinstance(defaults, list) else []
    else:
        return []


# ---------- 產生 embed（每個交集一張卡，列出完整清單） ----------
def _build_embed_for_overlap(name: str, items: List[dict], date_str: str) -> dict:
    lines = [f"• {it.get('代號', '')} {it.get('名稱', '')}".strip() for it in items] or ["（無）"]
    fields = [
        {"name": "日期", "value": f"`{date_str}`", "inline": True},
        {"name": "統計", "value": f"共 **{len(items)}** 檔", "inline": True},
    ]
    for idx, chunk in enumerate(_chunk_lines(lines, max_chars=1000), start=1):
        fields.append({
            "name": "清單" if idx == 1 else f"清單（續 {idx}）",
            "value": chunk,
            "inline": False
        })
    return {
        "title": name,
        "color": EMBED_COLOR,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": fields,
        "footer": {"text": FOOTER_TEXT},
    }


def send_discord(json_path: str) -> None:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    date_str = data.get("date") or data.get("summary", {}).get("date_for_zgb", "")
    overlaps = data.get("overlaps") or {}
    if not overlaps:
        print("ℹ️ overlaps 為空，無卡片可發送。")
        return

    # 逐交集 → 依規則路由到對應 webhook
    for name, items in overlaps.items():
        embed = _build_embed_for_overlap(name, items, date_str)
        webhooks = _select_webhooks_for_name(name)

        if not webhooks:
            print(f"ℹ️ {name} 沒有匹配到任何 webhook（也無預設），略過發送。")
            continue

        for url in webhooks:
            try:
                payload = {
                    "username": BOT_NAME,
                    **({"avatar_url": BOT_AVATAR} if BOT_AVATAR else {}),
                    "embeds": [embed],
                }
                r = requests.post(url, json=payload, timeout=20)
                if r.status_code >= 400:
                    try:
                        detail = r.json()
                    except Exception:
                        detail = r.text
                    raise RuntimeError(f"HTTP {r.status_code} → {detail}")
                print(f"[OK] {name} → {url[:40]}…")
            except Exception as e:
                print(f"⚠️ 發送失敗（{name}）: {e}")
