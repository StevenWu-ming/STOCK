# config.py
import os, sys
from pathlib import Path
from dotenv import load_dotenv

def APP_DIR() -> Path:
    # 打包後用 exe 所在資料夾；開發時用檔案所在資料夾
    return Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent

BASE_DIR = APP_DIR()
load_dotenv(BASE_DIR / ".env")     # ✅ 固定從 exe 同層載入 .env

# 建議輸出也跟著放在 exe 同層
OUT_DIR = BASE_DIR / "out"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# 清理策略：擇一或並用
OUT_CLEAN_BEFORE_RUN = True  # True：每次跑前清空 out 裡的 fubon_*.json
OUT_MAX_KEEP = 30             # 只保留最近 N 份（OUT_CLEAN_BEFORE_RUN=True 時忽略）

# ── 輸出與排程 ─────────────────────────────────────────────
OUT_DIR = Path(__file__).parent / "out"
SCHEDULE_TIME = "18:00"  # Linux/macOS 用 schedule；Windows 建議用工作排程器

# ── Discord 外觀（可選）────────────────────────────────────
DISCORD_BOT_NAME = "Fubon Scraper"
DISCORD_BOT_AVATAR = ""  # 放你的頭像 URL（留空則用預設）
DISCORD_EMBED_COLOR = 0x2ECC71  # 綠色；想換顏色填 0xRRGGBB
DISCORD_FOOTER_TEXT = "Fubon eBrokerDJ"

# ── Fubon 來源設定（集中管理）──────────────────────────────
DD_URLS = [
    ("單日_上市", "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_0_1.djhtm"),
    ("單日_上櫃", "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_1_1.djhtm"),
    ("3日_上市",  "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_0_3.djhtm"),
    ("3日_上櫃",  "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_1_3.djhtm"),
    ("5日_上市",  "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_0_5.djhtm"),
    ("5日_上櫃",  "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_dd_1_5.djhtm"),
]

ZGB_BASE = "https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm"

# 預設 ZGB 目標（可增刪）
# a=分點起、b=分點迄、c=B買超/S賣超、d=天數(1/3/5)
ZGB_CODES = [
    ("ZGB_1470_單日", {"a": "1470", "b": "1470", "c": "B", "d": "1"}),
    ("ZGB_1470_3日",  {"a": "1470", "b": "1470", "c": "B", "d": "3"}),
    ("ZGB_1470_5日",  {"a": "1470", "b": "1470", "c": "B", "d": "5"}),
    ("ZGB_1650_單日", {"a": "1650", "b": "1650", "c": "B", "d": "1"}),
    ("ZGB_1650_3日",  {"a": "1650", "b": "1650", "c": "B", "d": "3"}),
    ("ZGB_1650_5日",  {"a": "1650", "b": "1650", "c": "B", "d": "5"}),
]

# 額外 ZGB 目標（已加入：凱基台北 9200 一日買超）
EXTRA_ZGB_TARGETS = [
    {"label": "KGI_台北_單日", "params": {"a": "9200", "b": "9268", "c": "B", "d": "1"}},
    {"label": "KGI_松山_單日", "params": {"a": "9200", "b": "9217", "c": "B", "d": "1"}},
]

# 通用 HTTP 標頭
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://fubon-ebrokerdj.fbs.com.tw/",
}

# ── 通知設定 ───────────────────────────────────────────────
# 可放多個 Webhook；留空代表不發送
DISCORD_WEBHOOKS = {
    "單日_上市": os.getenv("DISCORD_WEBHOOK_單日_上市", ""),
    "單日_上櫃": os.getenv("DISCORD_WEBHOOK_單日_上櫃", ""),
    "3日_上市": os.getenv("DISCORD_WEBHOOK_3日_上市", ""),
    "3日_上櫃": os.getenv("DISCORD_WEBHOOK_3日_上櫃", ""),
    "5日_上市": os.getenv("DISCORD_WEBHOOK_5日_上市", ""),
    "5日_上櫃": os.getenv("DISCORD_WEBHOOK_5日_上櫃", ""),
    "KGI_台北_單日": os.getenv("DISCORD_WEBHOOK_KGI_台北_單日", ""),
    "KGI_松山_單日": os.getenv("DISCORD_WEBHOOK_KGI_松山_單日", ""),
    }

# ── 交集規則（已包含預設 6 組 + KGI 台北示範）────────────────
INTERSECTION_RULES = [
    # 預設：單日 / 3日 / 5日 ×（上市、上櫃）×（1470、1650）
    {"name": "單日_上市×1470×1650", "groups": ["單日_上市", "ZGB_1470_單日", "ZGB_1650_單日"]},
    {"name": "單日_上櫃×1470×1650", "groups": ["單日_上櫃", "ZGB_1470_單日", "ZGB_1650_單日"]},
    {"name": "3日_上市×1470×1650", "groups": ["3日_上市", "ZGB_1470_3日", "ZGB_1650_3日"]},
    {"name": "3日_上櫃×1470×1650", "groups": ["3日_上櫃", "ZGB_1470_3日", "ZGB_1650_3日"]},
    {"name": "5日_上市×1470×1650", "groups": ["5日_上市", "ZGB_1470_5日", "ZGB_1650_5日"]},
    {"name": "5日_上櫃×1470×1650", "groups": ["5日_上櫃", "ZGB_1470_5日", "ZGB_1650_5日"]},

    # 進階示範：把「凱基台北(9200) 一日買超」也納入交集
    {"name": "KGI_台北_單日", "groups": ["ZGB_KGI_台北_單日"]},
    {"name": "KGI_松山_單日", "groups": ["ZGB_KGI_松山_單日"]},
    # 你也可以按需求再加更多組合，例如上市×KGI×1650 等
    # {"name": "單日_上市×KGI_台北_單日×1650", "groups": ["單日_上市", "ZGB_KGI_台北_單日", "ZGB_1650_單日"]},
]