# Fubon Scraper 專案

## 📂 專案結構


project/
│── config.py # 全域設定（來源 URL、ZGB 分點、Discord webhook、排程時間…）
│── daily_runner.py # 每日流程入口（排程器呼叫）
│── send_discord_multi.py # 輕量包裝：手動發送 JSON 到 Discord
│── fubon_scraper/
│ ├── init.py
│ ├── scraper.py # 主流程：run_scraper()
│ ├── extractors.py # 解析器（onclick/js/table/ZGB）
│ └── utils.py # 共用工具（requests、日期處理…）
│── notifier/
│ ├── init.py
│ └── discord.py # 發送 Discord 訊息
│── out/ # JSON 輸出結果



---

## ⚙️ 安裝需求
- Python 3.10+
- 套件：
  ```bash
  pip install requests pandas beautifulsoup4 lxml certifi

1. 手動執行
python -m fubon_scraper.scraper --out ./out --date 2025-09-06 --simple
參數說明：
--out: 輸出資料夾
--date: 指定日期（YYYY-MM-DD），預設今天
--simple: 只輸出交集結果（不含全部 data）

2. 動態加入分點（例：凱基台北 9200 一日買超）
python -m fubon_scraper.scraper --out ./out --zgb-broker 9200 --zgb-days 1 --zgb-mode B --zgb-label KGI_台北
--zgb-broker: 券商分點代碼（9200 = 凱基台北）
--zgb-days: 1/3/5 日
--zgb-mode: B=買超 / S=賣超
--zgb-label: 顯示名稱（如 KGI_台北）

3. 發送 Discord
python send_discord_multi.py ./out/fubon_20250906_180000.json

或直接不帶參數 → 會找最新檔：
python send_discord_multi.py


Windows

開啟 工作排程器。

新增工作 → 動作：執行 python.exe daily_runner.py。

設定時間（建議每日 18:00）。

Linux/macOS

用 cron 或直接跑內建 schedule：

python daily_runner.py


會在啟動時執行一次，並每天在 config.SCHEDULE_TIME 指定時間再跑。

📑 設定檔 config.py

DD_URLS：單日/3日/5日 上市上櫃來源。

ZGB_CODES：預設的分點（1470、1650）。

EXTRA_ZGB_TARGETS：自訂分點，例：

EXTRA_ZGB_TARGETS = [
    {"label": "KGI_台北_單日", "params": {"a": "9200", "b": "9200", "c": "B", "d": "1"}},
]


DISCORD_WEBHOOKS：放 Discord Webhook URL 列表。

SCHEDULE_TIME：每日排程時間（Linux/macOS）。

🛠 常見問題

爬不到資料 / rows=0

目標網站偶爾回傳空表，建議 retry 或隔天再跑。

Discord 發送失敗

確認 config.DISCORD_WEBHOOKS 是否正確，或 webhook 是否被刪除。

字元亂碼

已內建多組編碼（big5-hkscs, cp950, utf-8），仍失敗時檢查網頁原始碼。

排程沒有跑

Windows：檢查 Task Scheduler。

Linux/macOS：檢查 cron 或保持 daily_runner.py 執行。


✅ 建議流程

修改 config.py → 加入分點、Webhook。

先手動跑 python -m fubon_scraper.scraper，確認 JSON 正常。

跑 send_discord_multi.py，確認 Discord 收到訊息。

最後再設定排程。



---

## 🔀 自訂交集規則（進階）

預設交集邏輯：  
- 「上市 × ZGB_1470 × ZGB_1650」  
- 「上櫃 × ZGB_1470 × ZGB_1650」  
針對單日 / 3日 / 5日 都會各算一次。

如果你想把其他分點（例如 **KGI_台北**）也納入交集，可以在 `config.py` 加入 `INTERSECTION_RULES`：

```python
# config.py

INTERSECTION_RULES = [
    {
        "name": "單日_上市×1470×1650", 
        "groups": ["單日_上市", "ZGB_1470_單日", "ZGB_1650_單日"]
    },
    {
        "name": "單日_上櫃×KGI台北×1470",
        "groups": ["單日_上櫃", "ZGB_KGI_台北_單日", "ZGB_1470_單日"]
    },
]
