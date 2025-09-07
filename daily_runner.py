#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import platform
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import config
from fubon_scraper import run_scraper
from notifier.discord import send_discord


def _housekeep_out():
    """依 config 設定清理 out 目錄的 fubon_*.json。"""
    p = Path(config.OUT_DIR)
    p.mkdir(parents=True, exist_ok=True)
    files = sorted(p.glob("fubon_*.json"))

    if getattr(config, "OUT_CLEAN_BEFORE_RUN", False):
        for f in files:
            try:
                f.unlink()
            except Exception as e:
                print(f"⚠️ 無法刪除 {f}: {e}")
        return

    max_keep = getattr(config, "OUT_MAX_KEEP", None)
    if isinstance(max_keep, int) and max_keep > 0 and len(files) > max_keep:
        to_delete = files[:-max_keep]
        for f in to_delete:
            try:
                f.unlink()
            except Exception as e:
                print(f"⚠️ 無法刪除 {f}: {e}")


def job():
    _housekeep_out()  # ← 跑之前先清理
    today = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    print(f"🔍 爬蟲日期: {today}")

    json_path = run_scraper(str(config.OUT_DIR), date=today, simple=True)
    send_discord(json_path)
    print("✅ 今日流程完成")


def main():
    if platform.system() == "Windows":
        job()  # Windows：交給工作排程器定時啟動本檔
    else:
        import schedule
        job()  # 啟動先跑一次
        schedule.every().day.at(config.SCHEDULE_TIME).do(job)
        print(f"⏰ 已設定每日 {config.SCHEDULE_TIME} 執行爬蟲+發送通知")
        while True:
            schedule.run_pending()
            time.sleep(30)


if __name__ == "__main__":
    main()
