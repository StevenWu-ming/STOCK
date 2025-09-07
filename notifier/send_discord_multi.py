#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
用途：
- 兼容舊流程：直接呼叫本檔，會自動找 out/ 底下最新的 fubon_*.json 並發送到 Discord
- 或指定一個/多個 JSON 路徑作為參數逐一發送

例：
  python send_discord_multi.py
  python send_discord_multi.py ./out/fubon_20250906_180001.json
  python send_discord_multi.py ./out/a.json ./out/b.json
"""
import sys
from pathlib import Path
from typing import Optional, Iterable

from notifier.discord import send_discord
import config


def _latest_json_in(dirpath: Path) -> Optional[Path]:
    files = sorted(dirpath.glob("fubon_*.json"))
    return files[-1] if files else None


def _iter_inputs(argv: list[str]) -> Iterable[Path]:
    if argv:
        for p in argv:
            yield Path(p)
    else:
        latest = _latest_json_in(Path(config.OUT_DIR))
        if latest:
            yield latest


def main() -> int:
    any_sent = False
    for path in _iter_inputs(sys.argv[1:]):
        if not path.exists():
            print(f"⚠️ 檔案不存在：{path}")
            continue
        try:
            send_discord(str(path))
            any_sent = True
        except Exception as e:
            print(f"⚠️ 發送失敗 {path}: {e}")

    if not any_sent:
        print("ℹ️ 沒有可發送的檔案（請指定 JSON 路徑，或先執行 daily_runner 產生結果）")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
