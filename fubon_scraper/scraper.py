# fubon_scraper/scraper.py
import os
import json
import datetime as dt
from typing import Dict, List, Tuple

import pandas as pd

import config
from .utils import (
    tw_today, parse_date_arg, requests_session, fetch_html,
    build_zgb_url, zgb_side_from_url,
)
from .extractors import parse_codes_generic, extract_zgb_side


def _assert_membership(inter_df: pd.DataFrame, parents: List[pd.DataFrame], label: str) -> pd.DataFrame:
    """確保交集結果真的存在於所有父集合，否則刪除異常代號"""
    parent_sets = [set(p["代號"]) for p in parents]
    keep, dropped = [], []
    for _, r in inter_df.iterrows():
        code = r["代號"]
        if all(code in s for s in parent_sets):
            keep.append(r)
        else:
            dropped.append(code)
    if dropped:
        print(f"[FIX] {label}: 移除不存在於全部父集合的代號 → {sorted(set(dropped))}")
    if not keep:
        return pd.DataFrame(columns=["代號", "名稱"])
    return pd.DataFrame(keep, columns=["代號", "名稱"]).reset_index(drop=True)


def _triple_intersection(a: pd.DataFrame, b: pd.DataFrame, c: pd.DataFrame) -> pd.DataFrame:
    """三集合交集"""
    sa, sb, sc = set(a["代號"]), set(b["代號"]), set(c["代號"])
    codes = sa & sb & sc
    if not codes:
        return pd.DataFrame(columns=["代號", "名稱"])
    name_map: Dict[str, str] = {}
    for df in (a, b, c):
        for _, r in df.iterrows():
            code = r["代號"]
            nm = str(r["名稱"]).strip()
            if code not in name_map and nm:
                name_map[code] = nm
    rows = [(code, name_map.get(code, "")) for code in sorted(codes)]
    return pd.DataFrame(rows, columns=["代號", "名稱"])

def _nway_intersection(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """N 個集合的交集（名稱以第一個出現者為準）。"""
    if not dfs:
        return pd.DataFrame(columns=["代號", "名稱"])
    code_sets = [set(df["代號"]) for df in dfs if not df.empty]
    if not code_sets:
        return pd.DataFrame(columns=["代號", "名稱"])
    inter_codes = set.intersection(*code_sets) if len(code_sets) > 1 else code_sets[0]
    if not inter_codes:
        return pd.DataFrame(columns=["代號", "名稱"])

    name_map: Dict[str, str] = {}
    for df in dfs:
        for _, r in df.iterrows():
            code = str(r["代號"])
            nm = str(r.get("名稱", "")).strip()
            if code in inter_codes and code not in name_map and nm:
                name_map[code] = nm
    rows = [(code, name_map.get(code, "")) for code in sorted(inter_codes)]
    return pd.DataFrame(rows, columns=["代號", "名稱"])



def _df_to_list(df: pd.DataFrame) -> List[Dict[str, str]]:
    return [{"代號": str(r["代號"]), "名稱": str(r["名稱"])} for _, r in df.iterrows()]


def _scrape_group(name: str, url: str, session) -> pd.DataFrame:
    """抓取單一 group"""
    html = fetch_html(url, session)
    if "/z/zg/zgb/zgb0.djhtm" in url.lower():
        side = "賣超" if name.endswith("S") else "買超"
        df = extract_zgb_side(html, side=side)
    else:
        df = parse_codes_generic(html)

    df = df[["代號", "名稱"]].copy()
    df["代號"], df["名稱"] = df["代號"].astype(str), df["名稱"].astype(str)
    df = df.dropna().drop_duplicates().reset_index(drop=True)

    if name.startswith("ZGB_"):
        print("[DBG]", name, "first10=", df["代號"].head(10).tolist())
    return df


def run_scraper(out_dir: str, date: str | None = None, simple: bool = False) -> str:
    """主流程：抓取所有目標，產生 JSON 檔，回傳檔案路徑"""
    target_date = parse_date_arg(date) if date else tw_today()
    session = requests_session()

    targets: List[Tuple[str, str]] = []

    # 1) DD
    for n, url in config.DD_URLS:
        targets.append((n, url))

    # 2) 預設 ZGB
    for n, params in config.ZGB_CODES:
        targets.append((n, build_zgb_url(params, target_date)))

    # 3) 額外 ZGB（來自 config）
    for extra in getattr(config, "EXTRA_ZGB_TARGETS", []):
        label = extra.get("label") or "EXTRA"
        params = extra.get("params", {})
        targets.append((f"ZGB_{label}", build_zgb_url(params, target_date)))

    # 抓取
    data: Dict[str, pd.DataFrame] = {}
    for name, url in targets:
        try:
            df = _scrape_group(name, url, session)
            data[name] = df
            print(f"[OK] {name} rows={len(df)}")
        except Exception as e:
            print(f"[ERR] {name}: {e}")
            data[name] = pd.DataFrame(columns=["代號", "名稱"])

        # ── 交集計算（支援 config.INTERSECTION_RULES；無設定則用預設規則） ──
    overlaps: Dict[str, List[Dict[str, str]]] = {}

    rules = getattr(config, "INTERSECTION_RULES", None)
    if rules:
        # 依規則清單動態計算（每一條規則是一個 N 向交集）
        for rule in rules:
            name = rule.get("name") or "INTERSECTION"
            group_keys = rule.get("groups") or []
            parent_dfs = []
            for key in group_keys:
                parent_dfs.append(data.get(key, pd.DataFrame(columns=["代號", "名稱"])))

            inter_df = _nway_intersection(parent_dfs)
            inter_df = _assert_membership(inter_df, parent_dfs, name)
            overlaps[name] = _df_to_list(inter_df)
    else:
        # 預設：上市/上櫃 × 1470 × 1650，分別計算 單日/3日/5日
        for period in ["單日", "3日", "5日"]:
            s  = data.get(f"{period}_上市",  pd.DataFrame(columns=["代號", "名稱"]))
            ot = data.get(f"{period}_上櫃",  pd.DataFrame(columns=["代號", "名稱"]))
            z1 = data.get(f"ZGB_1470_{period}", pd.DataFrame(columns=["代號", "名稱"]))
            z2 = data.get(f"ZGB_1650_{period}", pd.DataFrame(columns=["代號", "名稱"]))

            inter_s  = _triple_intersection(s,  z1, z2)
            inter_s  = _assert_membership(inter_s,  [s,  z1, z2], f"{period}_上市")
            inter_ot = _triple_intersection(ot, z1, z2)
            inter_ot = _assert_membership(inter_ot, [ot, z1, z2], f"{period}_上櫃")

            overlaps[f"{period}_上市∩ZGB_1470_{period}∩ZGB_1650_{period}"] = _df_to_list(inter_s)
            overlaps[f"{period}_上櫃∩ZGB_1470_{period}∩ZGB_1650_{period}"] = _df_to_list(inter_ot)


    # 輸出 JSON
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(out_dir, exist_ok=True)
    outpath = f"{str(out_dir).rstrip('/')}/fubon_{ts}.json"

    if simple:
        payload = {"date": target_date.isoformat(), "overlaps": overlaps}
    else:
        payload = {
            "summary": {
                "date_for_zgb": target_date.isoformat(),
                "group_counts": {k: int(len(v)) for k, v in data.items()},
                "triple_intersection_counts": {k: len(v) for k, v in overlaps.items()},
            },
            "data": {k: _df_to_list(v) for k, v in data.items()},
            "overlaps": overlaps,
        }

    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved JSON: {outpath}")
    return outpath


# ── CLI 支援 ─────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=str, default=str(config.OUT_DIR), help="輸出資料夾")
    ap.add_argument("--date", type=str, default=None, help="ZGB 指定日期 (YYYY-MM-DD)")
    ap.add_argument("--simple", action="store_true", help="只輸出交集結果")

    # CLI 可臨時新增一個 ZGB 目標
    ap.add_argument("--zgb-broker", type=str, default=None, help="分點代碼（如 9200）")
    ap.add_argument("--zgb-days", type=int, choices=[1, 3, 5], default=1)
    ap.add_argument("--zgb-mode", type=str, choices=["B", "S"], default="B")
    ap.add_argument("--zgb-label", type=str, default=None, help="群組名稱（如 KGI_台北_單日）")

    args = ap.parse_args()

    # 如果 CLI 指定，就臨時 push 到 EXTRA_ZGB_TARGETS
    if args.zgb_broker:
        label = args.zgb_label or f"{args.zgb_broker}_{args.zgb_days}d_{args.zgb_mode}"
        config.EXTRA_ZGB_TARGETS.append({
            "label": label,
            "params": {"a": args.zgb_broker, "b": args.zgb_broker, "c": args.zgb_mode, "d": str(args.zgb_days)},
        })

    run_scraper(args.out, date=args.date, simple=args.simple)
