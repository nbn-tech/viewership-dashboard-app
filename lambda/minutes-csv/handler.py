"""
分単位コーナーCSV作成 Lambda。

動画分析システム(video_analyzer)が s3://bangumi-info/results/ に分析結果のCSVを置いたら、
同じ動画の内容を1分ごとにまとめたCSVを s3://bangumi-info/minutes/ に作る。

入力(分析結果): results/{channel}/{YYYYMMDD}/{ファイル名}_corners.csv
    列: broadcast_date,channel,filename,start_sec,end_sec,title,summary,tags,segment
    start_sec/end_sec は動画ファイルの先頭からの秒数。動画の開始時刻はファイル名
    (例: CH6_20261020_060000.mp4 → 2026-10-20 06:00:00)から求める。
出力: minutes/{channel}/{YYYYMMDD}/{ファイル名}_minutes.csv (UTF-8 BOM付き、Excelでそのまま開ける)
    列: 日時,項目,内容
    例: 2026年10月20日6時29分,スポーツ,佐藤輝明選手、MLB挑戦へ

1分ごとの決め方:
- その1分間に一番長く重なっているコーナーを採用する(同じ長さなら先に始まった方)
- その1分間のうちコーナーで埋まっているのが30秒未満の分は出力しない
  (動画の切れ目の分は、より長く映っている隣の動画の方で出力される)
- segment 列が無い古い形式のCSVは対象外(何も出力しない)

起動: EventBridge ルール(S3 の Object Created、bucket=bangumi-info、key が results/ で始まり
_corners.csv で終わるもの)。S3 バケットの通知設定(分析ワーカー用のSQS通知)には触らない。

実行ロールに必要な権限:
    s3:GetObject on arn:aws:s3:::bangumi-info/results/*
    s3:PutObject on arn:aws:s3:::bangumi-info/minutes/*
"""

import csv
import io
import os
import re
from datetime import datetime, timedelta
from urllib.parse import unquote_plus

BUCKET = os.environ.get("BUCKET", "bangumi-info")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "minutes/")
MIN_COVERAGE_SEC = 30

RESULTS_KEY_RE = re.compile(r"^results/(ch\d{1,2})/(\d{8})/(CH\d{1,2}_(\d{8})_(\d{6}))_corners\.csv$")

# ダッシュボード(src/App.jsx の SEG)と同じ表示名
SEGMENT_LABELS = {
    "news": "ニュース",
    "weather": "天気予報",
    "sports": "スポーツ",
    "feature": "特集",
    "ent": "エンタメ",
    "live": "中継",
    "opening": "オープニング",
    "ending": "エンディング",
    "cm": "CM",
    "sponsor": "提供",
    "other": "その他",
}


def output_key(results_key):
    m = RESULTS_KEY_RE.match(results_key)
    if not m:
        return None
    ch, folder, base = m.group(1), m.group(2), m.group(3)
    return f"{OUTPUT_PREFIX}{ch}/{folder}/{base}_minutes.csv"


def video_start(results_key):
    m = RESULTS_KEY_RE.match(results_key)
    return datetime.strptime(m.group(4) + m.group(5), "%Y%m%d%H%M%S")


def _fmt(dt):
    return f"{dt.year}年{dt.month}月{dt.day}日{dt.hour}時{dt.minute}分"


def build_minutes(corners_csv_text, start):
    """分析結果CSVの文字列と動画の開始時刻から、[(日時文字列, 項目, 内容), ...] を返す。
    segment 列が無い古い形式なら None を返す。"""
    reader = csv.DictReader(io.StringIO(corners_csv_text.lstrip("﻿")))
    if "segment" not in (reader.fieldnames or []):
        return None
    corners = []
    for row in reader:
        try:
            s, e = float(row["start_sec"]), float(row["end_sec"])
        except (TypeError, ValueError):
            continue
        if e <= s:
            continue
        seg = (row.get("segment") or "").strip()
        label = SEGMENT_LABELS.get(seg, "その他")
        content = "CM中" if seg == "cm" else (row.get("title") or "").strip()
        corners.append((start + timedelta(seconds=s), start + timedelta(seconds=e), label, content))
    if not corners:
        return []

    rows = []
    minute = min(c[0] for c in corners).replace(second=0, microsecond=0)
    last = max(c[1] for c in corners)
    while minute < last:
        nxt = minute + timedelta(minutes=1)
        best, best_overlap, covered = None, 0.0, 0.0
        for cs, ce, label, content in corners:
            overlap = (min(ce, nxt) - max(cs, minute)).total_seconds()
            if overlap <= 0:
                continue
            covered += overlap
            if overlap > best_overlap:
                best, best_overlap = (label, content), overlap
        if best and covered >= MIN_COVERAGE_SEC:
            rows.append((_fmt(minute), best[0], best[1]))
        minute = nxt
    return rows


def to_csv_bytes(rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(["日時", "項目", "内容"])
    w.writerows(rows)
    return ("﻿" + buf.getvalue()).encode("utf-8")


def convert(s3, results_key):
    """1ファイル変換して出力先キーを返す。対象外なら None。"""
    out_key = output_key(results_key)
    if not out_key:
        return None
    text = s3.get_object(Bucket=BUCKET, Key=results_key)["Body"].read().decode("utf-8")
    rows = build_minutes(text, video_start(results_key))
    if rows is None:
        print(f"skip (segment列なしの古い形式): {results_key}")
        return None
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=to_csv_bytes(rows), ContentType="text/csv; charset=utf-8")
    print(f"wrote {out_key} ({len(rows)} rows)")
    return out_key


def lambda_handler(event, context):
    import boto3

    s3 = boto3.client("s3")
    # EventBridge(S3 Object Created)の形式
    key = unquote_plus(event.get("detail", {}).get("object", {}).get("key", ""))
    return {"output": convert(s3, key) if key else None}
