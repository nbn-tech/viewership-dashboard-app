"""
番組アノテーション検索 Lambda。

Glue Data Catalog (video_analyzer.corner_csv, 実体は s3://bangumi-info/results/
以下のCSV) を Athena 経由で検索する。このテーブルはLake Formation非管理の
通常のGlue外部テーブルなので、Lambda実行ロールへの標準的なIAM権限
(Athena/Glue/S3のGetObject)だけでアクセスできる。

デプロイ想定:
- Lambda ランタイム: Python 3.13 (boto3 同梱)
- 実行ロールに以下を許可:
    athena:StartQueryExecution / GetQueryExecution / GetQueryResults
    glue:GetTable / GetDatabase (video_analyzer.corner_csv)
    s3:GetObject / ListBucket (s3://bangumi-info/results/)
    s3:GetObject / PutObject (Athena結果出力先 s3://bangumi-info/athena-results/)
- Lambda Function URL 経由で /api/search/annotation として公開する想定
  (既存の /api/search/semantic と同じ構成)

補足(旧S3 Tablesベースからの移行経緯):
以前は bangumi_annotations.annotation (S3 Tables/Iceberg, Lake Formation管理)
を対象にしていたが、Lambda実行ロールからのアクセスがLake Formation側で
常に0件になる問題があり(原因は組織のネットワーク制限の可能性が高いが未特定)、
解析結果を video_analyzer.corner_csv という通常のCSVベースのGlueテーブルに
集約する方式に切り替えた。
"""

import json
import os
import re
import time
from datetime import datetime, timedelta

import boto3

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "video_analyzer")
ATHENA_TABLE = os.environ.get("ATHENA_TABLE", "corner_csv")
ATHENA_OUTPUT_LOCATION = os.environ.get(
    "ATHENA_OUTPUT_LOCATION", "s3://bangumi-info/athena-results/"
)

POLL_INTERVAL_SECONDS = 1
MAX_POLL_ATTEMPTS = 30
DEFAULT_LIMIT = 200
MAX_LIMIT = 1000

# channel列 (例: "ch6") → 局コード（録画対象の6chのみ。NHKE は録画対象外）
CH_TO_STATION = {
    "1": "THK",
    "2": "TVA",
    "3": "NHK",
    "4": "CTV",
    "5": "CBC",
    "6": "NBN",
}

CHANNEL_RE = re.compile(r"ch(?P<ch>\d+)", re.IGNORECASE)
# filename列 (例: "CH6_20260629_231500.mp4") から録画開始時刻(HHMMSS)を取り出す
FILENAME_TIME_RE = re.compile(r"_(?P<time>\d{6})\.mp4$", re.IGNORECASE)

_athena_client = None


def _athena():
    global _athena_client
    if _athena_client is None:
        _athena_client = boto3.client("athena", region_name=AWS_REGION)
    return _athena_client


def _escape_like_literal(value: str) -> str:
    # Presto/Athena の文字列リテラル ' と LIKE の特殊文字 % _ をエスケープする。
    # ESCAPE '\' を使うクエリと組み合わせる前提。
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    escaped = escaped.replace("'", "''")
    return escaped


def _build_query(search_term: str, limit: int) -> str:
    term = _escape_like_literal(search_term)
    return f"""
SELECT
    broadcast_date,
    channel,
    filename,
    trim(start_sec) AS start_sec,
    trim(end_sec) AS end_sec,
    trim(title) AS title,
    trim(summary) AS summary,
    trim(tags) AS tags
FROM {ATHENA_TABLE}
WHERE title LIKE '%{term}%' ESCAPE '\\'
   OR summary LIKE '%{term}%' ESCAPE '\\'
   OR tags LIKE '%{term}%' ESCAPE '\\'
ORDER BY broadcast_date, channel, filename, TRY_CAST(start_sec AS DOUBLE)
LIMIT {limit}
"""


def _run_athena_query(query: str) -> str:
    resp = _athena().start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    return resp["QueryExecutionId"]


def _wait_for_query(query_execution_id: str) -> None:
    for _ in range(MAX_POLL_ATTEMPTS):
        resp = _athena().get_query_execution(QueryExecutionId=query_execution_id)
        state = resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = resp["QueryExecution"]["Status"].get(
                "StateChangeReason", "unknown error"
            )
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(
        f"Athena query {query_execution_id} did not finish within timeout"
    )


def _fetch_all_rows(query_execution_id: str):
    rows = []
    next_token = None
    first_page = True
    while True:
        kwargs = {"QueryExecutionId": query_execution_id, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = _athena().get_query_results(**kwargs)
        result_rows = resp["ResultSet"]["Rows"]
        start_idx = 1 if first_page else 0  # 1ページ目だけヘッダ行をスキップ
        for row in result_rows[start_idx:]:
            values = [col.get("VarCharValue", "") for col in row["Data"]]
            rows.append(values)
        first_page = False
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return rows


def _derive_schedule_fields(broadcast_date: str, channel: str, filename: str,
                             start_sec: float, end_sec: float):
    ch_match = CHANNEL_RE.search(channel)
    if not ch_match:
        return None
    station_id = CH_TO_STATION.get(ch_match.group("ch"))
    if station_id is None or not re.fullmatch(r"\d{8}", broadcast_date):
        return None

    result = {
        "stId": station_id,
        "date": f"{broadcast_date[0:4]}-{broadcast_date[4:6]}-{broadcast_date[6:8]}",
    }

    time_match = FILENAME_TIME_RE.search(filename)
    if time_match:
        file_start_dt = datetime.strptime(
            broadcast_date + time_match.group("time"), "%Y%m%d%H%M%S"
        )
        result["startMin"] = (file_start_dt + timedelta(seconds=start_sec)).strftime("%H:%M")
        result["endMin"] = (file_start_dt + timedelta(seconds=end_sec)).strftime("%H:%M")

    return result


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _row_to_result(row):
    padded = (row + [""] * 8)[:8]
    broadcast_date, channel, filename, start_sec_str, end_sec_str, title, summary, tags = padded
    start_sec = _to_float(start_sec_str)
    end_sec = _to_float(end_sec_str, default=start_sec)

    result = {
        "object_key": filename,
        "start_sec": start_sec,
        "end_sec": end_sec,
        "title": title,
        "summary": summary,
        "tags": tags,
    }
    schedule = _derive_schedule_fields(broadcast_date, channel, filename, start_sec, end_sec)
    if schedule:
        result.update(schedule)
    return result


def search_annotations(query_text: str, limit: int = DEFAULT_LIMIT):
    if not query_text or not query_text.strip():
        return []
    limit = max(1, min(int(limit), MAX_LIMIT))
    sql = _build_query(query_text.strip(), limit)
    query_execution_id = _run_athena_query(sql)
    _wait_for_query(query_execution_id)
    rows = _fetch_all_rows(query_execution_id)
    return [_row_to_result(row) for row in rows]


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except (TypeError, json.JSONDecodeError):
        body = {}

    query_text = body.get("query", "")
    limit = body.get("limit", DEFAULT_LIMIT)

    try:
        results = search_annotations(query_text, limit)
    except (RuntimeError, TimeoutError) as exc:
        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(exc)}, ensure_ascii=False),
        }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"results": results}, ensure_ascii=False),
    }


if __name__ == "__main__":
    import sys

    q = sys.argv[1] if len(sys.argv) > 1 else "大谷"
    print(json.dumps(search_annotations(q), ensure_ascii=False, indent=2))
