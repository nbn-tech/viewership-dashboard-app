"""
番組アノテーション検索 Lambda。

Glue Data Catalog (bangumi_annotations.annotation) を Athena 経由で検索し、
object_key (movie/ch{n}/YYYYMMDD/CH{n}_YYYYMMDD_HHMMSS.mp4) から
局コード・放送日・実時刻を逆算して返す。

デプロイ想定:
- Lambda ランタイム: Python 3.12 (boto3 同梱)
- 実行ロールに以下を許可:
    athena:StartQueryExecution / GetQueryExecution / GetQueryResults
    glue:GetTable / GetDatabase / GetPartitions (bangumi_annotations.annotation)
    s3:GetObject (アノテーションデータ本体)
    s3:GetObject, s3:PutObject, s3:GetBucketLocation
      (Athena結果出力先 s3://bangumi-info/athena-results/)
- Lambda Function URL 経由で /api/search/annotation として公開する想定
  (既存の /api/search/semantic と同じ構成)
- 認証情報は Lambda 実行ロールに任せる (静的アクセスキーは使わない)。
  ローカル実行時は `aws configure` / 環境変数 (AWS_ACCESS_KEY_ID 等) の
  boto3標準の資格情報チェーンがそのまま使える。
"""

import json
import os
import re
import time
from datetime import datetime, timedelta

import boto3

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "bangumi_annotations")
ATHENA_TABLE = os.environ.get("ATHENA_TABLE", "annotation")
ATHENA_OUTPUT_LOCATION = os.environ.get(
    "ATHENA_OUTPUT_LOCATION", "s3://bangumi-info/athena-results/"
)

POLL_INTERVAL_SECONDS = 1
MAX_POLL_ATTEMPTS = 30
DEFAULT_LIMIT = 200
MAX_LIMIT = 1000

# movie/ch{n}/ の n → 局コード（録画対象の6chのみ。NHKE は録画対象外）
CH_TO_STATION = {
    "1": "THK",
    "2": "TVA",
    "3": "NHK",
    "4": "CTV",
    "5": "CBC",
    "6": "NBN",
}

OBJECT_KEY_RE = re.compile(
    r"movie/ch(?P<ch>\d+)/\d{8}/CH\d+_(?P<date>\d{8})_(?P<time>\d{6})\.mp4$"
)

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
    object_key,
    trim(split_part(text_value, ',', 1)) AS start_sec,
    trim(split_part(text_value, ',', 2)) AS end_sec,
    trim(split_part(text_value, ',', 3)) AS title,
    trim(split_part(text_value, ',', 4)) AS summary,
    trim(split_part(text_value, ',', 5)) AS tags
FROM {ATHENA_TABLE}
WHERE name LIKE 'corner\\_%' ESCAPE '\\'
  AND (
    split_part(text_value, ',', 3) LIKE '%{term}%' ESCAPE '\\'
    OR split_part(text_value, ',', 4) LIKE '%{term}%' ESCAPE '\\'
    OR split_part(text_value, ',', 5) LIKE '%{term}%' ESCAPE '\\'
  )
ORDER BY object_key, CAST(split_part(text_value, ',', 1) AS DOUBLE)
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


def _derive_schedule_fields(object_key: str, start_sec: float, end_sec: float):
    match = OBJECT_KEY_RE.search(object_key)
    if not match:
        return None
    station_id = CH_TO_STATION.get(match.group("ch"))
    if station_id is None:
        return None
    date_str = match.group("date")  # YYYYMMDD
    time_str = match.group("time")  # HHMMSS (このファイルの録画開始時刻)
    file_start_dt = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
    corner_start_dt = file_start_dt + timedelta(seconds=start_sec)
    corner_end_dt = file_start_dt + timedelta(seconds=end_sec)
    return {
        "stId": station_id,
        "date": f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}",
        "startMin": corner_start_dt.strftime("%H:%M"),
        "endMin": corner_end_dt.strftime("%H:%M"),
    }


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _row_to_result(row):
    padded = (row + [""] * 6)[:6]
    object_key, start_sec_str, end_sec_str, title, summary, tags = padded
    start_sec = _to_float(start_sec_str)
    end_sec = _to_float(end_sec_str, default=start_sec)

    result = {
        "object_key": object_key,
        "start_sec": start_sec,
        "end_sec": end_sec,
        "title": title,
        "summary": summary,
        "tags": tags,
    }
    schedule = _derive_schedule_fields(object_key, start_sec, end_sec)
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
