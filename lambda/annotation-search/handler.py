"""
番組アノテーション検索 Lambda。

Glue Data Catalog (video_analyzer.daily_corners) を Athena 経由で検索する。
実体は s3://bangumi-info/athena-data/channel={ch}/day_of_week={dow}/
broadcast_date={yyyymmdd}/corners.parquet というHiveパーティション構成のParquet。
パーティションはPartition Projectionで自動解決されるため、Glue Crawlerや
MSCK REPAIR TABLEは不要。このテーブルはLake Formation非管理の通常のGlue
外部テーブルなので、Lambda実行ロールへの標準的なIAM権限
(Athena/Glue/S3のGetObject)だけでアクセスできる。

デプロイ想定:
- Lambda ランタイム: Python 3.13 (boto3 同梱)
- 実行ロールに以下を許可:
    athena:StartQueryExecution / GetQueryExecution / GetQueryResults
    glue:GetTable / GetDatabase (video_analyzer.daily_corners)
    s3:GetObject / ListBucket (s3://bangumi-info/athena-data/)
    s3:GetObject / PutObject (Athena結果出力先 s3://bangumi-info/athena-results/)
- Lambda Function URL 経由で /api/search/annotation として公開する想定
  (既存の /api/search/semantic と同じ構成)

補足(移行経緯):
当初は bangumi_annotations.annotation (S3 Tables/Iceberg, Lake Formation管理)
を対象にしていたが、Lambda実行ロールからのアクセスがLake Formation側で
常に0件になる問題があり(原因は組織のネットワーク制限の可能性が高いが未特定)、
video_analyzer.corner_csv というCSVベースのGlueテーブルに切り替えた。
その後、CSV内のフィールドエスケープ崩れ(テロップ等にカンマ・タグが含まれ
列がズレる行がある)が見つかったため、Parquet+Hiveパーティション構成の
video_analyzer.daily_corners に再度切り替えた。
"""

import json
import os
import re
import time
from datetime import datetime, timedelta

import boto3

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "video_analyzer")
ATHENA_TABLE = os.environ.get("ATHENA_TABLE", "daily_corners")
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
# channelはpartition projectionが"injected"型のため、WHERE句で列挙する必要がある
KNOWN_CHANNELS = [f"ch{n}" for n in CH_TO_STATION]

CHANNEL_RE = re.compile(r"ch(?P<ch>\d+)", re.IGNORECASE)

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


def _validate_yyyymmdd(value):
    if value and re.fullmatch(r"\d{8}", str(value)):
        return str(value)
    return None


def _build_query(terms, limit: int, date_from=None, date_to=None, channels=None) -> str:
    channel_list = channels if channels else KNOWN_CHANNELS
    channels_sql = ",".join(f"'{c}'" for c in channel_list)

    date_conditions = ""
    date_from = _validate_yyyymmdd(date_from)
    date_to = _validate_yyyymmdd(date_to)
    if date_from:
        date_conditions += f"  AND broadcast_date >= '{date_from}'\n"
    if date_to:
        date_conditions += f"  AND broadcast_date <= '{date_to}'\n"

    # termsが空の場合はキーワード条件なし(= 局・期間だけで絞った全件取得モード。
    # Dashboardの番組表〜コーナー別〜表示で、特定局・特定日の実分析結果を丸ごと取得するのに使う)
    terms_condition = ""
    if terms:
        term_clauses = []
        for raw_term in terms:
            term = _escape_like_literal(raw_term)
            term_clauses.append(
                f"(title LIKE '%{term}%' ESCAPE '\\' "
                f"OR summary LIKE '%{term}%' ESCAPE '\\' "
                f"OR tags LIKE '%{term}%' ESCAPE '\\')"
            )
        terms_condition = f"  AND ({' OR '.join(term_clauses)})\n"

    return f"""
SELECT
    channel,
    broadcast_date,
    filename,
    program_start_sec,
    start_sec,
    end_sec,
    title,
    summary,
    tags
FROM {ATHENA_TABLE}
WHERE channel IN ({channels_sql})
{date_conditions}{terms_condition}ORDER BY broadcast_date, channel, filename, start_sec
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


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: str, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _derive_schedule_fields(broadcast_date: str, channel: str,
                             program_start_sec: int, start_sec: float, end_sec: float):
    ch_match = CHANNEL_RE.search(channel)
    if not ch_match:
        return None
    station_id = CH_TO_STATION.get(ch_match.group("ch"))
    if station_id is None or not re.fullmatch(r"\d{8}", broadcast_date):
        return None

    day_start_dt = datetime.strptime(broadcast_date, "%Y%m%d") + timedelta(seconds=program_start_sec)
    corner_start_dt = day_start_dt + timedelta(seconds=start_sec)
    corner_end_dt = day_start_dt + timedelta(seconds=end_sec)

    return {
        "stId": station_id,
        "date": f"{broadcast_date[0:4]}-{broadcast_date[4:6]}-{broadcast_date[6:8]}",
        "startMin": corner_start_dt.strftime("%H:%M"),
        "endMin": corner_end_dt.strftime("%H:%M"),
    }


def _row_to_result(row):
    padded = (row + [""] * 9)[:9]
    (channel, broadcast_date, filename, program_start_sec_str,
     start_sec_str, end_sec_str, title, summary, tags) = padded
    program_start_sec = _to_int(program_start_sec_str)
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
    schedule = _derive_schedule_fields(broadcast_date, channel, program_start_sec, start_sec, end_sec)
    if schedule:
        result.update(schedule)
    return result


def search_annotations(terms, limit: int = DEFAULT_LIMIT, date_from=None, date_to=None, channels=None):
    terms = [t.strip() for t in (terms or []) if t and t.strip()]
    # termsが空でも、channels/date_from/date_toによる絞り込み全件取得として続行する
    # (以前はここで空リストを返して打ち切っていたが、Dashboardの実データ表示用に全件取得モードを追加)
    limit = max(1, min(int(limit), MAX_LIMIT))
    sql = _build_query(terms, limit, date_from, date_to, channels)
    query_execution_id = _run_athena_query(sql)
    _wait_for_query(query_execution_id)
    rows = _fetch_all_rows(query_execution_id)
    return [_row_to_result(row) for row in rows]


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except (TypeError, json.JSONDecodeError):
        body = {}

    # keywords (AI意味検索によるキーワード群) があればOR検索、query単独指定でもOR検索、
    # どちらも無ければキーワード条件なし(局・期間で絞った全件取得モード)
    keywords = body.get("keywords") or []
    query = body.get("query") or ""
    terms = keywords if keywords else ([query] if query.strip() else [])
    limit = body.get("limit", DEFAULT_LIMIT)
    date_from = body.get("date_from")
    date_to = body.get("date_to")
    # channels: 例 ["ch6"]。未指定なら全局。既知チャンネルのみ許可(SQLに直接埋め込むためのホワイトリスト)
    channels = [c for c in (body.get("channels") or []) if c in KNOWN_CHANNELS] or None

    try:
        results = search_annotations(terms, limit, date_from, date_to, channels)
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
    print(json.dumps(search_annotations([q]), ensure_ascii=False, indent=2))
