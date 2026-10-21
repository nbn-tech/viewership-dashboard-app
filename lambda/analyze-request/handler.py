"""
動画分析リクエスト受付 Lambda。

ダッシュボードの「分析する」ボタンから動画のS3キーを受け取り、
s3://bangumi-info/requests/analyze_requests.csv に1行追記する。
このCSVは動画分析システム(video_analyzer)のワーカーが毎晩18:00の起動時
(または再起動時)に読み込み、記載された動画を最優先で分析する。

CSVの仕様(ワーカー側):
- 1行に1つ、動画のS3キー(movie/{channel}/{YYYYMMDD}/{ファイル名})をそのまま書く
- 動画の拡張子で終わらない行と "#" で始まる行は無視される
- 分析済み(results/ 配下にCSVがある)動画は書かれたままでも自動スキップされるので、
  処理済みのキーを消す必要はない

複数人がほぼ同時にボタンを押しても互いの追記を上書きしないよう、S3の条件付き書き込み
(既存ファイルは If-Match=ETag、新規作成は If-None-Match=*)で読み込み→追記→書き込みを行い、
競合した場合は読み直して再試行する。

デプロイ想定:
- Lambda ランタイム: Python 3.13 (boto3 同梱)
- 実行ロールに以下を許可:
    s3:GetObject / s3:PutObject on arn:aws:s3:::bangumi-info/requests/*
- Lambda Function URL で公開し、CORSの許可オリジンをダッシュボードのドメインに限定する
  (既存の /api/search/annotation と同じ構成)
"""

import base64
import json
import os
import re

import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ.get("REQUEST_BUCKET", "bangumi-info")
REQUEST_KEY = os.environ.get("REQUEST_KEY", "requests/analyze_requests.csv")
# 誰でも叩けるURLなので、ファイルが際限なく膨らまないよう行数に上限を設ける
MAX_LINES = int(os.environ.get("MAX_LINES", "5000"))
MAX_RETRIES = 5

# 録画アーカイブの動画キー。例: movie/ch6/20261021/CH6_20261021_154000.mp4
# 任意の文字列をCSVに書かせないよう、この形式以外は受け付けない
VIDEO_KEY_RE = re.compile(r"^movie/ch(\d{1,2})/\d{8}/CH(\d{1,2})_\d{8}_\d{6}\.mp4$")

s3 = boto3.client("s3")


def _response(status, payload):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload, ensure_ascii=False),
    }


def _read_requests():
    """現在のCSVの行一覧とETagを返す。ファイルが無ければ ([], None)。"""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=REQUEST_KEY)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return [], None
        raise
    text = obj["Body"].read().decode("utf-8-sig")
    return text.splitlines(), obj["ETag"]


def add_request(key):
    """キーをCSVに追記する。戻り値は "added" / "already_requested" / "full"。"""
    for _ in range(MAX_RETRIES):
        lines, etag = _read_requests()
        if key in (line.strip() for line in lines):
            return "already_requested"
        if len(lines) >= MAX_LINES:
            return "full"
        body = "\n".join(lines + [key]) + "\n"
        cond = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=REQUEST_KEY,
                Body=body.encode("utf-8"),
                ContentType="text/csv; charset=utf-8",
                **cond,
            )
            return "added"
        except ClientError as exc:
            # 読み込み後に他のリクエストが先に書き込んだ → 読み直して再試行
            if exc.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict"):
                continue
            raise
    raise RuntimeError("同時書き込みが続いたため追記できませんでした")


def lambda_handler(event, context):
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    try:
        body = json.loads(raw)
    except (TypeError, ValueError):
        body = {}

    key = str(body.get("key") or "").strip()
    m = VIDEO_KEY_RE.match(key)
    # フォルダのチャンネル番号とファイル名のチャンネル番号が一致しないキーも不正として弾く
    if not m or m.group(1) != m.group(2):
        return _response(400, {"error": "動画のS3キーの形式が正しくありません", "key": key})

    try:
        status = add_request(key)
    except (ClientError, RuntimeError) as exc:
        return _response(502, {"error": str(exc)})

    if status == "full":
        return _response(429, {"error": "リクエストが上限件数に達しています。管理者に連絡してください"})
    return _response(200, {"status": status, "key": key})
