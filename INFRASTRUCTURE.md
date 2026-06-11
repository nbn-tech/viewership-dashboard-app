# バックエンド・インフラ構成

視聴率ダッシュボードを支えるバックグラウンドシステムの全体像と各コンポーネントの説明。

---

## システム全体図

```
  ┌───────────────────────────────────────────────────────┐
  │  オンプレ録画サーバ (AlmaLinux 10)                       │
  │  capture-system (Python/Flask + FFmpeg)               │
  │  6ch USBキャプチャ → 30分セグメントMP4                   │
  └───────────────┬───────────────────────────────────────┘
                  │ Pre-signed POST URL (API Gateway経由)
                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS インフラ                                  │
│                                                                     │
│  ┌─────────────────────────────┐   ┌──────────────────┐            │
│  │  動画アップロード用           │   │  番組表(EPG)      │            │
│  │  API Gateway + Lambda       │   │  取得システム     │            │
│  │                             │   │                  │            │
│  │  capture-system-api         │   │  EventBridge     │            │
│  │  /getuploadurl (Lambda)     │   │      ↓           │            │
│  │  /upload       (Lambda)     │   │   Lambda         │            │
│  │  /uploadstatus (Lambda)     │   │      ↓           │            │
│  └──────────────┬──────────────┘   └────────┬─────────┘            │
│                 │ Pre-signed POST              │ CSV                 │
│                 ▼                             ▼                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  Amazon S3 (bangumi-info バケット)                             │  │
│  │   ├── movie/ch{1-6}/CH{n}_YYYYMMDD_HHMMSS.mp4  ← 動画        │  │
│  │   └── epg-all/bangumi_YYYYMMDD.csv              ← 番組表CSV   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌────────────────────────────┐   ┌──────────────────────────────┐  │
│  │  天気情報取得システム         │   │  AIバックエンド（分析・検索）   │  │
│  │                            │   │                              │  │
│  │  EventBridge               │   │  Lambda Function URL         │  │
│  │      ↓                    │   │  + Anthropic Claude API      │  │
│  │  Lambda → Open-Meteo API   │   │  /api/search/semantic        │  │
│  │      ↓                    │   │  /api/analysis/overview      │  │
│  │  DynamoDB（天気DB）         │   │  /api/analysis/highlight     │  │
│  │      ↓                    │   │  /api/analysis/conclusion    │  │
│  │  API Gateway               │   │  /api/analysis/topic         │  │
│  └────────────────────────────┘   └──────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
              ↑                    ↑                    ↑
┌─────────────────────────────────────────────────────────────────────┐
│                   視聴率ダッシュボード (フロントエンド)                   │
│                     AWS Amplify でホスティング                          │
└─────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────┐
  │  AI動画分析システム（コーナー分類）                   │
  │  S3(動画) → Whisper(ローカルPC) → Gemini API      │
  │  → コーナー分類結果                                │
  └──────────────────────────────────────────────────┘
```

---

## 1. 動画収録 → アップロードシステム（capture-system）

### 概要
在名7局の放送動画を6チャンネル同時録画し、AWS S3へ自動アップロードするシステム。
AlmaLinux 10 サーバ上で稼働し、完全自動で運用される。

### システム仕様
| 項目 | 内容 |
|------|------|
| OS | AlmaLinux 10 |
| 最大同時録画 | 6チャンネル（CH1〜CH6）|
| 映像フォーマット | MP4 / H.264 / 1280×720 / 30fps |
| 音声フォーマット | AAC / 48kHz / ステレオ |
| セグメント分割 | 30分ごとに自動分割 |
| ローカル保持期間 | 1日（S3アップロード後に自動削除）|
| 録画デバイス | USB キャプチャデバイス（ezcap U3）|
| 映像取り込み | FFmpeg + v4l2（MJPEG入力）|
| 音声取り込み | ALSA（カード名指定: `hw:CARD=capture,DEV=0`）|
| アプリフレームワーク | Python 3 / Flask 3.1 |
| スケジューラ | APScheduler 3.10 |
| サービス管理 | systemd（`capture-system.service`）|
| Web UI | `http://サーバIP:8000` |

### 処理フロー

```
USB キャプチャデバイス（ezcap U3）× 6台
    ↓ FFmpeg (v4l2 + ALSA)
ローカル録画 /opt/capture-system/recordings/ch{n}/
    CH{n}_YYYYMMDD_HHMMSS.mp4（30分セグメント）
    ↓ APScheduler（10分ごと）
S3Uploader（未アップロードファイルをスキャン）
    ↓ GET API Gateway /getuploadurl
capture-system-getuploadurl Lambda
    ↓ Pre-signed POST URL を返却
S3Uploader → POST Pre-signed URL + mp4バイナリ
    ↓
Amazon S3 (bangumi-info) に保存
    ↓ アップロード成功後
ローカルファイルを自動削除
```

### S3アップロードの仕組み（Pre-signed POST URL方式）
サーバ側にAWSアクセスキーを持たせず、Lambda側のIAMロールで認証を処理する安全な方式。

```
1. S3Uploader → GET /getuploadurl?ch=1&filename=CH1_YYYYMMDD_HHMMSS.mp4
         ↓ API Gateway → capture-system-getuploadurl Lambda
   ← { url, fields, s3_key } を返す（Pre-signed POST URL）

2. S3Uploader → POST {url} + fields + mp4バイナリ（multipart）
         ↓ S3 に直接書き込み
   → アップロード結果を SQLite (s3_uploads.db) に記録
   → 成功後にローカルファイルを削除
```

### Lambda関数一覧（capture-system-api）
| Lambda関数名 | メソッド | パス | 用途 |
|-------------|---------|------|------|
| `capture-system-getuploadurl` | GET | `/getuploadurl` | Pre-signed POST URL生成 |
| `capture-system-upload` | POST | `/upload` | S3へのmp4アップロード実行 |
| `capture-system-uploadstatus` | GET | `/uploadstatus` | アップロード状況取得 |

### S3保存先
- **バケット名**: `bangumi-info`（番組表CSVと同じバケット）
- **リージョン**: `ap-northeast-1`（東京）
- **格納パス**: `movie/ch{n}/CH{n}_YYYYMMDD_HHMMSS.mp4`

```
bangumi-info/
└── movie/
    ├── ch1/
    │   ├── CH1_20260601_120000.mp4
    │   └── CH1_20260601_123000.mp4
    ├── ch2/
    ├── ch3/
    ├── ch4/
    ├── ch5/
    └── ch6/
```

### 使用AWSサービス
| サービス | 用途 |
|---------|------|
| Amazon API Gateway | Pre-signed URL取得・アップロード・状況確認エンドポイント |
| AWS Lambda | Pre-signed URL生成・アップロード処理・状況取得 |
| Amazon S3 | 動画ファイル（mp4）の永続保存 |
| AWS IAM | Lambda実行ロールによるS3アクセス制御（サーバ側に認証情報不要）|

### 自動削除・監視スケジューラ
| ジョブ | 実行タイミング | 処理 |
|-------|--------------|------|
| 古いファイル削除 | 毎日 0:00 (JST) | `RETENTION_DAYS` (1日) を超えた MP4 を自動削除 |
| ディスク監視 | 1時間ごと | 使用率85%超過で警告ログ出力 |

### ディスク容量目安
| 条件 | 概算 |
|------|------|
| 1ファイル（2Mbps・30分）| 約 450 MB |
| 6CH × 24時間 | 約 13 GB/日 |
| 推奨空き容量 | 30 GB 以上 |

---

## 2. 番組表情報取得システム

### 概要
在名7局の番組表（EPG: Electronic Program Guide）データを毎日自動取得し、
ダッシュボードで参照できる形式（CSV）に変換してS3に保存する。

### 処理フロー

```
Amazon EventBridge (毎日スケジュール実行)
    ↓
AWS Lambda (番組表取得・変換処理)
    ↓
Amazon S3 (CSVファイルとして保存)
    ↓
ダッシュボード (フロントエンドから直接S3にアクセス)
```

### 使用AWSサービス
| サービス | 用途 |
|---------|------|
| Amazon EventBridge | 毎日定時に Lambda を起動するスケジューラ |
| AWS Lambda | EPGデータの取得・整形・S3保存 |
| Amazon S3 | 番組表CSVファイルの保管 |

### S3バケット
- **バケット名**: `bangumi-info`
- **リージョン**: `ap-northeast-1`（東京）
- **格納パス**: `epg-all/bangumi_YYYYMMDD.csv`（日付ごとにファイルを生成）

### CSVフォーマット
| カラム | 内容 |
|-------|------|
| station | 局番号（1=THK, 2=NHKE, 3=NHK, 4=CTV, 5=CBC, 6=NBN, 10=TVA）|
| title | 番組名 |
| description | 番組説明 |
| start_time | 開始時刻（HH:MM形式）|
| end_time | 終了時刻（HH:MM形式）|

### 注意事項
- ダッシュボードのフロントエンドが S3 に直接アクセスするため、バケットの CORS 設定が必要

---

## 3. AI動画分析（コーナー分類）システム

### 概要
放送動画を解析して番組内のコーナー（ニュース・天気・スポーツ・特集など）を
自動分類するシステム。文字起こし（Whisper）とAI分類（Gemini API）の2段階で処理。

### 処理フロー

```
S3 (放送動画 mp4)
    ↓
Whisper（音声→テキスト変換）
    ※ 現在: ローカルPCで定期実行予定
    ↓
テキストデータ（文字起こし結果）
    ↓
Google Gemini API（コーナー分類）
    ↓
コーナー分類結果
```

### 分類されるコーナー種別
| 種別ID | 表示名 |
|--------|--------|
| news | ニュース |
| weather | 天気予報 |
| sports | スポーツ |
| feature | 特集 |
| ent | エンタメ |
| live | 生中継 |
| opening | オープニング |
| ending | エンディング |
| cm | CMブロック |
| sponsor | スポンサー |
| other | その他 |

### 使用技術
| 技術 | 用途 |
|------|------|
| Whisper | 動画音声をテキストに変換（文字起こし）|
| Google Gemini API | テキストからコーナー種別を分類 |

### 現在のステータス
- 分類結果は `src/data/ドデスカ動画コーナー分類結果.xlsx` に格納（4/17放送分）
- Whisper 実行環境: ローカルPC（定期実行）→ 将来的にクラウド化を検討

---

## 4. 天気情報取得システム

### 概要
放送日の名古屋地区の気象データ（最高・最低気温等）を取得し、
ダッシュボードの各コーナーモーダルや分析画面に表示する。

### 処理フロー

```
Amazon EventBridge（定期実行）
    ↓
AWS Lambda
    ↓
Open-Meteo API（過去気象データ・無償API）
    ↓
Amazon DynamoDB（天気データを保存）
    ↓
Amazon API Gateway（ダッシュボードに提供）
    ↓
ダッシュボード（放送日の天気を取得・表示）
```

### 使用AWSサービス
| サービス | 用途 |
|---------|------|
| Amazon EventBridge | Lambda の定期起動 |
| AWS Lambda | Open-Meteo APIからのデータ取得・整形・DB保存 |
| Amazon DynamoDB | 取得した天気データの永続化 |
| Amazon API Gateway | ダッシュボードへの天気データ提供 |

### 外部API
- **Open-Meteo** (`https://open-meteo.com`) — 無償の過去気象データAPI
  - 対象地点: 名古屋（緯度・経度指定）
  - 取得データ: 最高気温・最低気温・天気コード等

### APIエンドポイント（ダッシュボード向け）
```
GET https://qajccvs8yd.execute-api.ap-northeast-1.amazonaws.com/weather
```

---

## 5. 視聴率データ連携（計画中）

### 概要
視聴率データをダッシュボードに自動連携する仕組み。現在は計画段階。

### 予定フロー

```
視聴率データ（元データ）
    ↓
Amazon S3（自動アップロード予定）
    ↓
ダッシュボード（S3 または API 経由で取得）
```

### 現在のステータス
- 現状: Excel/CSVから手動で変換したデモデータを `src/data/ratings_data.json` 等に格納
- 今後: S3への自動アップロードパイプラインを構築予定
- コード上のTODO: `genRatings → S3/DynamoDB APIへの差し替えポイント`

---

## 6. AIバックエンド（分析・検索）

### 概要
ダッシュボードのAI機能（意味検索・番組分析）を提供するバックエンドAPI。
AWS Lambda + Anthropic Claude API で構成。

### エンドポイント一覧
| パス | 機能 |
|------|------|
| `POST /api/search/semantic` | 意味検索（キーワード抽出・類似コーナー検索）|
| `POST /api/analysis/overview` | 全体統括分析（視聴率トレンドの総括）|
| `POST /api/analysis/highlight` | ハイライト分析（注目コーナーの深掘り）|
| `POST /api/analysis/conclusion` | 結論生成（分析結果のまとめ）|
| `POST /api/analysis/topic` | トピック分析（特定テーマに関する分析）|

### 使用AWSサービス
| サービス | 用途 |
|---------|------|
| AWS Lambda | APIリクエストの処理 |
| AWS Lambda Function URL | ダッシュボードからの直接呼び出し |

### 使用外部API
- **Anthropic Claude API** (`claude-sonnet-4-20250514`) — 自然言語による番組分析・検索

### Lambda Function URL
```
https://fecc7uq4uapxx37bmockq3gyvi0ibsdf.lambda-url.ap-northeast-1.amazonaws.com
```

---

## AWSサービス一覧まとめ

| サービス | 用途 |
|---------|------|
| Amazon S3 (`bangumi-info`) | 動画ファイル（`movie/ch{n}/`）・番組表CSV（`epg-all/`）の保存 |
| Amazon API Gateway | 動画アップロード用API（capture-system-api）・天気情報提供 |
| AWS Lambda | 動画アップロード処理（3関数）・番組表取得・天気取得・AI分析 |
| Amazon EventBridge | 定期実行スケジューラ（番組表・天気の定時取得）|
| Amazon DynamoDB | 天気データの永続化 |
| AWS IAM | Lambda実行ロールによるS3アクセス制御 |
| AWS Amplify | ダッシュボード（フロントエンド）のホスティング |
| Lambda Function URL | AIバックエンドAPIのエンドポイント提供 |

---

## 外部サービス・技術一覧まとめ

| サービス・技術 | 用途 |
|--------------|------|
| Anthropic Claude API | 番組分析・AI意味検索（`claude-sonnet-4-20250514`）|
| Google Gemini API | 動画コーナー分類 |
| Open-Meteo API | 過去気象データの取得（無償）|
| OpenAI Whisper | 動画音声の文字起こし |
| FFmpeg | 動画録画・エンコード（v4l2 + ALSA）|
| SQLite | capture-system のS3アップロード履歴管理（`s3_uploads.db`）|
