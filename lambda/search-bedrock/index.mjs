import { BedrockRuntimeClient, InvokeModelCommand } from "@aws-sdk/client-bedrock-runtime";

const client = new BedrockRuntimeClient({ region: "ap-northeast-1" });

const headers = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "POST,OPTIONS",
};

const invokeBedrock = async (messages, maxTokens = 8192) => {
  const command = new InvokeModelCommand({
    modelId: "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
    contentType: "application/json",
    accept: "application/json",
    body: JSON.stringify({
      anthropic_version: "bedrock-2023-05-31",
      max_tokens: maxTokens,
      messages,
    }),
  });
  const response = await client.send(command);
  const responseBody = JSON.parse(new TextDecoder().decode(response.body));
  return responseBody.content[0].text;
};

export const handler = async (event) => {
  if (event.requestContext?.http?.method === "OPTIONS") {
    return { statusCode: 200, headers, body: "" };
  }

  const body = JSON.parse(event.body || "{}");
  const path = event.rawPath;

  // ① セマンティック検索
  if (path === "/api/search/semantic") {
    const { query } = body;
    const prompt = `あなたは検索キーワード正規化AIです。
ユーザー入力に対して、同じ意味・同一人物・同一名称として検索に使える語だけを返してください。
禁止: 音が似ているだけの別単語、無関係な推測、別ジャンル語への変換
許可: 漢字変換、ひらがな/カタカナ変換、英語表記、軽微な誤字修正
出力ルール: JSON配列のみ、5個以内
ユーザー入力: ${query}`;
    const text = await invokeBedrock([{ role: "user", content: prompt }], 500);
    let keywords = [];
    try {
      const match = text.match(/\[[\s\S]*\]/);
      keywords = match ? JSON.parse(match[0]) : [];
    } catch { keywords = []; }
    keywords = [query, ...keywords].map(k => String(k).trim()).filter(Boolean);
    keywords = [...new Set(keywords)];
    return { statusCode: 200, headers, body: JSON.stringify({ keywords }) };
  }

  // ② Phase1: NBNの動き・競合各局の動き
  if (path === "/api/analysis/overview") {
    const { periodLabel, ctx } = body;
    const prompt = `あなたは名古屋テレビ(NBN)の視聴率担当アナリストです。以下は在名7局の${periodLabel || ""}の番組・コーナー別占拠率データです。NBN現場スタッフ向けの視聴率レポートを作成してください。

【データの読み方(必ず守ること)】
- 指標は占拠率(その時間帯に視聴中のテレビ全世帯のうち何%がその局を見ているか)
- 「裏局↓マイナス(NBNへ流入)」= 視聴者がNBNに流れてきた
- 「裏局↑プラス(NBNから流出)」= 視聴者がNBNから他局へ流れた

${ctx}

【出力形式(必ず守ること)】
・■ で始まる行はセクション見出しとして使用
・具体的な時刻・番組名・数値を必ず記載(例: 7:03 CTV「ZIP!」終了後に+1.4pt流入)
・→ を使って流れや因果を表現

以下の2セクションのみ作成してください(他のセクションは含めないこと):

■ NBN各番組の動き
(番組ごとに冒頭・中盤・終盤の流れ、ピーク・ボトムの時刻と数値、急上昇・急降下した箇所)

■ 競合各局の動き
(各局の主要番組の概況、NBNへの影響)`;

    const text = await invokeBedrock([{ role: "user", content: prompt }]);
    return { statusCode: 200, headers, body: JSON.stringify({ text }) };
  }

  // ③ Phase2: 流入流出・ハイライト (Phase1の続き)
  if (path === "/api/analysis/highlight") {
    const { prevPrompt, prevText, followup } = body;
    const highlightInstruction = followup ||
      `続けて、以下の4セクションを追加してください(総評・示唆は含めないこと)。

■ 流入・流出まとめ
(NBN視点で、どの局の何の番組終了/開始時に視聴者が動いたか、時刻と数値を箇条書きで)

■ 最高占拠率のタイミング
(何時何分・どのコーナー・何%・なぜ高かったか)

■ 急上昇コーナー TOP3
(コーナー名・時間帯・上昇幅・要因)

■ 急降下コーナー TOP3
(コーナー名・時間帯・下降幅・要因)`;

    const text = await invokeBedrock([
      { role: "user", content: prevPrompt },
      { role: "assistant", content: prevText },
      { role: "user", content: highlightInstruction },
    ]);
    return { statusCode: 200, headers, body: JSON.stringify({ text }) };
  }

  // ④ Phase3: 総評・今後の示唆
  if (path === "/api/analysis/conclusion") {
    const { combinedAnalysis } = body;
    const prompt = `以下はNBNの視聴率分析レポートです。この内容をもとに、現場スタッフ向けの総評と今後の示唆を作成してください。

${combinedAnalysis}

【出力形式(必ず守ること)】
・■ で始まる行はセクション見出しとして使用
・具体的な番組名・コーナー名・数値を使って記載
・2セクションのみ出力

■ 総評
(この期間のNBNのパフォーマンスを3〜4行で評価。良かった点・課題点を具体的に)

■ 今後の示唆
(編成・制作担当者への提言を箇条書きで3〜4点。具体的な番組名・コーナー名・時間帯を使って記載)`;

    const text = await invokeBedrock([{ role: "user", content: prompt }]);
    return { statusCode: 200, headers, body: JSON.stringify({ text }) };
  }

  // ⑤ トピック分析
  if (path === "/api/analysis/topic") {
    const { query, rows } = body;
    const prompt = `あなたは在名テレビ局（名古屋テレビ=NBN）の視聴率専門アナリストです。「${query}」というトピックに関する在名7局の過去放送実績を分析してください。

【データの読み方】
- 指標は占拠率
- 「裏局↓マイナス」= 視聴者が自局に流入
- 「裏局↑プラス」= 視聴者が自局から流出

【局別データ】
${rows}

以下の観点で400字程度でまとめてください：
1. このトピックは占拠率を取りやすいか
2. 各局の傾向の違い
3. 裏番組からの流入・流出パターン
4. 今後の放送への示唆`;

    const text = await invokeBedrock([{ role: "user", content: prompt }], 1000);
    return { statusCode: 200, headers, body: JSON.stringify({ text }) };
  }


// ⑥ 視聴率データダウンロード条件解析
if (path === "/api/download/parse") {
  const { text: userText, today, minDate, maxDate } = body;

  if (typeof userText !== "string" || !userText.trim()) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ error: "text is required" }),
    };
  }

  const prompt = `あなたはテレビ視聴率データの検索条件抽出AIです。
ユーザーの文章から、日付・放送局・番組名・時間帯・視聴率属性を抽出してください。

現在日: ${today}
選択可能期間: ${minDate} ～ ${maxDate}

【放送局ID】
NBN=メ〜テレ・名古屋テレビ
THK=東海テレビ
CTV=中京テレビ
CBC=CBCテレビ
TVA=テレビ愛知
NHK=NHK総合
NHKE=NHK Eテレ

【属性ID】
ALL=個人全体・全体・4才以上
U49=男女4～49才・コア
C=男女4～12才・子供
T=男女13～19才・ティーン
M1=男20～34才
M2=男35～49才
M3=男50～64才
M4=男65才以上
F1=女20～34才
F2=女35～49才
F3=女50～64才
F4=女65才以上

【重要】
- 番組名に属性表現を含めない
- 「チョコレートサムネットの全体」なら、
  program_queryは「チョコレートサムネット」、
  attributesは["ALL"]とする
- 番組名から放送時間を推測しない
- 時刻が明記されていない場合はnull
- 不明な項目は推測せずnullまたは空配列
- JSON以外は出力しない

【出力形式】
{
  "date_expression": string | null,
  "dates": string[],
  "stations": string[],
  "program_query": string | null,
  "start_time": string | null,
  "end_time": string | null,
  "attributes": string[]
}

ユーザー入力:
${userText}`;

  const aiText = await invokeBedrock(
    [{ role: "user", content: prompt }],
    1000
  );

  let conditions;
  try {
    const match = aiText.match(/\{[\s\S]*\}/);
    conditions = match ? JSON.parse(match[0]) : null;
  } catch {
    conditions = null;
  }

  if (!conditions) {
    return {
      statusCode: 422,
      headers,
      body: JSON.stringify({ error: "条件を解析できませんでした" }),
    };
  }

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({ conditions }),
  };
}

return { statusCode: 404, headers, body: JSON.stringify({ error: "Not found" }) };
};