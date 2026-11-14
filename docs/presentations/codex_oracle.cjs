const pptxgen = require("pptxgenjs");
const path = require("path");
const fs = require("fs");
const p = new pptxgen();
p.layout = "LAYOUT_WIDE";
p.author = "Lang2SQL";
p.subject = "Codex 기반 Oracle 데모와 실제 검증";
p.title = "Codex × Lang2SQL × Oracle";
p.company = "PseudoLab";
p.lang = "ko-KR";
p.theme = {
  headFontFace: "Pretendard",
  bodyFontFace: "Pretendard",
  lang: "ko-KR",
};
const C = {
  ink: "17211F",
  muted: "596B65",
  green: "007F64",
  mint: "DFF5EC",
  red: "B83D32",
  pink: "FBEDEA",
  white: "FFFFFF",
  bg: "F4F7F6",
};
function text(s, t, x, y, w, h, size = 20, color = C.ink, bold = false) {
  s.addText(t, {
    x,
    y,
    w,
    h,
    fontFace: "Pretendard",
    fontSize: size,
    color,
    bold,
    margin: 0,
    breakLine: false,
    isTextBox: true,
    vertAlign: "mid",
  });
}
function box(s, x, y, w, h, fill = C.bg) {
  s.addShape(p.ShapeType.roundRect, {
    x,
    y,
    w,
    h,
    rectRadius: 0.15,
    radius: 0.15,
    line: { color: fill, transparency: 100 },
    fill: { color: fill },
  });
}
function slide(kicker, title, dark = false) {
  const s = p.addSlide();
  s.background = { color: dark ? C.ink : C.white };
  text(
    s,
    kicker.toUpperCase(),
    0.6,
    0.45,
    11,
    0.3,
    11,
    dark ? "80DBC0" : C.green,
    true,
  );
  text(s, title, 0.6, 1, 12.1, 1.15, 34, dark ? C.white : C.ink, true);
  text(
    s,
    "LANG2SQL  /  CODEX × ORACLE",
    0.6,
    7.02,
    10,
    0.2,
    9,
    dark ? "A0B4AD" : C.muted,
  );
  text(
    s,
    String(p._slides.length).padStart(2, "0"),
    12,
    6.98,
    0.7,
    0.3,
    11,
    dark ? C.white : C.muted,
  );
  return s;
}
function card(s, x, y, w, label, value, body, fill = C.bg) {
  box(s, x, y, w, 3.15, fill);
  text(s, label, x + 0.25, y + 0.25, w - 0.5, 0.35, 14, C.muted, true);
  text(s, value, x + 0.25, y + 0.8, w - 0.5, 0.8, 31, C.green, true);
  text(s, body, x + 0.25, y + 1.85, w - 0.5, 1, 18);
}
function notes(s, t) {
  s.addNotes(t);
}
function table(s, rows, y = 2.45, width = 12.1) {
  s.addTable(rows, {
    x: 0.6,
    y,
    w: width,
    rowH: 0.65,
    border: { type: "solid", pt: 1, color: "FFFFFF" },
    fill: C.bg,
    color: C.ink,
    fontFace: "Pretendard",
    fontSize: 17,
    margin: 0.15,
    colW: rows[0].length === 3 ? [3, 5.5, 3.6] : undefined,
    bold: false,
    autoPage: false,
  });
}
let s = slide(
  "ORACLE TECH DEMO • 2026.11",
  "Codex가 묻고,\nLang2SQL이 Oracle에서 확인합니다",
  true,
);
text(
  s,
  "업무 정의 · 실제 SQL · DB 권한을 한 흐름으로",
  0.65,
  3.1,
  11.8,
  0.6,
  25,
  "C5E7DB",
);
["Codex", "Lang2SQL MCP", "Oracle Database"].forEach((v, i) => {
  box(s, 0.65 + i * 4.2, 4.5, 3.65, 1.2, i === 2 ? C.red : C.green);
  text(s, v, 0.85 + i * 4.2, 4.77, 3.25, 0.6, 22, C.white, true);
});
notes(
  s,
  "오프닝 45초. 이 발표는 Codex가 자연어 요청을 실행하고 Lang2SQL이 업무 정의와 데이터 접근을 연결하는 실제 Oracle 데모입니다. 모든 데이터는 합성입니다. 제품 일반 정확도를 주장하지 않습니다.",
);
s = slide("01 • WHY", "SQL이 실행돼도, 답이 맞았다는 뜻은 아닙니다");
card(
  s,
  0.6,
  2.65,
  3.8,
  "정의 문제",
  "매출 ≠ 매출",
  "영업은 주문 총액\n재무는 할인·반품 차감",
);
card(
  s,
  4.75,
  2.65,
  3.8,
  "모호함 문제",
  "충성고객?",
  "기준이 없으면\n계산 전에 질문",
);
card(
  s,
  8.9,
  2.65,
  3.8,
  "접근 문제",
  "누가 묻는가",
  "같은 급여 질문도\nDB 계정별 권한 적용",
  C.pink,
);
notes(
  s,
  "1분. Oracle 청중에게 자연어를 SQL로 바꾸는 것만으로는 부족하다는 문제를 먼저 제시합니다. 오늘 보여줄 것은 부서 정의, 확인 질문, 실제 DB 권한의 세 가지입니다.",
);
s = slide("02 • DEMO CONTRACT", "오늘 확인할 세 가지 약속");
table(s, [
  [
    { text: "장면", options: { bold: true } },
    { text: "눈으로 확인할 것", options: { bold: true } },
    { text: "합격 기준", options: { bold: true } },
  ],
  ["같은 질문, 다른 부서", "적용 정의 → SQL → 숫자", "200만 / 170만 원"],
  ["정의 없는 충성고객", "질문을 되돌려주는 도구 호출", "업무 SQL 0회"],
  ["같은 질문, 다른 권한", "HR 결과 / 영업 접근 불가", "급여 결과 유출 없음"],
]);
text(
  s,
  "Codex의 말과 Oracle의 실행 기록을 함께 보여줍니다.",
  0.65,
  5.75,
  12,
  0.6,
  23,
  C.green,
  true,
);
notes(
  s,
  "45초. 시연을 보기 전에 합격 기준을 공유합니다. 숫자만 비교하지 않고 어떤 정의와 SQL로 나온 값인지 같이 확인합니다.",
);
s = slide(
  "03 • ARCHITECTURE",
  "Codex는 판단하고, Lang2SQL은 도구를 제공합니다",
);
["사용자 질문", "Codex CLI", "Lang2SQL MCP", "Oracle"].forEach((v, i) => {
  box(s, 0.6 + i * 3.2, 2.65, 2.55, 1.1, i === 3 ? C.pink : C.mint);
  text(
    s,
    v,
    0.75 + i * 3.2,
    2.92,
    2.25,
    0.5,
    21,
    i === 3 ? C.red : C.green,
    true,
  );
  if (i < 3) text(s, "→", 3.25 + i * 3.2, 2.91, 0.5, 0.5, 28, C.muted);
});
text(s, "의도 해석\n도구 선택\n한국어 답변", 3.85, 4.2, 2.5, 1.5, 20);
text(s, "팀별 정의\nSQL 안전 검사\n감사 기록", 7.05, 4.2, 2.5, 1.5, 20);
text(s, "실제 데이터\nSELECT 권한\n접속 사용자", 10.25, 4.2, 2.45, 1.5, 20);
text(
  s,
  "내부 LLM 루프를 중첩하지 않는 1회 요청 구조",
  0.6,
  6.35,
  12,
  0.35,
  18,
  C.muted,
);
notes(
  s,
  "1분. Codex가 유일한 에이전트 오케스트레이터입니다. 기존 Lang2SQL의 LLM 루프는 이 경로에서 호출하지 않습니다. MCP는 기존 도구와 안전 파이프라인을 연결합니다. Oracle 계정 권한은 프롬프트와 별개입니다.",
);
s = slide("04 • TOOL CONTRACT", "도구는 네 개, 신원은 실행 시작 시 고정");
table(s, [
  [
    { text: "도구", options: { bold: true } },
    { text: "책임", options: { bold: true } },
    { text: "제약", options: { bold: true } },
  ],
  ["get_context", "부서 정의 · SQL dialect 읽기", "SQL보다 먼저"],
  ["explore_schema", "허용된 스키마 확인", "DB 계정 범위"],
  ["run_sql", "안전 검사 후 Oracle 실행", "SELECT 권한"],
  ["ask_user", "부족한 업무 기준 질문", "이 요청 중단"],
]);
notes(
  s,
  "1분. 정의나 계정을 바꾸는 도구는 노출하지 않습니다. get_context를 읽기 전에 SQL을 요청하면 서버가 거부합니다. ask_user나 권한 오류가 stop_turn을 반환하면 뒤의 호출은 서버가 차단합니다. 환경변수 신원은 로컬 입력이며 운영 인증은 아직 별도 과제입니다.",
);
s = slide("05 • LIVE DEMO 1", "“2026년 10월 매출 얼마야?”");
card(
  s,
  0.6,
  2.6,
  5.85,
  "영업 / L2S_DEMO_SALES",
  "2,000,000원",
  "주문 금액 합계\nSUM(gross_amount)",
  C.mint,
);
card(
  s,
  6.85,
  2.6,
  5.85,
  "재무 / L2S_DEMO_FINANCE",
  "1,700,000원",
  "주문 금액 − 할인 금액 − 반품 금액\n정의에 따라 순매출 집계",
);
text(
  s,
  "실제 Codex 실행 · 실제 Oracle 결과 · 동일한 질문",
  0.65,
  6.15,
  12,
  0.5,
  19,
  C.muted,
);
notes(
  s,
  "2분. 실제 데모에서는 sales와 finance 결과 JSON의 answer, calls의 get_context와 run_sql, db_user를 차례로 엽니다. 날짜 조건이 동일하고 업무 정의에 따라 집계식만 달라졌음을 보여줍니다. 데이터는 합성이며 9월과 11월 행도 있어 날짜 누락을 검출합니다.",
);
s = slide("06 • EVIDENCE", "답변 뒤에 실행한 SQL과 계정이 남습니다");
box(s, 0.6, 2.5, 8.15, 3.85, C.ink);
text(
  s,
  "SELECT SUM(gross_amount) AS revenue_krw\nFROM l2s_demo_owner.orders\nWHERE ordered_at >= DATE '2026-10-01'\n  AND ordered_at < DATE '2026-11-01'\nFETCH FIRST 1000 ROWS ONLY",
  0.9,
  2.9,
  7.6,
  2.7,
  19,
  "DFF5EC",
);
text(s, "실행 근거", 9.15, 2.65, 3.2, 0.5, 23, C.green, true);
text(
  s,
  "db_user\nL2S_DEMO_SALES\n\n실제 조회 값\n2,000,000",
  9.15,
  3.45,
  3.3,
  2.55,
  21,
);
notes(
  s,
  "1분. 최초 검증 JSON에서 가져온 SQL입니다. 답변만 채점하지 않고 audit에 남은 성공 SQL을 같은 고정 fixture와 같은 DB 계정으로 다시 실행해 숫자를 비교합니다. 결과 파일에는 도구 호출과 Codex 이벤트도 들어 있습니다.",
);
s = slide("07 • LIVE DEMO 2", "충성고객의 기준이 없으면, 먼저 묻습니다");
box(s, 0.6, 2.45, 12.1, 1, C.bg);
text(
  s,
  "사용자  “2026년 10월 충성고객 매출을 알려줘.”",
  0.9,
  2.68,
  11.5,
  0.5,
  23,
);
box(s, 0.6, 3.85, 8.35, 2.2, C.mint);
text(s, "Codex → ask_user", 0.9, 4.1, 7.7, 0.4, 18, C.green, true);
text(
  s,
  "“구매 횟수 또는 누적 구매액의\n기준값과 산정 기간을 알려주세요.”",
  0.9,
  4.8,
  7.65,
  0.9,
  24,
);
text(s, "0회", 9.5, 4.05, 3, 1, 57, C.green, true);
text(s, "업무 SQL 실행", 9.5, 5.25, 3, 0.4, 19);
notes(
  s,
  "1분. 정의 없는 충성고객은 임의로 3회 구매처럼 추정하지 않고 확인합니다. 모호함을 감지하는 것은 모델 판단입니다. ask_user 이후 추가 실행을 막는 부분은 MCP 서버의 결정적 제어입니다. 다음 답변은 새 요청으로 시작하며 자동 대화 기억은 없습니다.",
);
s = slide("08 • LIVE DEMO 3", "“부서별 급여 평균” — 결과는 권한을 따릅니다");
card(
  s,
  0.6,
  2.6,
  5.85,
  "HR / 조회 권한 있음",
  "조회 성공",
  "sales 3,000,000\nfinance 4,000,000",
  C.mint,
);
card(
  s,
  6.85,
  2.6,
  5.85,
  "영업 / 조회 권한 없음",
  "접근 불가",
  "Oracle ORA-00942 확인\n급여 데이터 반환 없음",
  C.pink,
);
text(
  s,
  "권한 또는 객체 존재 문제를 구분할 수 없으면 단정하지 않습니다.",
  0.65,
  6.1,
  12,
  0.5,
  19,
  C.muted,
);
notes(
  s,
  "1분30초. HR과 sales는 별도 Oracle 사용자입니다. 영업의 급여 조회를 실제 DB가 차단합니다. ORA-00942는 권한 부족과 객체 부재를 모두 나타낼 수 있어 테이블이 없다고 단정하지 않습니다. 별도 SQL probe로도 권한 차단을 검증합니다. 금액 단위는 fixture에 정의되지 않아 답변에 원 단위를 임의 부여하지 않았습니다.",
);
s = slide("09 • CONTROL", "멈춰야 할 때는 서버가 다음 호출을 막습니다");
["질문 요청 / 접근 오류", "stop_turn = true", "추가 도구 호출 차단"].forEach(
  (v, i) => {
    box(s, 0.6 + i * 4.2, 2.7, 3.7, 1.35, i === 2 ? C.pink : C.mint);
    text(
      s,
      v,
      0.85 + i * 4.2,
      3.04,
      3.2,
      0.6,
      21,
      i === 2 ? C.red : C.green,
      true,
    );
  },
);
text(
  s,
  "프로세스 단위 중단 상태 + 호출 직렬화",
  0.65,
  4.65,
  12,
  0.55,
  27,
  C.ink,
  true,
);
text(
  s,
  "모델의 중단 지시 준수에만 의존하지 않음\n이미 먼저 시작된 SQL의 취소나 DB 서버 시간 제한은 별도 범위",
  0.65,
  5.5,
  12,
  0.9,
  20,
  C.muted,
);
notes(
  s,
  "1분. 같은 프로세스에서 stop_turn 뒤에는 get_context를 포함한 모든 도구가 거부됩니다. 중단 상태 reset 도구는 없습니다. 한 요청당 새 프로세스라 다음 요청은 독립적으로 시작합니다. 동시 호출은 순서대로 처리되지만 먼저 시작된 질의를 뒤늦게 취소하지는 않습니다.",
);
const verifiedFile = path.join(
  __dirname,
  "../../bench/codex/results/verified.json",
);
const report = JSON.parse(
  fs.readFileSync(
    fs.existsSync(verifiedFile)
      ? verifiedFile
      : path.join(__dirname, "../../bench/codex/results/first.json"),
    "utf8",
  ),
);
const passed = report.cases.filter((c) => c.passed).length;
const total = report.cases.length;
s = slide("10 • VERIFICATION", "실제 Codex와 Oracle로 실행해 확인했습니다");
text(s, `${passed}/${total}`, 0.65, 2.45, 5.4, 1.35, 76, C.green, true);
text(s, `고정 5개 시나리오 × ${total / 5}회`, 0.7, 4.1, 5.2, 0.6, 24);
text(
  s,
  "통과 여부는 DB 결과·도구 호출·권한으로 판정",
  0.7,
  5.05,
  5.2,
  1.1,
  21,
  C.muted,
);
box(s, 6.55, 2.5, 6.15, 3.7);
text(s, "검증 환경", 6.85, 2.82, 5.55, 0.4, 20, C.green, true);
text(
  s,
  "Codex CLI 0.166.0 / CLI 기본 모델\nMCP SDK 1.30.0\nOracle Free 23.9 / 합성 데이터\npytest 188개 통과",
  6.85,
  3.5,
  5.55,
  1.8,
  22,
);
text(
  s,
  "일반 정확도 100%나 독립 질문 15개의 평가를 의미하지 않습니다.",
  0.65,
  6.5,
  12,
  0.3,
  14,
  C.muted,
);
notes(
  s,
  "1분. 결과 파일에서 숫자를 자동으로 가져왔습니다. 동일 5개 시나리오를 반복한 기능 검증입니다. Codex CLI 기본 모델을 사용했으므로 모델 ID를 추정해서 표기하지 않습니다. 기존 gpt-4.1-mini 평가 수치와 분리했습니다.",
);
s = slide(
  "11 • IMPLEMENTATION",
  "새 실행 경로를 만들고, 검증된 구성요소를 재사용",
);
table(s, [
  [
    { text: "변경", options: { bold: true } },
    { text: "구현 내용", options: { bold: true } },
    { text: "효과", options: { bold: true } },
  ],
  [
    "Codex 실행기",
    "exec + JSONL 완료 검사 + timeout",
    "실패를 성공으로 보고하지 않음",
  ],
  ["stdio MCP", "고정 신원 · 4개 도구 · 중단 상태", "Codex와 안전 경계 연결"],
  [
    "기존 모듈 재사용",
    "정의 저장소 · SafetyPipeline · Oracle adapter",
    "SQL 경로 중복 제거",
  ],
  ["평가 · 발표", "실제 실행 JSON + 재현 문서 + PPT", "주장과 실행 근거 연결"],
]);
notes(
  s,
  "45초. 기존 CLI와 Discord를 삭제하는 대규모 변경 대신 새 Codex 경로를 분리하고 핵심 도구를 재사용했습니다. FakeLLM 필드는 레거시 context 타입을 채우기 위해 존재하지만 이 경로에서는 호출되지 않습니다.",
);
s = slide("12 • ORACLE DISCUSSION", "다음 PoC에서 Oracle과 확인할 것");
card(
  s,
  0.6,
  2.6,
  3.8,
  "데이터와 의미",
  "업무 질문 20개",
  "익명화된 스키마\n부서별 지표 정의와 정답",
);
card(
  s,
  4.75,
  2.6,
  3.8,
  "권한과 신원",
  "계정 매핑",
  "SSO 사용자 → 팀 → DB\nVPD·행/컬럼 정책 검증",
);
card(
  s,
  8.9,
  2.6,
  3.8,
  "운영 기준",
  "허용 비용",
  "질의 시간·행수·비용 제한\n감사·보존·실패 복구",
  C.pink,
);
notes(
  s,
  "1분. Oracle에게 요청할 것은 막연한 협업이 아니라 대표 업무 질문, 권한 매핑, 운영 제한 조건입니다. VPD와 행·컬럼 정책은 아직 검증하지 않았습니다. 현재 로컬 신원 주입을 운영 인증으로 오해하지 않도록 구분합니다.",
);
s = slide(
  "13 • TAKEAWAY",
  "보여줄 것은 “SQL 생성”을 넘어선\n업무 정의와 권한을 따르는 실행입니다",
  true,
);
text(
  s,
  "같은 질문에는 맞는 정의를\n모호한 질문에는 확인을\n권한 밖 데이터에는 중단을",
  0.65,
  3.15,
  11.8,
  2,
  32,
  "DFF5EC",
  true,
);
box(s, 0.65, 5.75, 12, 0.65, C.green);
text(
  s,
  "Codex × Lang2SQL MCP × Oracle",
  0.9,
  5.9,
  11.5,
  0.35,
  21,
  C.white,
  true,
);
notes(
  s,
  "30초. 데모 세 장면을 다시 연결하고 PoC 범위 합의로 끝냅니다. 정확도 일반화나 운영 보안 전체가 완성됐다고 주장하지 않습니다.",
);
s = slide("APPENDIX A • RUNBOOK", "시연은 준비 → 실행 → 증거 확인 순서로");
table(s, [
  [
    { text: "단계", options: { bold: true } },
    { text: "실행", options: { bold: true } },
    { text: "확인", options: { bold: true } },
  ],
  [
    "환경",
    "codex login / uv sync --extra codex --extra oracle",
    "Codex 인증 · SDK",
  ],
  ["데이터", "Oracle 컨테이너 + 공용 seed", "합성 주문 · 별도 읽기 계정"],
  ["검증", "bench/codex_oracle_demo.py --repeats 3", "JSON 생성 · exit 0"],
  ["발표", "answer → calls → rows → db_user", "화면과 기록 대조"],
]);
text(
  s,
  "상세 명령: docs/codex_oracle_demo.md",
  0.65,
  6.15,
  12,
  0.4,
  18,
  C.green,
);
notes(
  s,
  "시연 전 Docker 상태와 Codex 인증을 확인합니다. seed는 새 disposable DB에서만 실행합니다. 이미 있는 사용자는 삭제하지 않습니다. 실시간 호출 장애가 생기면 저장된 실제 실행 JSON을 재생하되 녹화·기록임을 명시합니다.",
);
s = slide("APPENDIX B • SCOPE & SOURCES", "검증한 것과, 아직 검증하지 않은 것");
box(s, 0.6, 2.5, 5.85, 3.65, C.mint);
text(s, "검증 완료", 0.9, 2.8, 5.25, 0.5, 24, C.green, true);
text(
  s,
  "Codex → MCP → Oracle 실제 연결\n팀별 정의·재질문·테이블 권한\nstdio handshake·중단 상태·실행 실패\n합성 데이터의 고정 5개 사례",
  0.9,
  3.65,
  5.25,
  1.85,
  20,
);
box(s, 6.85, 2.5, 5.85, 3.65, C.bg);
text(s, "후속 검증", 7.15, 2.8, 5.25, 0.5, 24, C.ink, true);
text(
  s,
  "운영 인증·계정 라우팅\nVPD·행/컬럼 권한·민감정보 관리\n질의 비용·서버 실행시간 제한\n넓은 질문군·다중 대화",
  7.15,
  3.65,
  5.25,
  1.85,
  20,
);
notes(
  s,
  "정본은 docs/codex_oracle_demo.md와 bench/codex/results JSON입니다. 공식 참고: https://learn.chatgpt.com/docs/extend/mcp.md?surface=cli ; https://learn.chatgpt.com/docs/non-interactive-mode.md ; https://github.com/modelcontextprotocol/python-sdk ; https://docs.sqlalchemy.org/en/20/dialects/oracle.html . 기존 mypy baseline 오류 19개는 이번 Codex 변경과 별개로 남아 있습니다.",
);
p.writeFile({
  fileName: path.join(__dirname, "Oracle_Lang2SQL_Codex_v2.pptx"),
});
