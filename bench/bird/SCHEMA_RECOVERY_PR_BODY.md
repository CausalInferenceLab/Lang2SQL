## 실행 조건

저장된 #275 기준군과 새 #278 변경군의 BIRD Mini-Dev SQLite 비교다. 기준군은 #276에 변경군으로 게시된 `9274ff1f7fca3834701e1927f8c412407216f0b2`이고, 변경군은 #279의 `fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7`이다. 변경군에서 365문항 × 9개 조합, 총 3,285건을 실행했다. [기준군 PR](https://github.com/CausalInferenceLab/Lang2SQL/pull/276) · [제품 PR #279](https://github.com/CausalInferenceLab/Lang2SQL/pull/279)

평가기 `bench.bird.evaluate`의 원본 버전은 `1f47f2821559992717aeb23e1cd34ca489142300`다. `experiment/278-bird-schema-recovery`의 실행기 커밋은 `781fd3871a87afd689794204343b0d9e9a0eb601`다. 실행 전 `src/`를 제품 SHA와 대조했다. 조합별 실행기 소스 해시와 런타임을 설정에 남겼다. 두 GPT‑4.1 프로필에서 OpenAI SDK 3.51.0, GPT‑6 Luna에서 3.57.0을 사용했다.

Python 3.13.14, SQLite 3.53.1, LiteLLM 1.109.1, SQLAlchemy 2.0.50을 사용했다. 문항당 모델 API 호출 8회에 강제 조회와 보조 호출을 포함했다. 호출당 출력 4,096토큰, 동시성 조합당 4, API·문항·SQL 제한 120·480·30초와 최종 결과 상한 100,000행, 공급자 자동 재시도 0회를 적용했다.

A는 evidence를 `term_custom`으로 등록해 시스템 메시지에 주입했다. B는 같은 evidence를 질문에 첨부했고 D는 질문만 전달했다. 새 세션에서 `run_sql`과 `explore_schema`를 제공했다. A의 `term_custom`은 문항별 입력 준비에 사용했고, 표의 도구 수는 모델 응답에서 요청한 도구를 기준으로 집계했다. EX는 예측과 기준 SQL의 행 튜플 집합이 일치하면 1이다. `gpt-6-luna`에는 `reasoning_effort=none`을 전달했다. 모델별로 A, B, D 순서로 실행했고 세 모델은 동시에 실행했다.

데이터 리비전은 `38818fcbc46038129592680c51b2c8b597aac27c`, 보관한 manifest SHA-256은 `05d1414e7a355c486331506a8a9619428add7df6beae76789f8ce6fa647edb5b`다. 주석·DB별 해시와 채점기 및 SQLite 연결 해시를 manifest와 설정에 기록했다. [Manifest](results/schema-278-v1/manifest.json) · [의존성 잠금 파일과 패키지 버전](results/schema-278-v1/checks/)

변경군 첫 문항 시작부터 마지막 채점 파일 저장까지 UTC `2026-11-23T11:55:20.303771+00:00` ~ `2026-11-23T12:56:51.763233+00:00`다. 기준군 원본 설정·채점 파일 시각 범위는 UTC `2026-11-22T13:27:14.296561+00:00` ~ `2026-11-22T14:27:19.136688+00:00`다.

[BIRD EX 채점 규칙](https://github.com/bird-bench/mini_dev/blob/1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py#L17)

준비한 manifest가 #276 원본과 일치했고 기준 SQL 365/365건이 전수 검사를 통과했다. A/B/D의 기존 dry run은 각각 365/365건을 완료했다. [입력과 dry run 검사](results/schema-278-v1/checks/)

모의 테이블 조회로 첫 요청 auto, 최종 SQL·run_sql 보류, 강제 조회와 컬럼 응답 후 auto, 같은 응답의 조회·실행 요청을 확인했다. 모델별 강제 호출과 복귀 요청을 실호출했다. 실호출의 HTTP JSON에 `tool_choice`와 출력 상한을 확인했다. [모의·실호출 기록](results/schema-278-v1/checks/)

## 주요 지표

화살표는 기준군 → 변경군이다. 정확도의 분모는 계획 문항 수이며 미제출·실패도 포함한다. 변화폭은 반올림 전 변경군 정확도에서 기준군 정확도를 뺀 값이다. 전체 비교 쌍은 3,285/3,285건이다. 변경군 완료 3,285건, 채점 3,285건, API 오류 호출 1회, 재실행 0문항이다.

- A-openai-gpt41-mini 정답 수 65/365 → 69/365, 변화폭 +1.10%p
- A-openai-gpt41 정답 수 13/365 → 80/365, 변화폭 +18.36%p
- A-openai-gpt6-luna 정답 수 146/365 → 149/365, 변화폭 +0.82%p
- B-openai-gpt41-mini 정답 수 76/365 → 74/365, 변화폭 -0.55%p
- B-openai-gpt41 정답 수 15/365 → 79/365, 변화폭 +17.53%p
- B-openai-gpt6-luna 정답 수 160/365 → 146/365, 변화폭 -3.84%p
- D-openai-gpt41-mini 정답 수 63/365 → 52/365, 변화폭 -3.01%p
- D-openai-gpt41 정답 수 13/365 → 64/365, 변화폭 +13.97%p
- D-openai-gpt6-luna 정답 수 127/365 → 123/365, 변화폭 -1.10%p

| 프로필·조건 | #275 정답/전체 (정확도) | #278 정답/전체 (정확도) | 변화(%p) | #275만 정답 | #278만 정답 | 양쪽 정답 | 양쪽 오답 | 비교 쌍 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A-openai-gpt41-mini](results/schema-278-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 65/365 (17.81%) | 69/365 (18.90%) | +1.10 | 24 | 28 | 41 | 272 | 365 |
| [A-openai-gpt41](results/schema-278-v1/A-openai-gpt41/paired-scores.jsonl) | 13/365 (3.56%) | 80/365 (21.92%) | +18.36 | 4 | 71 | 9 | 281 | 365 |
| [A-openai-gpt6-luna](results/schema-278-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 146/365 (40.00%) | 149/365 (40.82%) | +0.82 | 36 | 39 | 110 | 180 | 365 |
| [B-openai-gpt41-mini](results/schema-278-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 76/365 (20.82%) | 74/365 (20.27%) | -0.55 | 31 | 29 | 45 | 260 | 365 |
| [B-openai-gpt41](results/schema-278-v1/B-openai-gpt41/paired-scores.jsonl) | 15/365 (4.11%) | 79/365 (21.64%) | +17.53 | 2 | 66 | 13 | 284 | 365 |
| [B-openai-gpt6-luna](results/schema-278-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 160/365 (43.84%) | 146/365 (40.00%) | -3.84 | 48 | 34 | 112 | 171 | 365 |
| [D-openai-gpt41-mini](results/schema-278-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 63/365 (17.26%) | 52/365 (14.25%) | -3.01 | 25 | 14 | 38 | 288 | 365 |
| [D-openai-gpt41](results/schema-278-v1/D-openai-gpt41/paired-scores.jsonl) | 13/365 (3.56%) | 64/365 (17.53%) | +13.97 | 3 | 54 | 10 | 298 | 365 |
| [D-openai-gpt6-luna](results/schema-278-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 127/365 (34.79%) | 123/365 (33.70%) | -1.10 | 37 | 33 | 90 | 205 | 365 |

### 실행 상태

| 프로필·조건 | 계획 | 시도 문항 | 총 시도 | 완료 | 채점 | API 오류 호출 | 중단 | 재실행 | 누락 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A-openai-gpt41-mini](results/schema-278-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [A-openai-gpt41](results/schema-278-v1/A-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [A-openai-gpt6-luna](results/schema-278-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B-openai-gpt41-mini](results/schema-278-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 1 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B-openai-gpt41](results/schema-278-v1/B-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B-openai-gpt6-luna](results/schema-278-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D-openai-gpt41-mini](results/schema-278-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D-openai-gpt41](results/schema-278-v1/D-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D-openai-gpt6-luna](results/schema-278-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 369 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 4 → 0 | 4 → 0 | 0 → 0 |

모의 응답과 별도 실호출은 본평가의 시도·호출·채점에서 제외했다. 기준군 Luna D의 29·69·146·287번은 #276이 남긴 최종 재실행 결과를 사용했다. 최초 중단 기록은 해당 조합의 `baseline/interruptions.jsonl`에 있다. API 오류 유형은 B-openai-gpt41-mini InternalServerError 1회다.

### 최종 SQL 제출과 실행

| 프로필·조건 | 최종 SQL 제출 | 미제출 | 형식 오류 | SQL 실행 성공 | 결과 불일치 | 컬럼 오류 | 기타 SQL 오류 | SQL 시간 초과 | 실행 실패 원인 미분류 | 채점기 오류 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A-openai-gpt41-mini](results/schema-278-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 311 → 337 | 54 → 28 | 8 → 1 | 302 → 330 | 237 → 261 | 7 → 4 | 1 → 3 | 1 → 0 | 0 → 0 | 0 → 0 |
| [A-openai-gpt41](results/schema-278-v1/A-openai-gpt41/paired-scores.jsonl) | 365 → 363 | 0 → 2 | 0 → 0 | 66 → 351 | 53 → 271 | 289 → 10 | 9 → 2 | 1 → 0 | 0 → 0 | 0 → 0 |
| [A-openai-gpt6-luna](results/schema-278-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 349 → 346 | 16 → 19 | 0 → 0 | 349 → 346 | 203 → 197 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B-openai-gpt41-mini](results/schema-278-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 307 → 331 | 58 → 34 | 8 → 0 | 303 → 323 | 227 → 249 | 4 → 4 | 0 → 2 | 0 → 2 | 0 → 0 | 0 → 0 |
| [B-openai-gpt41](results/schema-278-v1/B-openai-gpt41/paired-scores.jsonl) | 365 → 363 | 0 → 2 | 0 → 0 | 65 → 349 | 50 → 270 | 292 → 13 | 8 → 1 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B-openai-gpt6-luna](results/schema-278-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 350 → 351 | 15 → 14 | 0 → 0 | 350 → 351 | 190 → 205 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D-openai-gpt41-mini](results/schema-278-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 309 → 327 | 56 → 38 | 14 → 0 | 305 → 322 | 242 → 270 | 3 → 4 | 1 → 1 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D-openai-gpt41](results/schema-278-v1/D-openai-gpt41/paired-scores.jsonl) | 365 → 363 | 0 → 2 | 0 → 0 | 60 → 346 | 47 → 282 | 295 → 11 | 9 → 5 | 1 → 1 | 0 → 0 | 0 → 0 |
| [D-openai-gpt6-luna](results/schema-278-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 340 → 339 | 25 → 26 | 0 → 0 | 339 → 339 | 212 → 216 | 0 → 0 | 1 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |

SQL 실행 성공은 공식 정답·결과 불일치 및 실행 후 채점기 오류를 포함한다. 형식 오류는 미제출의 부분집합이다. 제출 파서가 여러 SQL 블록 또는 SELECT/WITH가 아닌 블록을 거부했거나 답변에 SQL 블록 시작 표시 또는 줄 시작의 SELECT/WITH가 남아 있는데 파싱에 실패한 경우로 재집계했다. 최종 컬럼 오류는 실패한 제출 SQL을 같은 읽기 전용 DB에서 `EXPLAIN QUERY PLAN`으로 준비해 `no such column`을 확인했다.

### 스키마 복구

| 프로필·조건 | run_sql 보류 | 최종 SQL 보류 | 같은 응답에서 컬럼 확보 | 강제 조회 요청 | 강제 요청 후 컬럼 확보 | 전체 응답 중 컬럼 확보 | 재작성 제출/문항 | 재작성 정답/제출 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A-openai-gpt41-mini](results/schema-278-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 360 | 0 | 0 | 360 | 360 | 429 | 333/365 | 69/333 |
| [A-openai-gpt41](results/schema-278-v1/A-openai-gpt41/paired-scores.jsonl) | 3 | 358 | 0 | 361 | 361 | 426 | 359/365 | 78/359 |
| [A-openai-gpt6-luna](results/schema-278-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 400 | 0/365 | 0/0 |
| [B-openai-gpt41-mini](results/schema-278-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 345 | 0 | 0 | 345 | 345 | 447 | 312/365 | 67/312 |
| [B-openai-gpt41](results/schema-278-v1/B-openai-gpt41/paired-scores.jsonl) | 3 | 359 | 0 | 362 | 362 | 427 | 360/365 | 78/360 |
| [B-openai-gpt6-luna](results/schema-278-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 400 | 0/365 | 0/0 |
| [D-openai-gpt41-mini](results/schema-278-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 345 | 0 | 0 | 345 | 345 | 436 | 307/365 | 48/307 |
| [D-openai-gpt41](results/schema-278-v1/D-openai-gpt41/paired-scores.jsonl) | 0 | 362 | 0 | 362 | 362 | 417 | 360/365 | 64/360 |
| [D-openai-gpt6-luna](results/schema-278-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 433 | 0/365 | 0/0 |

복구 표는 #278 관측값이다. 보류와 조회 수는 한 문항에서 여러 번 증가할 수 있다. 컬럼 확보는 `tables_with_columns`가 한 개 이상 반환된 모델 응답 단위로 집계했다. 재작성 SQL 제출은 앞서 SQL이 보류되고 실제 컬럼을 얻은 문항에서 최종 SQL을 제출한 경우다.

### 호출, 토큰, 문항 시간

| 프로필·조건 | 모델 호출 | 도구 요청 | 도구 실행 | run_sql 요청 | run_sql 실행 | 도구 DB 실행 | 최종 SQL DB 실행 | 입력 토큰 | 출력 토큰 | P50 (초) | P95 (초) | 8회 도달 | 예산 소진 미제출 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A-openai-gpt41-mini](results/schema-278-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 1,614 → 1,688 | 1,342 → 1,287 | 1,342 → 927 | 809 → 857 | 809 → 497 | 미수집 → 497 | 311 → 337 | 2,532,758 → 2,432,881 | 249,309 → 263,154 | 6.27 → 9.13 | 19.81 → 27.44 | 33 → 41 | 27 → 27 |
| [A-openai-gpt41](results/schema-278-v1/A-openai-gpt41/paired-scores.jsonl) | 375 → 1,465 | 10 → 744 | 10 → 741 | 5 → 317 | 5 → 314 | 미수집 → 312 | 365 → 363 | 305,147 → 1,803,890 | 69,478 → 206,616 | 1.31 → 5.98 | 3.54 → 12.12 | 0 → 4 | 0 → 2 |
| [A-openai-gpt6-luna](results/schema-278-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 1,345 → 1,348 | 999 → 1,003 | 999 → 1,003 | 599 → 601 | 599 → 601 | 미수집 → 600 | 349 → 346 | 2,089,879 → 2,056,516 | 161,206 → 164,826 | 5.01 → 5.70 | 13.92 → 16.04 | 23 → 26 | 16 → 19 |
| [B-openai-gpt41-mini](results/schema-278-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 1,580 → 1,695 | 1,303 → 1,271 | 1,303 → 926 | 768 → 821 | 768 → 476 | 미수집 → 474 | 307 → 331 | 2,401,026 → 2,372,773 | 243,829 → 277,019 | 6.10 → 9.51 | 20.18 → 29.86 | 40 → 41 | 32 → 33 |
| [B-openai-gpt41](results/schema-278-v1/B-openai-gpt41/paired-scores.jsonl) | 365 → 1,473 | 0 → 743 | 0 → 740 | 0 → 316 | 0 → 313 | 미수집 → 312 | 365 → 363 | 266,710 → 1,745,079 | 69,753 → 208,471 | 1.30 → 5.91 | 3.38 → 12.83 | 0 → 4 | 0 → 2 |
| [B-openai-gpt6-luna](results/schema-278-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 1,386 → 1,339 | 1,039 → 991 | 1,039 → 991 | 610 → 588 | 610 → 588 | 미수집 → 586 | 350 → 351 | 2,032,774 → 1,902,144 | 164,733 → 160,798 | 5.46 → 5.75 | 15.62 → 14.56 | 24 → 14 | 15 → 14 |
| [D-openai-gpt41-mini](results/schema-278-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 1,449 → 1,703 | 1,162 → 1,274 | 1,162 → 929 | 674 → 835 | 674 → 490 | 미수집 → 485 | 309 → 327 | 2,062,627 → 2,306,112 | 226,901 → 264,842 | 5.48 → 9.71 | 21.30 → 24.96 | 27 → 43 | 17 → 38 |
| [D-openai-gpt41](results/schema-278-v1/D-openai-gpt41/paired-scores.jsonl) | 365 → 1,431 | 0 → 704 | 0 → 704 | 0 → 286 | 0 → 286 | 미수집 → 286 | 365 → 363 | 255,686 → 1,602,505 | 68,318 → 195,668 | 1.26 → 5.76 | 3.47 → 11.07 | 0 → 6 | 0 → 2 |
| [D-openai-gpt6-luna](results/schema-278-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 1,453 → 1,423 | 1,116 → 1,084 | 1,116 → 1,084 | 679 → 650 | 679 → 650 | 미수집 → 648 | 340 → 339 | 2,058,090 → 2,051,338 | 161,995 → 167,178 | 5.41 → 6.13 | 14.57 → 19.30 | 28 → 31 | 25 → 26 |

재집계에는 원본 JSONL을 사용했다. #275 도구 요청은 assistant transcript, 실행은 도구 결과 또는 도구 실행 이벤트에서 계산했다. #278의 도구 요청은 API 응답, 실행은 도구 레지스트리 호출에서 계산했다. 도구 DB 실행은 `run_sql` 경로에서 SQLite 커서 실행에 도달한 횟수다. #275의 실제 도구 DB 실행과 스키마 복구 이벤트는 미수집이다. 최종 DB 실행은 각 평가기의 제출 SQL 평가 시도 수다. 스키마 검사, 기준 SQL과 진단용 EXPLAIN 실행은 그 수에서 제외한다. 토큰은 응답 사용량 합계다. P50·P95는 기록된 문항 경과 시간의 정렬값을 선형 보간했다. Spider2 경과 시간에는 최종 SQL 내보내기가 포함되고 BIRD 경과 시간은 생성 단계에서 측정했다.

## 문항별 상태 전환

아래 표는 기준군의 최종 `no such column` 문항을 분모로 집계했다. 모든 시작 상태의 전환 행렬은 비교 JSON의 `state_transitions`에 있다. [전체 전환 행렬](results/schema-278-v1/comparison.json)

| 프로필·조건 | #275 컬럼 오류 | 정답 | 결과 불일치 | 컬럼 오류 | 기타 SQL 오류 | SQL 시간 초과 | 호출 예산 소진 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [A-openai-gpt41-mini](results/schema-278-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 7 | 2 | 3 | 0 | 0 | 0 | 2 |
| [A-openai-gpt41](results/schema-278-v1/A-openai-gpt41/paired-scores.jsonl) | 289 | 62 | 216 | 9 | 0 | 0 | 2 |
| [A-openai-gpt6-luna](results/schema-278-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| [B-openai-gpt41-mini](results/schema-278-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 4 | 0 | 2 | 0 | 0 | 0 | 2 |
| [B-openai-gpt41](results/schema-278-v1/B-openai-gpt41/paired-scores.jsonl) | 292 | 60 | 217 | 12 | 1 | 0 | 2 |
| [B-openai-gpt6-luna](results/schema-278-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| [D-openai-gpt41-mini](results/schema-278-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 3 | 0 | 2 | 0 | 0 | 0 | 1 |
| [D-openai-gpt41](results/schema-278-v1/D-openai-gpt41/paired-scores.jsonl) | 295 | 47 | 230 | 11 | 4 | 1 | 2 |
| [D-openai-gpt6-luna](results/schema-278-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## 상세 기록과 재현 명령

| 프로필·조건 | 프로필 요청 ID | #275 응답 모델 ID | #278 응답 모델 ID | 모델 ID 미수집 호출 |
| --- | --- | --- | --- | --- |
| [A-openai-gpt41-mini](results/schema-278-v1/A-openai-gpt41-mini/paired-scores.jsonl) | `openai/gpt-4.1-mini-2025-04-14` | `gpt-4.1-mini-2025-04-14` 1,614회 | `gpt-4.1-mini-2025-04-14` 1,688회 | 0 → 0 |
| [A-openai-gpt41](results/schema-278-v1/A-openai-gpt41/paired-scores.jsonl) | `openai/gpt-4.1-2025-04-14` | `gpt-4.1-2025-04-14` 375회 | `gpt-4.1-2025-04-14` 1,465회 | 0 → 0 |
| [A-openai-gpt6-luna](results/schema-278-v1/A-openai-gpt6-luna/paired-scores.jsonl) | `openai/gpt-6-luna` | `gpt-6-luna` 1,345회 | `gpt-6-luna` 1,348회 | 0 → 0 |
| [B-openai-gpt41-mini](results/schema-278-v1/B-openai-gpt41-mini/paired-scores.jsonl) | `openai/gpt-4.1-mini-2025-04-14` | `gpt-4.1-mini-2025-04-14` 1,580회 | `gpt-4.1-mini-2025-04-14` 1,694회 | 0 → 1 |
| [B-openai-gpt41](results/schema-278-v1/B-openai-gpt41/paired-scores.jsonl) | `openai/gpt-4.1-2025-04-14` | `gpt-4.1-2025-04-14` 365회 | `gpt-4.1-2025-04-14` 1,473회 | 0 → 0 |
| [B-openai-gpt6-luna](results/schema-278-v1/B-openai-gpt6-luna/paired-scores.jsonl) | `openai/gpt-6-luna` | `gpt-6-luna` 1,386회 | `gpt-6-luna` 1,339회 | 0 → 0 |
| [D-openai-gpt41-mini](results/schema-278-v1/D-openai-gpt41-mini/paired-scores.jsonl) | `openai/gpt-4.1-mini-2025-04-14` | `gpt-4.1-mini-2025-04-14` 1,449회 | `gpt-4.1-mini-2025-04-14` 1,703회 | 0 → 0 |
| [D-openai-gpt41](results/schema-278-v1/D-openai-gpt41/paired-scores.jsonl) | `openai/gpt-4.1-2025-04-14` | `gpt-4.1-2025-04-14` 365회 | `gpt-4.1-2025-04-14` 1,431회 | 0 → 0 |
| [D-openai-gpt6-luna](results/schema-278-v1/D-openai-gpt6-luna/paired-scores.jsonl) | `openai/gpt-6-luna` | `gpt-6-luna` 1,453회 | `gpt-6-luna` 1,423회 | 0 → 0 |

| 프로필·조건 | #275 문항별 기록 · 설정 · 채점 | #278 문항별 기록 · 설정 · 채점 |
| --- | --- | --- |
| [A-openai-gpt41-mini](results/schema-278-v1/A-openai-gpt41-mini/paired-scores.jsonl) | [원본](results/schema-278-v1/A-openai-gpt41-mini/baseline/records.jsonl) · [설정](results/schema-278-v1/A-openai-gpt41-mini/baseline/settings.json) · [채점](results/schema-278-v1/A-openai-gpt41-mini/baseline/scores.json) | [원본](results/schema-278-v1/A-openai-gpt41-mini/treatment/records.jsonl) · [설정](results/schema-278-v1/A-openai-gpt41-mini/treatment/settings.json) · [채점](results/schema-278-v1/A-openai-gpt41-mini/treatment/scores.json) |
| [A-openai-gpt41](results/schema-278-v1/A-openai-gpt41/paired-scores.jsonl) | [원본](results/schema-278-v1/A-openai-gpt41/baseline/records.jsonl) · [설정](results/schema-278-v1/A-openai-gpt41/baseline/settings.json) · [채점](results/schema-278-v1/A-openai-gpt41/baseline/scores.json) | [원본](results/schema-278-v1/A-openai-gpt41/treatment/records.jsonl) · [설정](results/schema-278-v1/A-openai-gpt41/treatment/settings.json) · [채점](results/schema-278-v1/A-openai-gpt41/treatment/scores.json) |
| [A-openai-gpt6-luna](results/schema-278-v1/A-openai-gpt6-luna/paired-scores.jsonl) | [원본](results/schema-278-v1/A-openai-gpt6-luna/baseline/records.jsonl) · [설정](results/schema-278-v1/A-openai-gpt6-luna/baseline/settings.json) · [채점](results/schema-278-v1/A-openai-gpt6-luna/baseline/scores.json) | [원본](results/schema-278-v1/A-openai-gpt6-luna/treatment/records.jsonl) · [설정](results/schema-278-v1/A-openai-gpt6-luna/treatment/settings.json) · [채점](results/schema-278-v1/A-openai-gpt6-luna/treatment/scores.json) |
| [B-openai-gpt41-mini](results/schema-278-v1/B-openai-gpt41-mini/paired-scores.jsonl) | [원본](results/schema-278-v1/B-openai-gpt41-mini/baseline/records.jsonl) · [설정](results/schema-278-v1/B-openai-gpt41-mini/baseline/settings.json) · [채점](results/schema-278-v1/B-openai-gpt41-mini/baseline/scores.json) | [원본](results/schema-278-v1/B-openai-gpt41-mini/treatment/records.jsonl) · [설정](results/schema-278-v1/B-openai-gpt41-mini/treatment/settings.json) · [채점](results/schema-278-v1/B-openai-gpt41-mini/treatment/scores.json) |
| [B-openai-gpt41](results/schema-278-v1/B-openai-gpt41/paired-scores.jsonl) | [원본](results/schema-278-v1/B-openai-gpt41/baseline/records.jsonl) · [설정](results/schema-278-v1/B-openai-gpt41/baseline/settings.json) · [채점](results/schema-278-v1/B-openai-gpt41/baseline/scores.json) | [원본](results/schema-278-v1/B-openai-gpt41/treatment/records.jsonl) · [설정](results/schema-278-v1/B-openai-gpt41/treatment/settings.json) · [채점](results/schema-278-v1/B-openai-gpt41/treatment/scores.json) |
| [B-openai-gpt6-luna](results/schema-278-v1/B-openai-gpt6-luna/paired-scores.jsonl) | [원본](results/schema-278-v1/B-openai-gpt6-luna/baseline/records.jsonl) · [설정](results/schema-278-v1/B-openai-gpt6-luna/baseline/settings.json) · [채점](results/schema-278-v1/B-openai-gpt6-luna/baseline/scores.json) | [원본](results/schema-278-v1/B-openai-gpt6-luna/treatment/records.jsonl) · [설정](results/schema-278-v1/B-openai-gpt6-luna/treatment/settings.json) · [채점](results/schema-278-v1/B-openai-gpt6-luna/treatment/scores.json) |
| [D-openai-gpt41-mini](results/schema-278-v1/D-openai-gpt41-mini/paired-scores.jsonl) | [원본](results/schema-278-v1/D-openai-gpt41-mini/baseline/records.jsonl) · [설정](results/schema-278-v1/D-openai-gpt41-mini/baseline/settings.json) · [채점](results/schema-278-v1/D-openai-gpt41-mini/baseline/scores.json) | [원본](results/schema-278-v1/D-openai-gpt41-mini/treatment/records.jsonl) · [설정](results/schema-278-v1/D-openai-gpt41-mini/treatment/settings.json) · [채점](results/schema-278-v1/D-openai-gpt41-mini/treatment/scores.json) |
| [D-openai-gpt41](results/schema-278-v1/D-openai-gpt41/paired-scores.jsonl) | [원본](results/schema-278-v1/D-openai-gpt41/baseline/records.jsonl) · [설정](results/schema-278-v1/D-openai-gpt41/baseline/settings.json) · [채점](results/schema-278-v1/D-openai-gpt41/baseline/scores.json) | [원본](results/schema-278-v1/D-openai-gpt41/treatment/records.jsonl) · [설정](results/schema-278-v1/D-openai-gpt41/treatment/settings.json) · [채점](results/schema-278-v1/D-openai-gpt41/treatment/scores.json) |
| [D-openai-gpt6-luna](results/schema-278-v1/D-openai-gpt6-luna/paired-scores.jsonl) | [원본](results/schema-278-v1/D-openai-gpt6-luna/baseline/records.jsonl) · [설정](results/schema-278-v1/D-openai-gpt6-luna/baseline/settings.json) · [채점](results/schema-278-v1/D-openai-gpt6-luna/baseline/scores.json) | [원본](results/schema-278-v1/D-openai-gpt6-luna/treatment/records.jsonl) · [설정](results/schema-278-v1/D-openai-gpt6-luna/treatment/settings.json) · [채점](results/schema-278-v1/D-openai-gpt6-luna/treatment/scores.json) |

[전체 집계 JSON](results/schema-278-v1/comparison.json) · [평가 입력 manifest](results/schema-278-v1/manifest.json)

```bash
# #276의 잠금 파일과 두 SDK 버전 사용
git worktree add --detach "$DEPS" 8c1cb0b
uv sync --project "$DEPS" --locked --extra benchmark --python 3.13.14
PY="$DEPS/.venv/bin/python"
uv pip install --python "$PY" --target "$SDK_351" --no-deps openai==3.51.0
export PYTHONPATH="$PWD/src:$PWD"
export LITELLM_LOCAL_MODEL_COST_MAP=True LITELLM_LOG=ERROR

"$PY" -m bench.bird.evaluate prepare --source "$SOURCE" --db-dir "$DB_DIR" \
  --db-revision 8132aacfe61ecec7198a62e93cb8512dac89b1d6b9922fe3c7bb94b13f317d8f \
  --output "$RUN/prepared"
cmp "$RUN/prepared/manifest.json" bench/bird/results/schema-278-v1/manifest.json
"$PY" -m bench.bird.evaluate score --gold-only --source "$SOURCE" \
  --db-dir "$DB_DIR" --output "$RUN/prepared"

git diff --exit-code fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7 -- src/
lane() (
  PROFILE=$1
  if test "$PROFILE" != openai-gpt6-luna; then
    export PYTHONPATH="$PYTHONPATH:$SDK_351"
  fi
  for CONDITION in A B D; do
    OUT="$RUN/$CONDITION-$PROFILE"
    "$PY" -m bench.bird.evaluate run --condition "$CONDITION" --profile "$PROFILE" \
      --prepared "$RUN/prepared" --db-dir "$DB_DIR" --env-file "$CREDENTIALS" \
      --product-commit fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7 --output "$OUT" || return
    "$PY" -m bench.bird.evaluate score --source "$SOURCE" --db-dir "$DB_DIR" \
      --output "$OUT" || return
  done
)
lane openai-gpt41-mini &
MINI_PID=$!
lane openai-gpt41 &
FULL_PID=$!
lane openai-gpt6-luna &
LUNA_PID=$!
wait "$MINI_PID"
wait "$FULL_PID"
wait "$LUNA_PID"
```

`SOURCE`는 고정 SQLite JSONL 파일이고 `DB_DIR`은 같은 DB 스냅샷 11개를 담은 경로다.
`DEPS`, `SDK_351`, `RUN`, `CREDENTIALS`는 각각 의존성 워크트리, GPT‑4.1 SDK 설치 경로,
새 실행 루트, 기존 인증 설정 파일의 절대 경로다.
이어서 실행할 때 같은 조합의 `run`에 `--resume`을 추가한다.


`BASELINE`은 저장된 #275 원본이며 아래에서는 보고서에 동봉한 JSONL을 사용한다. `PUBLISHED`는 기존 PR의 문항별 점수 파일 사본이다. [게시 기준군 문항별 점수](https://github.com/jhpark324/Lang2SQL/blob/0fb4f34d86d9ea845c30e7b5d1f2952ef30f02f8/bench/bird/results/schema-275-v1/paired-scores.jsonl)

```bash
BASELINE=bench/bird/results/schema-278-v1
PUBLISHED="$BASELINE/published-baseline-scores.jsonl"
"$PY" bench/report_schema_recovery.py bird --baseline "$BASELINE" \
  --treatment "$RUN" --db-dir "$DB_DIR" --published "$PUBLISHED" \
  --output "$RUN/comparison"
```

[평가 보고서](SCHEMA_RECOVERY_RESULTS.md)
