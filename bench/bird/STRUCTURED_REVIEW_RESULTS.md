# BIRD SQLite 평가 · SQL 검토 JSON Schema와 12회 호출 한도

## 실행 조건

기준군은 #290에 게시한 #289 하네스 `0a7d85d8155b7202d45999631f526e028ddc1e1e`의 실행이다. 변경군은 #289에서 분기한 `846e3d3b966adff759b83c60384f6e0619058fef`이다. 검토용 모델 호출에만 `response_format=json_schema`와 `strict: true`를 적용했고, 문항당 모델 호출 한도를 8회에서 12회로 늘렸다. 두 변경을 함께 적용한 실행이다. [기준 실행 #290](https://github.com/CausalInferenceLab/Lang2SQL/pull/290) · [제품 #289](https://github.com/CausalInferenceLab/Lang2SQL/pull/289) · [실험 코드](https://github.com/jhpark324/Lang2SQL/tree/e0ebfa4e4874d6a61841382ec7a3446059e122a9)

BIRD Mini-Dev SQLite 365문항 × 세 모델 × A·B·D, 총 3,285건을 생성하고 채점했다. A는 evidence를 팀 레이어에 등록해 시스템 메시지에 넣었고, B는 같은 evidence를 질문에 첨부했다. D는 질문과 공통 SQL 제출 지시문을 사용했다. 모델별로 A, B, D 순서로 실행했고 세 모델을 병렬로 실행했다.

| 모델 | 요청 ID | 응답 ID | OpenAI SDK |
| --- | --- | --- | --- |
| GPT‑4.1 mini | `openai/gpt-4.1-mini-2025-04-14` | `gpt-4.1-mini-2025-04-14` | 3.51.0 |
| GPT‑4.1 | `openai/gpt-4.1-2025-04-14` | `gpt-4.1-2025-04-14` | 3.51.0 |
| GPT‑6 Luna | `openai/gpt-6-luna` | `gpt-6-luna` | 3.57.0 |

GPT‑6 Luna는 기준 실행과 같은 API 모델 ID와 `reasoning_effort=none`을 사용했다. Python 3.13.14, SQLite 3.53.1, LiteLLM 1.109.1, SQLAlchemy 2.0.50을 사용했다. 조합당 동시성 4, 호출당 출력 4,096토큰, 호출·문항·SQL 제한 120·480·30초, 채점 행 제한 100,000행, 공급자 자동 재시도 0회를 적용했다. 검토용 하위 모델 호출과 메인 모델 호출을 한도에 함께 계산했다. [런타임과 기준 SQL 확인 기록](results/structured-review-12-v1/checks/)

데이터 리비전은 `38818fcbc46038129592680c51b2c8b597aac27c`이고 manifest SHA-256은 `05d1414e7a355c486331506a8a9619428add7df6beae76789f8ce6fa647edb5b`이다. 주석과 SQLite DB 11개를 대조했다. 기준 SQL 365/365건이 실행됐다. EX는 제출한 SQL과 기준 SQL의 행 튜플 집합이 같으면 1이며, 계획 문항 전체가 분모다. [Manifest](results/structured-review-12-v1/manifest.json) · [BIRD EX 규칙](https://github.com/bird-bench/mini_dev/blob/1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py#L17)

문항 생성의 UTC 시각 범위는 #290이 `2026-11-24T17:46:17.931043+00:00`~`2026-11-24T19:19:00.959474+00:00`, 변경군이 `2026-11-25T02:27:53.652963+00:00`~`2026-11-25T03:32:21.632906+00:00`이다. 조합마다 한 번 실행했다. 실행 시점에 따른 모델 응답과 API 지연의 변동이 관측값에 포함될 수 있다.

## 실행 정확도와 호출

화살표는 #290 → JSON Schema·12회다. 합산 정확도는 모델별로 A·B·D 정답 수를 더해 1,095건으로 나눴다. 변화폭은 반올림 전 정확도를 비교했다.

| 모델 | #290 EX | JSON Schema·12회 EX | 변화 (%p) | SQL 제출 | 예산 소진 미제출 | 모델 호출 | 검토 판정 오류 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GPT‑4.1 mini | 116/1,095 (10.59%) | 234/1,095 (21.37%) | +10.78 | 613 → 1,007 | 433 → 48 | 7,602 → 6,815 | 2,027 → 0 |
| GPT‑4.1 | 246/1,095 (22.47%) | 247/1,095 (22.56%) | +0.09 | 927 → 1,008 | 129 → 45 | 6,561 → 7,061 | 2 → 0 |
| GPT‑6 Luna | 331/1,095 (30.23%) | 366/1,095 (33.42%) | +3.20 | 833 → 963 | 260 → 131 | 6,888 → 7,606 | 2 → 0 |

| 조건 · 모델 | #290 EX | JSON Schema·12회 EX | 변화 (%p) | SQL 미제출 | 예산 소진 미제출 | 모델 호출 | 문항 P50 (초) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 43/365 (11.78%) | 79/365 (21.64%) | +9.86 | 151 → 30 | 141 → 16 | 2,557 → 2,335 | 18.41 → 10.47 |
| [A · GPT‑4.1](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) | 89/365 (24.38%) | 76/365 (20.82%) | -3.56 | 54 → 27 | 41 → 11 | 2,174 → 2,297 | 9.57 → 7.99 |
| [A · GPT‑6 Luna](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 116/365 (31.78%) | 120/365 (32.88%) | +1.10 | 87 → 41 | 87 → 40 | 2,266 → 2,476 | 11.63 → 9.21 |
| [B · GPT‑4.1 mini](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 44/365 (12.05%) | 89/365 (24.38%) | +12.33 | 169 → 29 | 153 → 16 | 2,528 → 2,243 | 18.67 → 9.61 |
| [B · GPT‑4.1](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) | 91/365 (24.93%) | 106/365 (29.04%) | +4.11 | 57 → 30 | 43 → 15 | 2,189 → 2,369 | 9.42 → 8.60 |
| [B · GPT‑6 Luna](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 120/365 (32.88%) | 133/365 (36.44%) | +3.56 | 87 → 39 | 86 → 39 | 2,285 → 2,532 | 12.46 → 10.41 |
| [D · GPT‑4.1 mini](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 29/365 (7.95%) | 66/365 (18.08%) | +10.14 | 162 → 29 | 139 → 16 | 2,517 → 2,237 | 17.41 → 10.39 |
| [D · GPT‑4.1](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) | 66/365 (18.08%) | 65/365 (17.81%) | -0.27 | 57 → 30 | 45 → 19 | 2,198 → 2,395 | 9.83 → 8.16 |
| [D · GPT‑6 Luna](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 95/365 (26.03%) | 113/365 (30.96%) | +4.93 | 88 → 52 | 87 → 52 | 2,337 → 2,598 | 12.65 → 10.46 |

| 조건 · 모델 | #290만 정답 | 변경군만 정답 | 양쪽 정답 | 양쪽 오답 | 대응 쌍 |
| --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 18 | 54 | 25 | 268 | 365 |
| [A · GPT‑4.1](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) | 36 | 23 | 53 | 253 | 365 |
| [A · GPT‑6 Luna](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 29 | 33 | 87 | 216 | 365 |
| [B · GPT‑4.1 mini](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 14 | 59 | 30 | 262 | 365 |
| [B · GPT‑4.1](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) | 24 | 39 | 67 | 235 | 365 |
| [B · GPT‑6 Luna](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 36 | 49 | 84 | 196 | 365 |
| [D · GPT‑4.1 mini](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 9 | 46 | 20 | 290 | 365 |
| [D · GPT‑4.1](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) | 23 | 22 | 43 | 277 | 365 |
| [D · GPT‑6 Luna](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 26 | 44 | 69 | 226 | 365 |

## 실행 상태

변경군에서 3,285건을 모두 채점했다. API 오류 호출은 0회다.

| 조건 · 모델 | 완료 | 채점 | 총 시도 | API 오류 | 중단 | 재실행 | 누락 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [A · GPT‑4.1](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [A · GPT‑6 Luna](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B · GPT‑4.1 mini](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B · GPT‑4.1](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B · GPT‑6 Luna](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1 mini](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑6 Luna](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |

## 검토 구조와 호출 한도

변경군은 검토 호출 4,861회 중 4,861회에 JSON Schema를 전달했다. GPT‑4.1 mini의 검토 판정 오류는 2,027회에서 0회로 바뀌었다.

| 조건 · 모델 | 검토 호출 | 변경군 JSON Schema 요청 | pass | revise | 판정 오류 | 검토 호출 예산 차단 | 검토 통과·실행·제출 일치 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 761 → 477 | 477 | 65 → 361 | 31 → 116 | 665 → 0 | 51 → 5 | 32 → 224 |
| [A · GPT‑4.1](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) | 468 → 495 | 495 | 356 → 363 | 111 → 132 | 1 → 0 | 8 → 5 | 160 → 178 |
| [A · GPT‑6 Luna](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 577 → 633 | 633 | 343 → 380 | 232 → 253 | 2 → 0 | 35 → 13 | 230 → 251 |
| [B · GPT‑4.1 mini](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 790 → 465 | 465 | 59 → 354 | 29 → 111 | 702 → 0 | 61 → 3 | 36 → 226 |
| [B · GPT‑4.1](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) | 446 → 502 | 502 | 340 → 365 | 106 → 137 | 0 → 0 | 10 → 3 | 180 → 205 |
| [B · GPT‑6 Luna](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 583 → 649 | 649 | 331 → 379 | 252 → 270 | 0 → 0 | 36 → 13 | 223 → 260 |
| [D · GPT‑4.1 mini](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 751 → 455 | 455 | 68 → 346 | 23 → 109 | 660 → 0 | 60 → 7 | 40 → 221 |
| [D · GPT‑4.1](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) | 466 → 510 | 510 | 329 → 361 | 136 → 149 | 1 → 0 | 11 → 4 | 175 → 203 |
| [D · GPT‑6 Luna](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 587 → 675 | 675 | 340 → 385 | 247 → 290 | 0 → 0 | 35 → 16 | 215 → 235 |

mini A 92번의 검토 판정 오류는 3 → 0회, 모델 호출은 8 → 5회, EX는 0 → 1이었다. [문항 92 기록](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl#L72)

검토 통과·실행·제출 일치는 앞뒤 공백을 제거한 같은 SQL에 `pass` 판정이 나왔고, 별도 모델 응답이 `run_sql`을 요청해 실제 DB 실행에 성공한 뒤 같은 후보를 최종 제출한 문항이다.

| 조건 · 모델 | 한도 도달 (8 → 12) | 9~12회 사용 문항 | 9~12회 사용 정답 | 기존 예산 소진 → 정답 |
| --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 186 → 24 | 56 | 8 | 32 |
| [A · GPT‑4.1](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) | 69 → 17 | 51 | 8 | 7 |
| [A · GPT‑6 Luna](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 126 → 46 | 74 | 9 | 16 |
| [B · GPT‑4.1 mini](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 186 → 21 | 51 | 8 | 34 |
| [B · GPT‑4.1](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) | 63 → 20 | 51 | 10 | 9 |
| [B · GPT‑6 Luna](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 125 → 48 | 83 | 22 | 28 |
| [D · GPT‑4.1 mini](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 179 → 18 | 47 | 3 | 30 |
| [D · GPT‑4.1](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) | 69 → 22 | 55 | 4 | 7 |
| [D · GPT‑6 Luna](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 141 → 60 | 92 | 10 | 21 |

9~12회 사용 정답은 변경군에서 8회를 넘게 호출하고 정답을 제출한 문항이다. 기존 예산 소진 → 정답은 같은 조건·문항 ID의 상태 전환이다. [조합별 전체 상태 전환](results/structured-review-12-v1/comparison.json)

## SQL 제출과 실행

| 조건 · 모델 | 제출 | 형식 오류 | 실행 성공 | 결과 불일치 | 컬럼 오류 | 기타 SQL 오류 | SQL 시간 초과 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 214 → 335 | 3 → 0 | 206 → 331 | 163 → 252 | 5 → 2 | 2 → 2 | 1 → 0 |
| [A · GPT‑4.1](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) | 311 → 338 | 3 → 5 | 310 → 337 | 221 → 261 | 0 → 0 | 0 → 0 | 1 → 1 |
| [A · GPT‑6 Luna](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 278 → 324 | 0 → 0 | 277 → 324 | 161 → 204 | 0 → 0 | 0 → 0 | 1 → 0 |
| [B · GPT‑4.1 mini](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 196 → 336 | 10 → 2 | 191 → 334 | 147 → 245 | 4 → 0 | 1 → 1 | 0 → 1 |
| [B · GPT‑4.1](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) | 308 → 335 | 5 → 5 | 307 → 332 | 216 → 226 | 0 → 1 | 0 → 2 | 1 → 0 |
| [B · GPT‑6 Luna](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 278 → 326 | 0 → 0 | 278 → 326 | 158 → 193 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1 mini](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 203 → 336 | 12 → 3 | 200 → 331 | 171 → 265 | 3 → 2 | 0 → 3 | 0 → 0 |
| [D · GPT‑4.1](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) | 308 → 335 | 1 → 1 | 302 → 332 | 236 → 267 | 3 → 0 | 2 → 1 | 1 → 2 |
| [D · GPT‑6 Luna](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 277 → 313 | 0 → 0 | 277 → 313 | 182 → 200 | 0 → 0 | 0 → 0 | 0 → 0 |

실행 성공은 정답, 결과 불일치 및 실행 후 채점기 오류를 포함한다. 컬럼 오류는 같은 읽기 전용 DB에서 `EXPLAIN QUERY PLAN`으로 확인했다.

## 도구 호출, 토큰, 문항 지연

| 조건 · 모델 | 도구 요청 | 도구 실행 | run_sql 요청 | run_sql 실행 | 도구 DB 실행 | 최종 SQL DB 실행 | 입력 토큰 | 출력 토큰 | 토큰 미기록 호출 | P50 (초) | P95 (초) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 1,558 → 1,481 | 1,462 → 1,381 | 283 → 510 | 187 → 410 | 184 → 408 | 214 → 335 | 3,788,711 → 4,043,874 | 511,710 → 394,312 | 0 → 0 | 18.41 → 10.47 | 38.56 → 32.36 |
| [A · GPT‑4.1](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) | 1,337 → 1,391 | 1,334 → 1,388 | 364 → 377 | 361 → 374 | 360 → 369 | 311 → 338 | 3,371,051 → 3,787,392 | 336,402 → 364,689 | 0 → 0 | 9.57 → 7.99 | 19.87 → 20.14 |
| [A · GPT‑6 Luna](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 1,412 → 1,518 | 1,412 → 1,518 | 336 → 390 | 336 → 390 | 336 → 390 | 278 → 324 | 3,607,720 → 4,292,497 | 293,388 → 342,520 | 0 → 0 | 11.63 → 9.21 | 21.82 → 25.11 |
| [B · GPT‑4.1 mini](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 1,525 → 1,423 | 1,486 → 1,375 | 247 → 482 | 208 → 434 | 208 → 434 | 196 → 336 | 3,638,645 → 3,715,052 | 532,372 → 371,114 | 0 → 0 | 18.67 → 9.61 | 39.46 → 27.90 |
| [B · GPT‑4.1](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) | 1,281 → 1,371 | 1,280 → 1,371 | 346 → 391 | 345 → 391 | 341 → 389 | 308 → 335 | 3,260,485 → 3,801,172 | 333,208 → 373,504 | 0 → 0 | 9.42 → 8.60 | 20.29 → 20.87 |
| [B · GPT‑6 Luna](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 1,423 → 1,558 | 1,423 → 1,558 | 324 → 403 | 324 → 403 | 324 → 403 | 278 → 326 | 3,533,067 → 4,224,495 | 297,264 → 341,788 | 0 → 0 | 12.46 → 10.41 | 22.89 → 25.49 |
| [D · GPT‑4.1 mini](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 1,529 → 1,434 | 1,483 → 1,386 | 278 → 492 | 232 → 444 | 232 → 443 | 203 → 336 | 3,542,975 → 3,538,961 | 515,233 → 347,318 | 0 → 0 | 17.41 → 10.39 | 36.49 → 25.68 |
| [D · GPT‑4.1](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) | 1,299 → 1,422 | 1,299 → 1,422 | 342 → 391 | 342 → 391 | 339 → 389 | 308 → 335 | 3,228,044 → 3,764,009 | 340,036 → 370,213 | 0 → 0 | 9.83 → 8.16 | 19.36 → 18.01 |
| [D · GPT‑6 Luna](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 1,472 → 1,612 | 1,472 → 1,612 | 344 → 411 | 344 → 411 | 343 → 410 | 277 → 313 | 3,554,078 → 4,305,621 | 294,878 → 350,115 | 0 → 0 | 12.65 → 10.46 | 21.82 → 27.09 |

문항 지연은 하네스 생성 경과 시간이다. SQLite 커서 실행에 도달한 `run_sql`을 도구 DB 실행으로 셌다. 최종 SQL DB 실행은 채점기가 제출 SQL 실행을 시도한 횟수다.

## 스키마 복구

| 조건 · 모델 | run_sql 보류 | 최종 SQL 보류 | 같은 응답 컬럼 확보 | 강제 조회 | 강제 조회 후 컬럼 확보 | 컬럼 확보 응답 | 재작성 제출 | 재작성 정답 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 96 → 100 | 2 → 1 | 0 → 0 | 98 → 101 | 98 → 101 | 461 → 485 | 53 → 95 | 11 → 21 |
| [A · GPT‑4.1](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) | 3 → 3 | 62 → 64 | 0 → 0 | 65 → 67 | 65 → 67 | 475 → 492 | 52 → 65 | 7 → 9 |
| [A · GPT‑6 Luna](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 464 → 482 | 0 → 0 | 0 → 0 |
| [B · GPT‑4.1 mini](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 39 → 48 | 1 → 0 | 0 → 0 | 40 → 48 | 40 → 48 | 424 → 469 | 19 → 45 | 3 → 11 |
| [B · GPT‑4.1](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) | 1 → 0 | 134 → 124 | 0 → 0 | 135 → 125 | 135 → 124 | 474 → 469 | 118 → 119 | 25 → 33 |
| [B · GPT‑6 Luna](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 480 → 492 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1 mini](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 46 → 48 | 0 → 0 | 0 → 0 | 46 → 48 | 46 → 48 | 435 → 475 | 27 → 43 | 3 → 6 |
| [D · GPT‑4.1](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) | 0 → 0 | 115 → 114 | 0 → 0 | 115 → 114 | 115 → 114 | 473 → 504 | 94 → 108 | 17 → 15 |
| [D · GPT‑6 Luna](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 505 → 506 | 0 → 0 | 0 → 0 |

보류와 조회는 호출 단위다. 컬럼 확보는 컬럼이 반환된 모델 응답 단위이고, 재작성 제출과 정답은 문항 단위다.

## 실행 기록과 재현

| 조건 · 모델 | #290 원본 | 변경군 원본과 대응 기록 |
| --- | --- | --- |
| A · GPT‑4.1 mini | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/A-openai-gpt41-mini) | [설정](results/structured-review-12-v1/A-openai-gpt41-mini/settings.json) · [채점](results/structured-review-12-v1/A-openai-gpt41-mini/scores.json) · [실행 기록](results/structured-review-12-v1/A-openai-gpt41-mini/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/A-openai-gpt41-mini/paired-scores.jsonl) |
| A · GPT‑4.1 | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/A-openai-gpt41) | [설정](results/structured-review-12-v1/A-openai-gpt41/settings.json) · [채점](results/structured-review-12-v1/A-openai-gpt41/scores.json) · [실행 기록](results/structured-review-12-v1/A-openai-gpt41/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/A-openai-gpt41/paired-scores.jsonl) |
| A · GPT‑6 Luna | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/A-openai-gpt6-luna) | [설정](results/structured-review-12-v1/A-openai-gpt6-luna/settings.json) · [채점](results/structured-review-12-v1/A-openai-gpt6-luna/scores.json) · [실행 기록](results/structured-review-12-v1/A-openai-gpt6-luna/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/A-openai-gpt6-luna/paired-scores.jsonl) |
| B · GPT‑4.1 mini | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/B-openai-gpt41-mini) | [설정](results/structured-review-12-v1/B-openai-gpt41-mini/settings.json) · [채점](results/structured-review-12-v1/B-openai-gpt41-mini/scores.json) · [실행 기록](results/structured-review-12-v1/B-openai-gpt41-mini/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/B-openai-gpt41-mini/paired-scores.jsonl) |
| B · GPT‑4.1 | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/B-openai-gpt41) | [설정](results/structured-review-12-v1/B-openai-gpt41/settings.json) · [채점](results/structured-review-12-v1/B-openai-gpt41/scores.json) · [실행 기록](results/structured-review-12-v1/B-openai-gpt41/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/B-openai-gpt41/paired-scores.jsonl) |
| B · GPT‑6 Luna | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/B-openai-gpt6-luna) | [설정](results/structured-review-12-v1/B-openai-gpt6-luna/settings.json) · [채점](results/structured-review-12-v1/B-openai-gpt6-luna/scores.json) · [실행 기록](results/structured-review-12-v1/B-openai-gpt6-luna/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/B-openai-gpt6-luna/paired-scores.jsonl) |
| D · GPT‑4.1 mini | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/D-openai-gpt41-mini) | [설정](results/structured-review-12-v1/D-openai-gpt41-mini/settings.json) · [채점](results/structured-review-12-v1/D-openai-gpt41-mini/scores.json) · [실행 기록](results/structured-review-12-v1/D-openai-gpt41-mini/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/D-openai-gpt41-mini/paired-scores.jsonl) |
| D · GPT‑4.1 | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/D-openai-gpt41) | [설정](results/structured-review-12-v1/D-openai-gpt41/settings.json) · [채점](results/structured-review-12-v1/D-openai-gpt41/scores.json) · [실행 기록](results/structured-review-12-v1/D-openai-gpt41/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/D-openai-gpt41/paired-scores.jsonl) |
| D · GPT‑6 Luna | [기준 실행](https://github.com/jhpark324/Lang2SQL/tree/151939f9adcee2c073832eaf5df0e161cf73aa52/bench/bird/results/review-289-v1/D-openai-gpt6-luna) | [설정](results/structured-review-12-v1/D-openai-gpt6-luna/settings.json) · [채점](results/structured-review-12-v1/D-openai-gpt6-luna/scores.json) · [실행 기록](results/structured-review-12-v1/D-openai-gpt6-luna/records.jsonl.gz) · [대응 점수](results/structured-review-12-v1/D-openai-gpt6-luna/paired-scores.jsonl) |

본 실행의 평가기 커밋은 `a4dd2de41179ad32ad887337fbe604c44e91e2a5`다. 설정의 모델 ID·질문 순서·채점 파일 해시와 문항별 대응을 검사했다. 평가기 변경은 호출 한도 설정과 검토 `response_format` 전달이다. [실험 워크트리 소스](https://github.com/jhpark324/Lang2SQL/tree/e0ebfa4e4874d6a61841382ec7a3446059e122a9) · [지표 집계 소스](https://github.com/jhpark324/Lang2SQL/blob/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/report_schema_recovery.py)

`SOURCE`와 `DB_DIR`에는 manifest의 주석 파일과 DB 스냅샷을 지정한다. `CREDENTIALS`는 로컬 API 설정 파일이다. `DEPS`, `BASELINE`, `METRICS`, `RUN`, `SDK_351`에는 작업 경로를 지정한다. 아래 명령은 실험 코드 워크트리에서 실행한다.

```bash
git fetch origin refs/pull/276/head refs/pull/280/head refs/pull/290/head
git worktree add --detach "$DEPS" 8c1cb0b6b1ba68f179ed27f27779943c41a7b1d4
git worktree add --detach "$BASELINE" 151939f9adcee2c073832eaf5df0e161cf73aa52
git worktree add --detach "$METRICS" f47da5c669a2c691d59d90fd236e227f5db0da96
uv sync --project "$DEPS" --locked --extra benchmark --python 3.13.14
PY="$DEPS/.venv/bin/python"
uv pip install --python "$PY" --target "$SDK_351" --no-deps openai==3.51.0
export PYTHONPATH="$PWD/src:$PWD"
export LITELLM_LOCAL_MODEL_COST_MAP=True LITELLM_LOG=ERROR

"$PY" -m bench.bird.evaluate prepare --source "$SOURCE" --db-dir "$DB_DIR" \
  --db-revision 8132aacfe61ecec7198a62e93cb8512dac89b1d6b9922fe3c7bb94b13f317d8f \
  --output "$RUN/prepared"
reference="$BASELINE/bench/bird/results/review-289-v1"
cmp "$RUN/prepared/manifest.json" "$reference/manifest.json"
"$PY" -m bench.bird.evaluate score --gold-only --source "$SOURCE" --db-dir "$DB_DIR" \
  --output "$RUN/prepared"

lane() (
  set -e
  profile=$1
  if test "$profile" != openai-gpt6-luna; then
    export PYTHONPATH="$PYTHONPATH:$SDK_351"
  fi
  for condition in A B D; do
    output="$RUN/$condition-$profile"
    env -u OPENAI_API_KEY "$PY" -m bench.bird.evaluate run --condition "$condition" \
      --profile "$profile" --prepared "$RUN/prepared" --db-dir "$DB_DIR" \
      --env-file "$CREDENTIALS" --product-commit 846e3d3b966adff759b83c60384f6e0619058fef --output "$output"
    "$PY" -m bench.bird.evaluate score --source "$SOURCE" --db-dir "$DB_DIR" --output "$output"
  done
)
lane openai-gpt41-mini &
mini_pid=$!
lane openai-gpt41 &
full_pid=$!
lane openai-gpt6-luna &
luna_pid=$!
failed=0
wait "$mini_pid" || failed=1
wait "$full_pid" || failed=1
wait "$luna_pid" || failed=1
test "$failed" -eq 0

PYTHONPATH="$PYTHONPATH:$METRICS" "$PY" -m bench.compare_bird_review \
  --reference "$reference" --run "$RUN" --db-dir "$DB_DIR" --output "$RUN/comparison"
```
