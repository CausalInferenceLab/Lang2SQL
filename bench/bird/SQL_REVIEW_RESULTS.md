# BIRD Mini-Dev SQLite SQL 의미 검토 평가 (#289)

## 실행 조건

#280에 저장된 #279 하네스 실행 `fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7`과 #289 하네스 실행 `0a7d85d8155b7202d45999631f526e028ddc1e1e`을 문항 ID로 대조했다. 같은 BIRD Mini-Dev SQLite 365문항, 모델 3개, 입력 조건 3개를 사용했다. #289 완료·채점과 대응 쌍은 각각 3,285/3,285건이다. [#280](https://github.com/CausalInferenceLab/Lang2SQL/pull/280) · [#289](https://github.com/CausalInferenceLab/Lang2SQL/pull/289) · [실험 코드](https://github.com/jhpark324/Lang2SQL/tree/65d6d627a601eb4cb07060fdeed9a68e6a6d036e)

A는 evidence를 `term_custom` 팀 레이어로 등록해서 시스템 메시지에 주입했다. B는 같은 evidence를 사용자 질문에 첨부했고 D에는 질문과 공통 SQL 제출 지시문을 전달했다. 각 문항은 새 세션에서 실행했다. #289에서는 `run_sql`과 `explore_schema`에 제품의 `review_sql`이 추가된다. 검토는 생성과 같은 모델을 사용한다.

| 모델 | 요청 ID | 응답 ID (#280 · #289) | OpenAI SDK |
| --- | --- | --- | --- |
| GPT‑4.1 mini | `openai/gpt-4.1-mini-2025-04-14` | `gpt-4.1-mini-2025-04-14` | 3.51.0 |
| GPT‑4.1 | `openai/gpt-4.1-2025-04-14` | `gpt-4.1-2025-04-14` | 3.51.0 |
| GPT‑6 Luna | `openai/gpt-6-luna` | `gpt-6-luna` | 3.57.0 |

GPT‑6 Luna에 `reasoning_effort=none`을 사용했다. Luna의 공개 모델 ID는 #280에 기록된 별칭이다. GPT‑4.1과 GPT‑4.1 mini에는 날짜가 포함된 스냅샷 ID를 사용했다.

Python 3.13.14, SQLite 3.53.1, LiteLLM 1.109.1, SQLAlchemy 2.0.50을 사용했다. 문항당 모델 호출은 8회, 응답은 호출당 4,096토큰으로 제한했다. 강제 스키마 조회와 검토용 모델 호출을 모두 예산에 포함했다. 호출·문항·SQL 제한은 각각 120초·480초·30초이고, 채점의 행 제한은 100,000행이다. 공급자 자동 재시도는 0회, 조합당 동시성은 4다. 세 모델을 병렬로 실행했고 모델별로 A, B, D 순서를 따랐다. [실행 설정과 런타임](results/review-289-v1/checks/)

데이터 리비전은 `38818fcbc46038129592680c51b2c8b597aac27c`, manifest SHA-256은 `05d1414e7a355c486331506a8a9619428add7df6beae76789f8ce6fa647edb5b`이다. SQLite DB 11개와 주석의 해시를 대조했고 기준 SQL 365/365건이 실행됐다. EX는 제출 SQL과 기준 SQL의 행 튜플 집합이 같으면 1이다. 전체 계획 문항을 분모로 사용했다. [Manifest](results/review-289-v1/manifest.json) · [검증 기록](results/review-289-v1/checks/) · [BIRD EX 규칙](https://github.com/bird-bench/mini_dev/blob/1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py#L17)

문항 생성 시각 범위(UTC)는 #280이 `2026-11-23T11:55:20.303771+00:00`~`2026-11-23T12:55:26.686394+00:00`, #289가 `2026-11-24T17:46:17.931043+00:00`~`2026-11-24T19:19:00.959474+00:00`이다. 각 조건의 단일 실행 결과다. 실행 시점에 따른 모델 응답 및 API 지연 변동이 관측 차이에 포함될 수 있다.

## 실행 정확도

| 모델 | A·B·D 합산 정답/전체 | 변화 (%p) | 총 모델 호출 | 검토용 모델 호출 | 검토 통과·실행·제출 일치 |
| --- | --- | --- | --- | --- | --- |
| GPT‑4.1 mini | 195/1095 → 116/1095 | -7.21 | 5,086 → 7,602 | 2,302 | 108/613 |
| GPT‑4.1 | 223/1095 → 246/1095 | +2.10 | 4,369 → 6,561 | 1,380 | 515/927 |
| GPT‑6 Luna | 418/1095 → 331/1095 | -7.95 | 4,110 → 6,888 | 1,747 | 668/833 |

A·B·D 합산은 각 모델의 조건별 정답 수를 더한 값이다. 변화폭은 반올림 전 EX 차이다.

| 조건 · 모델 | #280 정답/전체 (EX) | #289 정답/전체 (EX) | 변화 (%p) | #280만 정답 | #289만 정답 | 양쪽 정답 | 양쪽 오답 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 69/365 (18.90%) | 43/365 (11.78%) | -7.12 | 44 | 18 | 25 | 278 |
| [A · GPT‑4.1](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) | 80/365 (21.92%) | 89/365 (24.38%) | +2.47 | 37 | 46 | 43 | 239 |
| [A · GPT‑6 Luna](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 149/365 (40.82%) | 116/365 (31.78%) | -9.04 | 63 | 30 | 86 | 186 |
| [B · GPT‑4.1 mini](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 74/365 (20.27%) | 44/365 (12.05%) | -8.22 | 47 | 17 | 27 | 274 |
| [B · GPT‑4.1](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) | 79/365 (21.64%) | 91/365 (24.93%) | +3.29 | 30 | 42 | 49 | 244 |
| [B · GPT‑6 Luna](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 146/365 (40.00%) | 120/365 (32.88%) | -7.12 | 60 | 34 | 86 | 185 |
| [D · GPT‑4.1 mini](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 52/365 (14.25%) | 29/365 (7.95%) | -6.30 | 31 | 8 | 21 | 305 |
| [D · GPT‑4.1](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) | 64/365 (17.53%) | 66/365 (18.08%) | +0.55 | 28 | 30 | 36 | 271 |
| [D · GPT‑6 Luna](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 123/365 (33.70%) | 95/365 (26.03%) | -7.67 | 49 | 21 | 74 | 221 |

## 실행 상태

표의 화살표는 #280 → #289다. #289 API 오류 호출 0회, 중단 0건, 재실행 0문항이다.

| 조건 · 모델 | 계획 | 완료 | 채점 | 총 시도 | API 오류 | 중단 | 재실행 | 누락 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [A · GPT‑4.1](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [A · GPT‑6 Luna](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B · GPT‑4.1 mini](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 1 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B · GPT‑4.1](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [B · GPT‑6 Luna](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1 mini](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑6 Luna](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 365 → 365 | 365 → 365 | 365 → 365 | 365 → 365 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |

## 최종 SQL 제출과 실행

| 조건 · 모델 | 제출 | 미제출 | 형식 오류 | 실행 성공 | 결과 불일치 | 컬럼 오류 | 기타 SQL 오류 | SQL 시간 초과 | 채점기 오류 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 337 → 214 | 28 → 151 | 1 → 3 | 330 → 206 | 261 → 163 | 4 → 5 | 3 → 2 | 0 → 1 | 0 → 0 |
| [A · GPT‑4.1](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) | 363 → 311 | 2 → 54 | 0 → 3 | 351 → 310 | 271 → 221 | 10 → 0 | 2 → 0 | 0 → 1 | 0 → 0 |
| [A · GPT‑6 Luna](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 346 → 278 | 19 → 87 | 0 → 0 | 346 → 277 | 197 → 161 | 0 → 0 | 0 → 0 | 0 → 1 | 0 → 0 |
| [B · GPT‑4.1 mini](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 331 → 196 | 34 → 169 | 0 → 10 | 323 → 191 | 249 → 147 | 4 → 4 | 2 → 1 | 2 → 0 | 0 → 0 |
| [B · GPT‑4.1](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) | 363 → 308 | 2 → 57 | 0 → 5 | 349 → 307 | 270 → 216 | 13 → 0 | 1 → 0 | 0 → 1 | 0 → 0 |
| [B · GPT‑6 Luna](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 351 → 278 | 14 → 87 | 0 → 0 | 351 → 278 | 205 → 158 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1 mini](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 327 → 203 | 38 → 162 | 0 → 12 | 322 → 200 | 270 → 171 | 4 → 3 | 1 → 0 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) | 363 → 308 | 2 → 57 | 0 → 1 | 346 → 302 | 282 → 236 | 11 → 3 | 5 → 2 | 1 → 1 | 0 → 0 |
| [D · GPT‑6 Luna](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 339 → 277 | 26 → 88 | 0 → 0 | 339 → 277 | 216 → 182 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |

실행 성공은 정답, 결과 불일치 및 실행 후 채점 오류를 포함한다. 형식 오류는 SQL 제출 실패의 부분집합이다. 실패 SQL의 컬럼 오류는 같은 읽기 전용 DB에서 `EXPLAIN QUERY PLAN`으로 `no such column`을 확인했다.

## 스키마 복구

| 조건 · 모델 | run_sql 보류 | 최종 SQL 보류 | 같은 응답 컬럼 확보 | 강제 조회 | 강제 조회 후 컬럼 확보 | 전체 응답 중 컬럼 확보 | 재작성 제출 | 재작성 정답 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 360 → 96 | 0 → 2 | 0 → 0 | 360 → 98 | 360 → 98 | 429 → 461 | 333 → 53 | 69 → 11 |
| [A · GPT‑4.1](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) | 3 → 3 | 358 → 62 | 0 → 0 | 361 → 65 | 361 → 65 | 426 → 475 | 359 → 52 | 78 → 7 |
| [A · GPT‑6 Luna](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 400 → 464 | 0 → 0 | 0 → 0 |
| [B · GPT‑4.1 mini](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 345 → 39 | 0 → 1 | 0 → 0 | 345 → 40 | 345 → 40 | 447 → 424 | 312 → 19 | 67 → 3 |
| [B · GPT‑4.1](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) | 3 → 1 | 359 → 134 | 0 → 0 | 362 → 135 | 362 → 135 | 427 → 474 | 360 → 118 | 78 → 25 |
| [B · GPT‑6 Luna](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 400 → 480 | 0 → 0 | 0 → 0 |
| [D · GPT‑4.1 mini](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 345 → 46 | 0 → 0 | 0 → 0 | 345 → 46 | 345 → 46 | 436 → 435 | 307 → 27 | 48 → 3 |
| [D · GPT‑4.1](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) | 0 → 0 | 362 → 115 | 0 → 0 | 362 → 115 | 362 → 115 | 417 → 473 | 360 → 94 | 64 → 17 |
| [D · GPT‑6 Luna](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 | 433 → 505 | 0 → 0 | 0 → 0 |

보류와 조회는 호출 단위, 컬럼 확보는 `tables_with_columns`에 컬럼을 받은 응답 단위, 재작성 제출은 SQL이 보류된 후 컬럼을 얻고 최종 제출까지 한 문항 단위다.

## 모델 호출, 토큰, 문항 시간

| 조건 · 모델 | 모델 호출 | 도구 요청 | 도구 실행 | run_sql 요청 | run_sql 실행 | 도구 DB 실행 | 최종 SQL DB 실행 | 입력 토큰 | 출력 토큰 | 토큰 미기록 호출 | P50 (초) | P95 (초) | 8회 도달 | 예산 소진 미제출 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 1,688 → 2,557 | 1,287 → 1,558 | 927 → 1,462 | 857 → 283 | 497 → 187 | 497 → 184 | 337 → 214 | 2,432,881 → 3,788,711 | 263,154 → 511,710 | 0 → 0 | 9.13 → 18.41 | 27.44 → 38.56 | 41 → 186 | 27 → 141 |
| [A · GPT‑4.1](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) | 1,465 → 2,174 | 744 → 1,337 | 741 → 1,334 | 317 → 364 | 314 → 361 | 312 → 360 | 363 → 311 | 1,803,890 → 3,371,051 | 206,616 → 336,402 | 0 → 0 | 5.98 → 9.57 | 12.12 → 19.87 | 4 → 69 | 2 → 41 |
| [A · GPT‑6 Luna](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 1,348 → 2,266 | 1,003 → 1,412 | 1,003 → 1,412 | 601 → 336 | 601 → 336 | 600 → 336 | 346 → 278 | 2,056,516 → 3,607,720 | 164,826 → 293,388 | 0 → 0 | 5.70 → 11.63 | 16.04 → 21.82 | 26 → 126 | 19 → 87 |
| [B · GPT‑4.1 mini](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 1,695 → 2,528 | 1,271 → 1,525 | 926 → 1,486 | 821 → 247 | 476 → 208 | 474 → 208 | 331 → 196 | 2,372,773 → 3,638,645 | 277,019 → 532,372 | 1 → 0 | 9.51 → 18.67 | 29.86 → 39.46 | 41 → 186 | 33 → 153 |
| [B · GPT‑4.1](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) | 1,473 → 2,189 | 743 → 1,281 | 740 → 1,280 | 316 → 346 | 313 → 345 | 312 → 341 | 363 → 308 | 1,745,079 → 3,260,485 | 208,471 → 333,208 | 0 → 0 | 5.91 → 9.42 | 12.83 → 20.29 | 4 → 63 | 2 → 43 |
| [B · GPT‑6 Luna](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 1,339 → 2,285 | 991 → 1,423 | 991 → 1,423 | 588 → 324 | 588 → 324 | 586 → 324 | 351 → 278 | 1,902,144 → 3,533,067 | 160,798 → 297,264 | 0 → 0 | 5.75 → 12.46 | 14.56 → 22.89 | 14 → 125 | 14 → 86 |
| [D · GPT‑4.1 mini](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 1,703 → 2,517 | 1,274 → 1,529 | 929 → 1,483 | 835 → 278 | 490 → 232 | 485 → 232 | 327 → 203 | 2,306,112 → 3,542,975 | 264,842 → 515,233 | 0 → 0 | 9.71 → 17.41 | 24.96 → 36.49 | 43 → 179 | 38 → 139 |
| [D · GPT‑4.1](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) | 1,431 → 2,198 | 704 → 1,299 | 704 → 1,299 | 286 → 342 | 286 → 342 | 286 → 339 | 363 → 308 | 1,602,505 → 3,228,044 | 195,668 → 340,036 | 0 → 0 | 5.76 → 9.83 | 11.07 → 19.36 | 6 → 69 | 2 → 45 |
| [D · GPT‑6 Luna](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 1,423 → 2,337 | 1,084 → 1,472 | 1,084 → 1,472 | 650 → 344 | 650 → 344 | 648 → 343 | 339 → 277 | 2,051,338 → 3,554,078 | 167,178 → 294,878 | 0 → 0 | 6.13 → 12.65 | 19.30 → 21.82 | 31 → 141 | 26 → 87 |

문항 시간은 하네스 생성 경과 시간이며, P50과 P95는 정렬된 문항 시간을 선형 보간했다. 도구 DB 실행은 `run_sql` 중 SQLite 커서 실행에 도달한 횟수다. 최종 SQL DB 실행은 채점 단계에서 제출한 SQL의 실행 시도 수다. 토큰은 API 응답이 보고한 사용량의 합이다.

## SQL 의미 검토

| 조건 · 모델 | 검토 요청 | 검토 실행 | 검토 API | pass | revise | 판정 오류 | 예산으로 호출 차단 | 검토 요청 없음 (문항) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 813 | 813 | 761 | 65 | 31 | 665 | 51 | 27 |
| [A · GPT‑4.1](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) | 476 | 476 | 468 | 356 | 111 | 1 | 8 | 2 |
| [A · GPT‑6 Luna](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 612 | 612 | 577 | 343 | 232 | 2 | 35 | 0 |
| [B · GPT‑4.1 mini](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 853 | 853 | 790 | 59 | 29 | 702 | 61 | 27 |
| [B · GPT‑4.1](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) | 457 | 457 | 446 | 340 | 106 | 0 | 10 | 2 |
| [B · GPT‑6 Luna](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 619 | 619 | 583 | 331 | 252 | 0 | 36 | 0 |
| [D · GPT‑4.1 mini](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 814 | 814 | 751 | 68 | 23 | 660 | 60 | 27 |
| [D · GPT‑4.1](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) | 477 | 477 | 466 | 329 | 136 | 1 | 11 | 1 |
| [D · GPT‑6 Luna](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 623 | 623 | 587 | 340 | 247 | 0 | 35 | 0 |

GPT‑4.1 mini의 검토 호출 2,302회 중 SQL 코드 블록을 포함한 응답은 2,027회였고, 도구가 유효한 판정을 얻지 못해 반환한 오류는 2,027회였다. 검토 입력의 질문에는 원래 BIRD SQL 제출 지시문도 포함된다.

| 조건 · 모델 | 제출 | 검토 요청 없이 제출 | 동일 SQL에 유효 판정 | 동일 SQL에 pass | 동일 SQL 실행 성공 | 별도 응답에서 pass 후 실행 성공 |
| --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 214 | 24 | 39 | 39 | 100 | 32 |
| [A · GPT‑4.1](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) | 311 | 2 | 161 | 160 | 164 | 160 |
| [A · GPT‑6 Luna](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 278 | 0 | 232 | 231 | 231 | 230 |
| [B · GPT‑4.1 mini](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 196 | 26 | 39 | 39 | 106 | 36 |
| [B · GPT‑4.1](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) | 308 | 1 | 182 | 181 | 183 | 180 |
| [B · GPT‑6 Luna](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 278 | 0 | 225 | 223 | 228 | 223 |
| [D · GPT‑4.1 mini](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 203 | 22 | 46 | 44 | 110 | 40 |
| [D · GPT‑4.1](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) | 308 | 1 | 182 | 177 | 179 | 175 |
| [D · GPT‑6 Luna](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 277 | 0 | 216 | 215 | 215 | 215 |

후보 일치는 양끝 공백을 제거한 SQL 문자열로 판정했다. 마지막 열은 동일 SQL이 마지막 검토에서 `pass`를 받은 뒤 별도 모델 응답에서 `run_sql`로 실행돼 성공했고, 최종 제출 SQL도 일치한 문항이다. 실제 DB 실행은 SQLite 이벤트에서 확인했다. 검토 흐름 관측값은 EX 채점에 영향을 주지 않는다.

## 문항별 상태 전환

아래 표는 #280에서 최종 제출 SQL에 컬럼 오류가 있었던 문항의 #289 결과다. 전체 상태 전환과 조합별 집계를 기록했다. [전체 집계 JSON](results/review-289-v1/comparison.json)

| 조건 · 모델 | #280 컬럼 오류 | 정답 | 결과 불일치 | 컬럼 오류 | 기타 SQL 오류 | SQL 시간 초과 | 형식 오류 | 호출 예산 소진 | 기타 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [A · GPT‑4.1 mini](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) | 4 | 1 | 1 | 0 | 0 | 0 | 0 | 2 | 0 |
| [A · GPT‑4.1](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) | 10 | 3 | 2 | 0 | 0 | 0 | 0 | 4 | 1 |
| [A · GPT‑6 Luna](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| [B · GPT‑4.1 mini](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) | 4 | 1 | 1 | 0 | 0 | 0 | 0 | 2 | 0 |
| [B · GPT‑4.1](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) | 13 | 3 | 4 | 0 | 0 | 0 | 1 | 4 | 1 |
| [B · GPT‑6 Luna](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| [D · GPT‑4.1 mini](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) | 4 | 0 | 2 | 0 | 0 | 0 | 0 | 1 | 1 |
| [D · GPT‑4.1](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) | 11 | 3 | 5 | 0 | 0 | 0 | 0 | 3 | 0 |
| [D · GPT‑6 Luna](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## 기록과 재현

| 조건 · 모델 | #280 원본 | #289 산출물 |
| --- | --- | --- |
| A · GPT‑4.1 mini | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/A-openai-gpt41-mini/treatment) | [설정](results/review-289-v1/A-openai-gpt41-mini/settings.json) · [채점](results/review-289-v1/A-openai-gpt41-mini/scores.json) · [실행 기록 (gzip)](results/review-289-v1/A-openai-gpt41-mini/records.jsonl.gz) · [대응 점수](results/review-289-v1/A-openai-gpt41-mini/paired-scores.jsonl) |
| A · GPT‑4.1 | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/A-openai-gpt41/treatment) | [설정](results/review-289-v1/A-openai-gpt41/settings.json) · [채점](results/review-289-v1/A-openai-gpt41/scores.json) · [실행 기록 (gzip)](results/review-289-v1/A-openai-gpt41/records.jsonl.gz) · [대응 점수](results/review-289-v1/A-openai-gpt41/paired-scores.jsonl) |
| A · GPT‑6 Luna | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/A-openai-gpt6-luna/treatment) | [설정](results/review-289-v1/A-openai-gpt6-luna/settings.json) · [채점](results/review-289-v1/A-openai-gpt6-luna/scores.json) · [실행 기록 (gzip)](results/review-289-v1/A-openai-gpt6-luna/records.jsonl.gz) · [대응 점수](results/review-289-v1/A-openai-gpt6-luna/paired-scores.jsonl) |
| B · GPT‑4.1 mini | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/B-openai-gpt41-mini/treatment) | [설정](results/review-289-v1/B-openai-gpt41-mini/settings.json) · [채점](results/review-289-v1/B-openai-gpt41-mini/scores.json) · [실행 기록 (gzip)](results/review-289-v1/B-openai-gpt41-mini/records.jsonl.gz) · [대응 점수](results/review-289-v1/B-openai-gpt41-mini/paired-scores.jsonl) |
| B · GPT‑4.1 | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/B-openai-gpt41/treatment) | [설정](results/review-289-v1/B-openai-gpt41/settings.json) · [채점](results/review-289-v1/B-openai-gpt41/scores.json) · [실행 기록 (gzip)](results/review-289-v1/B-openai-gpt41/records.jsonl.gz) · [대응 점수](results/review-289-v1/B-openai-gpt41/paired-scores.jsonl) |
| B · GPT‑6 Luna | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/B-openai-gpt6-luna/treatment) | [설정](results/review-289-v1/B-openai-gpt6-luna/settings.json) · [채점](results/review-289-v1/B-openai-gpt6-luna/scores.json) · [실행 기록 (gzip)](results/review-289-v1/B-openai-gpt6-luna/records.jsonl.gz) · [대응 점수](results/review-289-v1/B-openai-gpt6-luna/paired-scores.jsonl) |
| D · GPT‑4.1 mini | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/D-openai-gpt41-mini/treatment) | [설정](results/review-289-v1/D-openai-gpt41-mini/settings.json) · [채점](results/review-289-v1/D-openai-gpt41-mini/scores.json) · [실행 기록 (gzip)](results/review-289-v1/D-openai-gpt41-mini/records.jsonl.gz) · [대응 점수](results/review-289-v1/D-openai-gpt41-mini/paired-scores.jsonl) |
| D · GPT‑4.1 | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/D-openai-gpt41/treatment) | [설정](results/review-289-v1/D-openai-gpt41/settings.json) · [채점](results/review-289-v1/D-openai-gpt41/scores.json) · [실행 기록 (gzip)](results/review-289-v1/D-openai-gpt41/records.jsonl.gz) · [대응 점수](results/review-289-v1/D-openai-gpt41/paired-scores.jsonl) |
| D · GPT‑6 Luna | [설정·점수·원본 기록](https://github.com/jhpark324/Lang2SQL/tree/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/bird/results/schema-278-v1/D-openai-gpt6-luna/treatment) | [설정](results/review-289-v1/D-openai-gpt6-luna/settings.json) · [채점](results/review-289-v1/D-openai-gpt6-luna/scores.json) · [실행 기록 (gzip)](results/review-289-v1/D-openai-gpt6-luna/records.jsonl.gz) · [대응 점수](results/review-289-v1/D-openai-gpt6-luna/paired-scores.jsonl) |

실행기 커밋은 `41c9450cd9ffb67dffbdbffe500b1954c2746b1d`이다. `src/`는 #289 제품 SHA와 일치했다. #280의 평가기에서 제품 SHA 및 도구 등록을 연결해 사용했다. 채점기와 SQLite 연결 코드의 해시, 모델 ID, 문항 순서, 입력 배치, 평가 예산을 조합별로 대조했다. [실험 코드](https://github.com/jhpark324/Lang2SQL/tree/65d6d627a601eb4cb07060fdeed9a68e6a6d036e) · [#280 집계 코드](https://github.com/jhpark324/Lang2SQL/blob/f47da5c669a2c691d59d90fd236e227f5db0da96/bench/report_schema_recovery.py)

`SOURCE`와 `DB_DIR`에는 manifest의 주석 파일과 DB 스냅샷을 지정한다. `CREDENTIALS`는 로컬 API 설정 파일이다. `DEPS`, `REFERENCE`, `RUN`, `SDK_351`에는 각 작업 경로를 지정한다. 아래 명령은 실험 코드 워크트리에서 실행한다.

```bash
git fetch origin refs/pull/276/head refs/pull/280/head
git worktree add --detach "$DEPS" 8c1cb0b6b1ba68f179ed27f27779943c41a7b1d4
git worktree add --detach "$REFERENCE" f47da5c669a2c691d59d90fd236e227f5db0da96
uv sync --project "$DEPS" --locked --extra benchmark --python 3.13.14
PY="$DEPS/.venv/bin/python"
uv pip install --python "$PY" --target "$SDK_351" --no-deps openai==3.51.0
export PYTHONPATH="$PWD/src:$PWD"
export LITELLM_LOCAL_MODEL_COST_MAP=True LITELLM_LOG=ERROR

"$PY" -m bench.bird.evaluate prepare --source "$SOURCE" --db-dir "$DB_DIR" \
  --db-revision 8132aacfe61ecec7198a62e93cb8512dac89b1d6b9922fe3c7bb94b13f317d8f \
  --output "$RUN/prepared"
cmp "$RUN/prepared/manifest.json" "$REFERENCE/bench/bird/results/schema-278-v1/manifest.json"
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
      --env-file "$CREDENTIALS" --product-commit 0a7d85d8155b7202d45999631f526e028ddc1e1e --output "$output"
    "$PY" -m bench.bird.evaluate score --source "$SOURCE" --db-dir "$DB_DIR" --output "$output"
  done
)
lane openai-gpt41-mini &
mini_pid=$!
lane openai-gpt41 &
full_pid=$!
lane openai-gpt6-luna &
luna_pid=$!
wait "$mini_pid"
wait "$full_pid"
wait "$luna_pid"

PYTHONPATH="$PYTHONPATH:$REFERENCE" "$PY" -m bench.compare_bird_review \
  --reference "$REFERENCE/bench/bird/results/schema-278-v1" \
  --run "$RUN" --db-dir "$DB_DIR" --output "$RUN/comparison"
```
