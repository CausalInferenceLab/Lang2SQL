# BIRD SQLite 100문항 SQL 실행 후 답변 검증 평가

PR #314의 프롬프트 미적용군과 적용군에서 각각 600건을 생성·채점했습니다. 합산 EX는 151/600 (25.17%)에서 153/600 (25.50%)로 +0.33%p 변했습니다. 오답→정답은 50쌍, 정답→오답은 48쌍입니다.

## 실행 정확도

| 조건 · 모델 | 미적용 EX | 적용 EX | 변화 (%p) | 오답→정답 | 정답→오답 |
| --- | ---: | ---: | ---: | ---: | ---: |
| B · GPT‑4.1 mini | 19/100 (19.00%) | 20/100 (20.00%) | +1.00 | 8 | 7 |
| D · GPT‑4.1 mini | 14/100 (14.00%) | 15/100 (15.00%) | +1.00 | 6 | 5 |
| B · GPT‑4.1 | 26/100 (26.00%) | 23/100 (23.00%) | -3.00 | 6 | 9 |
| D · GPT‑4.1 | 19/100 (19.00%) | 19/100 (19.00%) | +0.00 | 11 | 11 |
| B · GPT‑6 Luna | 40/100 (40.00%) | 48/100 (48.00%) | +8.00 | 13 | 5 |
| D · GPT‑6 Luna | 33/100 (33.00%) | 28/100 (28.00%) | -5.00 | 6 | 11 |
| 합산 | 151/600 (25.17%) | 153/600 (25.50%) | +0.33 | 50 | 48 |

두 조건은 같은 질문과 SQL 제출 지시를 사용했고, B에서는 원본 evidence를 질문에 덧붙였습니다. 각 군의 B−D는 evidence 첨부 조건의 EX에서 미첨부 조건의 EX를 뺀 값입니다.

| 모델 | 미적용 B−D (%p) | 적용 B−D (%p) |
| --- | ---: | ---: |
| GPT‑4.1 mini | +5.00 | +5.00 |
| GPT‑4.1 | +7.00 | +4.00 |
| GPT‑6 Luna | +7.00 | +20.00 |

## 실행 지표

각 셀은 미적용군 → 적용군입니다. 문항 시간은 문항별 경과 시간의 합계입니다.

| 조건 · 모델 | 모델 호출 | 문항 시간 합계 (초) | SQL 미제출 | 채점 SQL 실행 실패 |
| --- | ---: | ---: | ---: | ---: |
| B · GPT‑4.1 mini | 453 → 418 | 927.8 → 944.5 | 9 → 9 | 6 → 4 |
| D · GPT‑4.1 mini | 440 → 415 | 870.4 → 1,096.7 | 9 → 4 | 3 → 3 |
| B · GPT‑4.1 | 432 → 427 | 676.8 → 657.5 | 9 → 10 | 3 → 4 |
| D · GPT‑4.1 | 440 → 434 | 680.0 → 659.3 | 9 → 6 | 4 → 3 |
| B · GPT‑6 Luna | 378 → 383 | 745.6 → 755.6 | 5 → 4 | 0 → 0 |
| D · GPT‑6 Luna | 378 → 420 | 817.4 → 847.8 | 5 → 6 | 0 → 0 |
| 합산 | 2,521 → 2,497 | 4,717.9 → 4,961.4 | 46 → 39 | 16 → 14 |

공급자가 보고한 입력 토큰은 5,385,561 → 5,575,494개, 출력 토큰은 372,776 → 369,900개입니다. 토큰 사용량이 비어 있는 호출은 0 → 1회, 공급자 오류는 0 → 1회, 취소된 호출은 0 → 0회였습니다. 적용군 GPT‑4.1 B의 문항 10은 HTTP 500 오류로 SQL 미제출에 포함했습니다. `run_sql`이 한 번 이상 성공한 문항은 504 → 484건입니다. [전체 지표](results/post-sql-answer-check-314-sqlite100-v1/comparison.json) · [문항별 EX](results/post-sql-answer-check-314-sqlite100-v1/paired-scores.jsonl)

## 실험 설정

미적용군은 `fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7`, 적용군은 `f4635ea567698f32d788688d327d2af33b59de9b`입니다. 두 군 모두 PR #316의 `d926f6011c15bd94d2e82d13cb84aaa25b6178b0`를 같은 벤치마크 워크트리에서 사용했습니다. [제품 PR #314](https://github.com/CausalInferenceLab/Lang2SQL/pull/314) · [벤치마크 PR #316](https://github.com/CausalInferenceLab/Lang2SQL/pull/316)

첫 모델 요청 600쌍에서 사용자 메시지와 도구 스키마·순서가 모두 일치했습니다. 적용군 시스템 메시지에서 PR #314가 추가한 검증 지시 문단을 제거하면 미적용군과 일치했습니다. 두 군 모두 `run_sql`, `explore_schema`, `enrich_schema`, `term_custom`, `org_setup`, `ask_user`, `remember`, `ingest_doc`, `confirm_ingest`까지 기본 도구 9개를 첫 모델 요청에 포함했습니다. 각 문항은 새 세션과 빈 인메모리 저장소에서 시작했고, 문서 도구는 인라인 본문을 받도록 구성했습니다. [요청 검증](results/post-sql-answer-check-314-sqlite100-v1/first-request-audit.json) · [스모크 검증](results/post-sql-answer-check-314-sqlite100-v1/smoke-first-request-audit.json)

고정 표본은 BIRD Mini-Dev의 SQLite 11개 DB에서 뽑은 100문항입니다. DB 지문과 기준 SQL 100개의 실행을 확인했습니다. EX는 SQLite에서 실행한 예측과 기준 SQL의 행 튜플 집합을 비교하며, 미제출과 실행 실패를 분모에 포함합니다. 최종 응답 형식은 SQL 블록 하나로 고정했습니다. [표본과 DB 지문](results/post-sql-answer-check-314-sqlite100-v1/manifest.json) · [기준 SQL 검사](results/post-sql-answer-check-314-sqlite100-v1/gold_check.json)

Python 3.13.14, SQLite 3.53.1, LiteLLM 1.109.1을 사용했습니다. OpenAI SDK는 GPT‑4.1 계열에서 3.51.0, GPT‑6 Luna에서 3.57.0을 사용했습니다. 도구 내부 호출을 포함해 문항당 모델 호출 8회, 호출당 출력 4,096토큰으로 제한했습니다. 호출·문항·SQL 제한은 각각 120초, 480초, 30초이며 채점에서는 최대 100,000행을 읽었습니다. 각 군의 모델 3종은 병렬로 실행했고, 각 모델은 B 다음 D를 조합당 동시성 4로 실행했습니다.

표의 합산은 같은 100문항을 모델·조건별로 반복한 600쌍의 기술 통계입니다. 조합당 한 번 실행했으며 미적용군을 먼저 실행했습니다. GPT‑6 Luna는 별칭으로 요청했고, 실제 응답 모델 ID와 공급자가 반환한 fingerprint는 조합별 요약에 기록했습니다. [실행 시각과 산출물 SHA-256](results/post-sql-answer-check-314-sqlite100-v1/provenance.json)

## 재현

PR #316의 고정 커밋과 위 제품 커밋 두 개를 체크아웃한 뒤 기존 사용법의 `prepare`, `check-gold`, `matrix.sh`, `compare`를 실행합니다. [벤치마크 사용법](https://github.com/jhpark324/Lang2SQL/blob/d926f6011c15bd94d2e82d13cb84aaa25b6178b0/bench/bird/README.md)

게시된 두 군의 채점 파일로 집계만 재현하려면 벤치마크 워크트리에서 `RESULTS`를 이 결과 디렉터리의 절대 경로로, `REPLAY_OUTPUT`을 새 출력 경로로 지정합니다.

```bash
"$BIRD_GPT41_PY" -m bench.bird.evaluate compare \
  --baseline "$RESULTS/baseline" --candidate "$RESULTS/candidate" \
  --output "$REPLAY_OUTPUT"
```
