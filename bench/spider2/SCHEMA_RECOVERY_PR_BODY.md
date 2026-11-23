## 실행 조건

저장된 #275 기준군과 새 #278 변경군의 Spider 2.0-Lite SQLite 비교다. 기준군은 #277에 변경군으로 게시된 `9274ff1f7fca3834701e1927f8c412407216f0b2`이고, 변경군은 #279의 `fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7`이다. 변경군에서 135문항 × 2개 조합, 총 270건을 실행했다. [기준군 PR](https://github.com/CausalInferenceLab/Lang2SQL/pull/277) · [제품 PR #279](https://github.com/CausalInferenceLab/Lang2SQL/pull/279)

평가기 `bench.spider2.evaluate`의 원본 버전은 `24cff8cb1126fc017d1c68389b76213858e3a2e6`다. `experiment/278-spider2-schema-recovery`의 실행기 커밋은 `45725643a58c785a8491034d04e2b0ad8103a902`다. 실행 전 `src/`를 제품 SHA와 대조했다. 조합별 실행기 소스 해시와 런타임을 설정에 남겼다. 두 GPT‑4.1 프로필에서 OpenAI SDK 3.51.0을 사용했다.

Python 3.13.14, SQLite 3.53.1, LiteLLM 1.109.1, SQLAlchemy 2.0.50을 사용했다. 문항당 모델 API 호출 8회에 강제 조회와 보조 호출을 포함했다. 호출당 출력 4,096토큰, 동시성 조합당 4, API·문항·SQL 제한 120·480·30초와 최종 결과 상한 100,000행, 공급자 자동 재시도 0회를 적용했다.

같은 SQLite DB 30개, 공식 정답과 문항별 외부 문서를 사용했다. #260의 결과 CSV 생성 절차와 Spider2 공식 채점 함수로 판정했다. GPT‑4.1 mini를 실행·채점한 뒤 GPT‑4.1을 실행·채점했다.

데이터 리비전은 `cafb867313aab4e674652054198f383cf4018943`, 보관한 manifest SHA-256은 `ea169d82a5473ddbdbecebe8460841502480bca63f3efbfedc955ac558e4a67a`다. 주석·DB별 해시와 문서·공식 채점기·정답·채점 설정 해시를 manifest에 기록했다. [Manifest](results/schema-278-v1/manifest.json) · [의존성 잠금 파일과 패키지 버전](results/schema-278-v1/checks/)

변경군 첫 문항 시작부터 마지막 채점 파일 저장까지 UTC `2026-11-23T11:55:21.289988+00:00` ~ `2026-11-23T12:10:57.259398+00:00`다. 기준군 원본 설정·채점 파일 시각 범위는 UTC `2026-11-22T15:58:21.009238+00:00` ~ `2026-11-22T16:07:27.209137+00:00`다.

[Spider2 공식 채점기](https://github.com/xlang-ai/Spider2/blob/cafb867313aab4e674652054198f383cf4018943/spider2-lite/evaluation_suite/evaluate.py)

모의 테이블 조회로 첫 요청 auto, 최종 SQL·run_sql 보류, 강제 조회와 컬럼 응답 후 auto, 같은 응답의 조회·실행 요청을 확인했다. 모델별 강제 호출과 복귀 요청을 실호출했다. 실호출의 HTTP JSON에 `tool_choice`와 출력 상한을 확인했다. [모의·실호출 기록](results/schema-278-v1/checks/)

## 주요 지표

화살표는 기준군 → 변경군이다. 정확도의 분모는 계획 문항 수이며 미제출·실패도 포함한다. 변화폭은 반올림 전 변경군 정확도에서 기준군 정확도를 뺀 값이다. 전체 비교 쌍은 270/270건이다. 변경군 완료 270건, 채점 270건, API 오류 호출 0회, 재실행 0문항이다.

- openai-gpt41-mini 정답 수 21/135 → 25/135, 변화폭 +2.96%p
- openai-gpt41 정답 수 16/135 → 39/135, 변화폭 +17.04%p

| 프로필·조건 | #275 정답/전체 (정확도) | #278 정답/전체 (정확도) | 변화(%p) | #275만 정답 | #278만 정답 | 양쪽 정답 | 양쪽 오답 | 비교 쌍 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [openai-gpt41-mini](results/schema-278-v1/openai-gpt41-mini/paired-scores.jsonl) | 21/135 (15.56%) | 25/135 (18.52%) | +2.96 | 6 | 10 | 15 | 104 | 135 |
| [openai-gpt41](results/schema-278-v1/openai-gpt41/paired-scores.jsonl) | 16/135 (11.85%) | 39/135 (28.89%) | +17.04 | 7 | 30 | 9 | 89 | 135 |

### 실행 상태

| 프로필·조건 | 계획 | 시도 문항 | 총 시도 | 완료 | 채점 | API 오류 호출 | 중단 | 재실행 | 누락 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [openai-gpt41-mini](results/schema-278-v1/openai-gpt41-mini/paired-scores.jsonl) | 135 → 135 | 135 → 135 | 135 → 135 | 135 → 135 | 135 → 135 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |
| [openai-gpt41](results/schema-278-v1/openai-gpt41/paired-scores.jsonl) | 135 → 135 | 135 → 135 | 135 → 135 | 135 → 135 | 135 → 135 | 0 → 0 | 0 → 0 | 0 → 0 | 0 → 0 |

모의 응답과 별도 실호출은 본평가의 시도·호출·채점에서 제외했다. GPT‑4.1의 local022·local066·local130은 원본 실행기가 `provider_error`로 저장했다. 세 문항의 모델 호출 8회가 모두 성공했고 보조 호출이 예산에 포함됐다. 9번째 호출 전에 발생한 RuntimeError는 실행기의 예산 가드에 해당해 호출 예산 소진으로 재집계했다.

### 최종 SQL 제출과 실행

| 프로필·조건 | 최종 SQL 제출 | 미제출 | 형식 오류 | SQL 실행 성공 | 결과 불일치 | 컬럼 오류 | 기타 SQL 오류 | SQL 시간 초과 | 실행 실패 원인 미분류 | 채점기 오류 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [openai-gpt41-mini](results/schema-278-v1/openai-gpt41-mini/paired-scores.jsonl) | 129 → 129 | 6 → 6 | 0 → 1 | 81 → 116 | 60 → 91 | 43 → 9 | 3 → 3 | 미수집 → 1 | 2 → 0 | 0 → 0 |
| [openai-gpt41](results/schema-278-v1/openai-gpt41/paired-scores.jsonl) | 133 → 118 | 2 → 17 | 1 → 1 | 45 → 110 | 29 → 71 | 81 → 4 | 6 → 2 | 미수집 → 2 | 1 → 0 | 0 → 0 |

SQL 실행 성공은 공식 정답·결과 불일치 및 실행 후 채점기 오류를 포함한다. 형식 오류는 미제출의 부분집합이다. 제출 파서가 여러 SQL 블록 또는 SELECT/WITH가 아닌 블록을 거부했거나 답변에 SQL 블록 시작 표시 또는 줄 시작의 SELECT/WITH가 남아 있는데 파싱에 실패한 경우로 재집계했다. 최종 컬럼 오류는 실패한 제출 SQL을 같은 읽기 전용 DB에서 `EXPLAIN QUERY PLAN`으로 준비해 `no such column`을 확인했다. #277 원본에 SQLite 실행 오류 코드가 빠져 있다. 준비에 성공했으나 실행이 실패한 세 건의 원인은 미분류로 기록했다. 그 세 건의 시간 초과 여부는 미수집이다.

### 스키마 복구

| 프로필·조건 | run_sql 보류 | 최종 SQL 보류 | 같은 응답에서 컬럼 확보 | 강제 조회 요청 | 강제 요청 후 컬럼 확보 | 전체 응답 중 컬럼 확보 | 재작성 제출/문항 | 재작성 정답/제출 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [openai-gpt41-mini](results/schema-278-v1/openai-gpt41-mini/paired-scores.jsonl) | 108 | 25 | 0 | 133 | 133 | 171 | 128/135 | 25/128 |
| [openai-gpt41](results/schema-278-v1/openai-gpt41/paired-scores.jsonl) | 3 | 106 | 0 | 109 | 109 | 193 | 98/135 | 31/98 |

복구 표는 #278 관측값이다. 보류와 조회 수는 한 문항에서 여러 번 증가할 수 있다. 컬럼 확보는 `tables_with_columns`가 한 개 이상 반환된 모델 응답 단위로 집계했다. 재작성 SQL 제출은 앞서 SQL이 보류되고 실제 컬럼을 얻은 문항에서 최종 SQL을 제출한 경우다.

### 호출, 토큰, 문항 시간

| 프로필·조건 | 모델 호출 | 도구 요청 | 도구 실행 | run_sql 요청 | run_sql 실행 | 도구 DB 실행 | 최종 SQL DB 실행 | 입력 토큰 | 출력 토큰 | P50 (초) | P95 (초) | 8회 도달 | 예산 소진 미제출 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [openai-gpt41-mini](results/schema-278-v1/openai-gpt41-mini/paired-scores.jsonl) | 414 → 540 | 294 → 377 | 294 → 269 | 155 → 204 | 155 → 96 | 미수집 → 96 | 129 → 129 | 1,104,882 → 1,285,978 | 116,257 → 146,659 | 6.45 → 11.04 | 26.43 → 38.10 | 10 → 6 | 6 → 5 |
| [openai-gpt41](results/schema-278-v1/openai-gpt41/paired-scores.jsonl) | 194 → 577 | 59 → 309 | 59 → 306 | 22 → 84 | 22 → 81 | 미수집 → 81 | 133 → 118 | 367,232 → 1,255,401 | 66,184 → 134,675 | 2.84 → 8.57 | 9.42 → 23.10 | 1 → 18 | 1 → 16 |

재집계에는 원본 JSONL을 사용했다. #275 도구 요청은 assistant transcript, 실행은 도구 결과 또는 도구 실행 이벤트에서 계산했다. #278의 도구 요청은 API 응답, 실행은 도구 레지스트리 호출에서 계산했다. 도구 DB 실행은 `run_sql` 경로에서 SQLite 커서 실행에 도달한 횟수다. #275의 실제 도구 DB 실행과 스키마 복구 이벤트는 미수집이다. 최종 DB 실행은 각 평가기의 제출 SQL 평가 시도 수다. 스키마 검사, 기준 SQL과 진단용 EXPLAIN 실행은 그 수에서 제외한다. 토큰은 응답 사용량 합계다. P50·P95는 기록된 문항 경과 시간의 정렬값을 선형 보간했다. Spider2 경과 시간에는 최종 SQL 내보내기가 포함되고 BIRD 경과 시간은 생성 단계에서 측정했다.

## 문항별 상태 전환

아래 표는 기준군의 최종 `no such column` 문항을 분모로 집계했다. 모든 시작 상태의 전환 행렬은 비교 JSON의 `state_transitions`에 있다. [전체 전환 행렬](results/schema-278-v1/comparison.json)

| 프로필·조건 | #275 컬럼 오류 | 정답 | 결과 불일치 | 컬럼 오류 | 기타 SQL 오류 | SQL 시간 초과 | 형식 오류 | 호출 예산 소진 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [openai-gpt41-mini](results/schema-278-v1/openai-gpt41-mini/paired-scores.jsonl) | 43 | 5 | 29 | 6 | 1 | 0 | 0 | 2 |
| [openai-gpt41](results/schema-278-v1/openai-gpt41/paired-scores.jsonl) | 81 | 25 | 41 | 3 | 1 | 1 | 1 | 9 |

## 상세 기록과 재현 명령

| 프로필·조건 | 프로필 요청 ID | #275 응답 모델 ID | #278 응답 모델 ID | 모델 ID 미수집 호출 |
| --- | --- | --- | --- | --- |
| [openai-gpt41-mini](results/schema-278-v1/openai-gpt41-mini/paired-scores.jsonl) | `openai/gpt-4.1-mini-2025-04-14` | `gpt-4.1-mini-2025-04-14` 414회 | `gpt-4.1-mini-2025-04-14` 540회 | 0 → 0 |
| [openai-gpt41](results/schema-278-v1/openai-gpt41/paired-scores.jsonl) | `openai/gpt-4.1-2025-04-14` | `gpt-4.1-2025-04-14` 194회 | `gpt-4.1-2025-04-14` 577회 | 0 → 0 |

| 프로필·조건 | #275 문항별 기록 · 설정 · 채점 | #278 문항별 기록 · 설정 · 채점 |
| --- | --- | --- |
| [openai-gpt41-mini](results/schema-278-v1/openai-gpt41-mini/paired-scores.jsonl) | [원본](results/schema-278-v1/openai-gpt41-mini/baseline/records.jsonl) · [설정](results/schema-278-v1/openai-gpt41-mini/baseline/settings.json) · [채점](results/schema-278-v1/openai-gpt41-mini/baseline/scores.json) | [원본](results/schema-278-v1/openai-gpt41-mini/treatment/records.jsonl) · [설정](results/schema-278-v1/openai-gpt41-mini/treatment/settings.json) · [채점](results/schema-278-v1/openai-gpt41-mini/treatment/scores.json) |
| [openai-gpt41](results/schema-278-v1/openai-gpt41/paired-scores.jsonl) | [원본](results/schema-278-v1/openai-gpt41/baseline/records.jsonl) · [설정](results/schema-278-v1/openai-gpt41/baseline/settings.json) · [채점](results/schema-278-v1/openai-gpt41/baseline/scores.json) | [원본](results/schema-278-v1/openai-gpt41/treatment/records.jsonl) · [설정](results/schema-278-v1/openai-gpt41/treatment/settings.json) · [채점](results/schema-278-v1/openai-gpt41/treatment/scores.json) |

[전체 집계 JSON](results/schema-278-v1/comparison.json) · [평가 입력 manifest](results/schema-278-v1/manifest.json)

```bash
# 평가기 의존성은 원래 실행기의 잠금 파일에서 설치
git worktree add --detach "$DEPS" 24cff8cb1126fc017d1c68389b76213858e3a2e6
uv sync --project "$DEPS" --locked --extra benchmark --python 3.13.14
PY="$DEPS/.venv/bin/python"
export PYTHONPATH="$PWD/src:$PWD"
export LITELLM_LOCAL_MODEL_COST_MAP=True LITELLM_LOG=ERROR

git diff --exit-code fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7 -- src/
for PROFILE in openai-gpt41-mini openai-gpt41; do
  "$PY" -c 'import json,sys; print(json.dumps([p for p in json.load(open(sys.argv[1])) if p["name"] == sys.argv[2]]))' \
    bench/spider2/models.example.json "$PROFILE" > "$PROFILE_JSON"
  "$PY" -m bench.spider2.evaluate run --root "$SPIDER" --db-dir "$DB_DIR" \
    --manifest bench/spider2/manifest-sqlite135.json --profiles "$PROFILE_JSON" \
    --env-file "$CREDENTIALS" --output "$RUN/$PROFILE" --max-calls 8 \
    --concurrency 4 --call-timeout 120 --case-timeout 480 --sql-timeout 30 --max-rows 100000
  "$PY" -m bench.spider2.evaluate score --root "$SPIDER" --output "$RUN/$PROFILE"
done
```

`SPIDER`는 고정 리비전의 Spider2 체크아웃 경로다. `DB_DIR`에 30개 SQLite 파일을 둔다.
`DEPS`, `RUN`, `PROFILE_JSON`, `CREDENTIALS`는 각각 의존성 워크트리, 새 실행 루트,
프로필 설정 임시 파일, 기존 인증 설정 파일의 절대 경로다.
이어서 실행할 때 같은 `run`에 `--resume`을 추가한다.


`BASELINE`은 저장된 #275 원본이며 아래에서는 보고서에 동봉한 JSONL을 사용한다. `PUBLISHED`는 기존 PR의 문항별 점수 파일 사본이다. [게시 기준군 문항별 점수](https://github.com/jhpark324/Lang2SQL/blob/306f5ba0c30bdf30cce510e758c38e3d6d75722d/bench/spider2/results/schema-275-v1/paired-scores.jsonl)

```bash
BASELINE=bench/spider2/results/schema-278-v1
PUBLISHED="$BASELINE/published-baseline-scores.jsonl"
"$PY" bench/report_schema_recovery.py spider2 --baseline "$BASELINE" \
  --treatment "$RUN" --db-dir "$DB_DIR" --published "$PUBLISHED" \
  --output "$RUN/comparison"
```

[평가 보고서](SCHEMA_RECOVERY_RESULTS.md)
