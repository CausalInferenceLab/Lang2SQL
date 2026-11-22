# BIRD SQLite 365 스키마 배치 조회 평가

PR 273의 게시 결과(`e0a7fb1`)와 PR 275의 새 실행(`9274ff1`)을 비교했다.
세 모델에서 evidence 조건 A, B, D를 각각 실행했다. 조건별 365문항과 11개 SQLite DB, 채점 규칙을 공유한다.
PR 275 제품에 PR 273 평가 실행 코드(`1f47f28`)를 가져와 3,285건을 실행했다.
두 제품 버전에 이르는 변경 전체와 각각의 모델 실행 변동이 비교에 반영된다.

| 모델 프로필 | 조건 | PR 273 EX (정답) | PR 275 EX (정답) | 차이(%p) | PR 273만 정답 | PR 275만 정답 | Holm 보정 p |
|---|---|---:|---:|---:|---:|---:|---:|
| openai-gpt41-mini | A | 16.44% (60) | 17.81% (65) | +1.37 | 26 | 31 | 1.000 |
| openai-gpt41-mini | B | 21.37% (78) | 20.82% (76) | -0.55 | 30 | 28 | 1.000 |
| openai-gpt41-mini | D | 16.16% (59) | 17.26% (63) | +1.10 | 22 | 26 | 1.000 |
| openai-gpt41 | A | 3.01% (11) | 3.56% (13) | +0.55 | 4 | 6 | 1.000 |
| openai-gpt41 | B | 2.47% (9) | 4.11% (15) | +1.64 | 2 | 8 | 0.875 |
| openai-gpt41 | D | 3.01% (11) | 3.56% (13) | +0.55 | 6 | 8 | 1.000 |
| openai-gpt6-luna | A | 40.27% (147) | 40.00% (146) | -0.27 | 42 | 41 | 1.000 |
| openai-gpt6-luna | B | 38.36% (140) | 43.84% (160) | +5.48 | 27 | 47 | 0.239 |
| openai-gpt6-luna | D | 35.07% (128) | 34.79% (127) | -0.27 | 42 | 41 | 1.000 |

**Luna D는 365문항 모두 실행·채점을 완료했다.** 정답은 127문항(34.79%)이며, 최종 미실행 문항은 0건이다.
세션 종료로 응답이 저장되지 않았던 29번, 69번, 146번, 287번은 같은 설정으로 다시 실행해 일반 문항과 같은 규칙으로 채점했다.

정확도 차이는 동일 문항을 짝지어 계산했다. p값은 양측 정확 McNemar 검정 후 아홉 비교에 Holm 보정을 적용한 값이다.

## 도구 사용과 호출 예산

| 모델 프로필 | 조건 | 다중 테이블 요청 문항 | 오류 없이 완료된 다중 테이블 조회 문항 | 스키마 조회 문항 | 전체 도구 호출 | 모델 호출 | 8회 상한 소진 |
|---|---|---:|---:|---:|---:|---:|---:|
| openai-gpt41-mini | A | 195/365 | 195/365 | 313 → 329 | 1,661 → 1,342 | 1,950 → 1,614 | 52 → 27 |
| openai-gpt41-mini | B | 238/365 | 238/365 | 326 → 337 | 1,712 → 1,303 | 1,988 → 1,580 | 54 → 32 |
| openai-gpt41-mini | D | 237/365 | 236/365 | 321 → 329 | 1,713 → 1,162 | 1,993 → 1,449 | 50 → 17 |
| openai-gpt41 | A | 2/365 | 2/365 | 0 → 2 | 0 → 10 | 365 → 375 | 0 → 0 |
| openai-gpt41 | B | 0/365 | 0/365 | 0 → 0 | 0 → 0 | 365 → 365 | 0 → 0 |
| openai-gpt41 | D | 0/365 | 0/365 | 0 → 0 | 0 → 0 | 365 → 365 | 0 → 0 |
| openai-gpt6-luna | A | 353/365 | 351/365 | 365 → 363 | 1,818 → 999 | 1,897 → 1,345 | 30 → 16 |
| openai-gpt6-luna | B | 350/365 | 349/365 | 365 → 364 | 1,922 → 1,039 | 1,927 → 1,386 | 38 → 15 |
| openai-gpt6-luna | D | 349/365 | 349/365 | 365 → 365 | 2,004 → 1,116 | 2,068 → 1,453 | 54 → 25 |

다중 테이블 요청은 `explore_schema`의 `tables`에 서로 다른 이름을 두 개 이상 전달한 경우다.
완료 문항은 그런 요청 중 오류 플래그 없이 반환된 호출이 있는 문항이다.
스키마 조회 문항은 오류 없이 한 테이블 이상의 컬럼을 요청한 문항이다.
API 오류, 실패 사유, 토큰, 응답 모델 ID와 시간은 [상세 지표](results/schema-275-v1/comparison.json)에 기록했다. 같은 문항의 EX와 PR 275 실행의 실패 사유는 [문항별 비교 점수](results/schema-275-v1/paired-scores.jsonl)에 있다.

## 실행 조건

- OpenAI SDK는 GPT‑4.1 두 프로필에 3.51.0, Luna에 3.57.0을 사용했다. Luna의 reasoning effort는 none이었다.
- 이번 실행은 Luna 평가 커밋의 공통 잠금 파일을 사용했다. PR 273의 초기 GPT‑4.1 평가 커밋의 잠금 파일과 비교해 Pydantic 및 Pydantic Core의 패치 버전에 차이가 있다.
- 각 프로필에서 A, B, D 순서로 실행했고 세 프로필은 동시에 실행했다. 개별 조합의 동시 실행은 4건이다.
- 준비한 manifest는 PR 273 게시 manifest와 일치했고 정답 SQL 365건이 사전 점검을 통과했다.

## 재현

[PR 273의 입력 준비 절차](https://github.com/jhpark324/Lang2SQL/blob/1f47f28/bench/bird/README.md)에 따라
고정된 BIRD 주석과 SQLite 배포본을 확보한다. 데이터 리비전과 DB별 해시는 상세 지표의 `provenance`에 있다.
저장소 루트에서 `uv sync --locked --extra benchmark`를 실행한 뒤, `bench.bird.evaluate`의
`prepare`와 `score --gold-only`로 입력을 준비하고 기준 SQL을 점검한다.
생성 단계에서는 아래 명령으로 PR 275 제품 소스를 지정한다.

```bash
uv run --locked --extra benchmark python -m bench.bird.evaluate run \
  --condition "$BIRD_CONDITION" --profile "$BIRD_PROFILE" \
  --product-commit 9274ff1 --prepared "$PREPARED" --db-dir "$DB_DIR" --output "$RUN"
uv run --locked --extra benchmark python -m bench.bird.evaluate score \
  --source "$SOURCE" --db-dir "$DB_DIR" --output "$RUN"
```

조건은 `A`, `B`, `D`이고 모델 프로필은 `openai-gpt41-mini`, `openai-gpt41`, `openai-gpt6-luna`이다.
`PREPARED`와 `DB_DIR`은 공유하고 조합마다 서로 다른 `RUN` 디렉터리를 사용한다.
GPT‑4.1 계열을 기존 SDK와 맞출 때는 별도 디렉터리에 `openai==3.51.0`을 설치해 해당 실행의 `PYTHONPATH`에 둔다.
