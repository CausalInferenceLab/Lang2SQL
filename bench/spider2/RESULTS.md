# Spider 2.0-Lite SQLite baseline — 2026-11-14

현재 Lang2SQL master (`e0a7fb1`)를 고정하고 API 모델만 교체한 첫 기준 실험이다. 제품 소스 변경은 없으며, 일반 모델 대비 Lang2SQL의 개선 효과를 측정한 ablation은 아니다.

## 전체 135문항 결과

| 모델 프로필 | 정답 | 공식 실행 결과 정확도 | 최종 예측 생성/실행 실패 | API 호출 | 입력 / 출력 토큰 |
|---|---:|---:|---:|---:|---:|
| openai-gpt41-mini | 21/135 | 15.56% | 50 | 519 | 1,351,879 / 130,702 |
| openai-gpt41 | 17/135 | 12.59% | 89 | 269 | 454,301 / 65,848 |
| anthropic-sonnet45 | 미실행 | — | — | — | API 키 없음 |
| google-gemini25-flash | 미실행 | — | — | — | API 키 없음 |

OpenAI 2개 모델은 실제 API로 실행했다. Anthropic/Gemini는 실행기와 프로필을 준비했지만 로컬에 해당 키가 없어 미실행이다. 다른 모델로 대체하거나 미실행을 0점으로 표시하지 않았다. 금액은 공급자 청구 자료로 확인하지 않아 계산하지 않았다.

## 재현 조건

- Spider2 upstream `cafb867313aab4e674652054198f383cf4018943`의 SQLite `local*` 135문항, DB 30개. 공식 Lite 전체 547문항 점수와 직접 비교하면 안 된다.
- seed 20261114로 고정한 문항 순서. 문항, 문서, DB, 채점 코드, gold CSV/config 해시를 manifest에 저장했다. Gold는 채점 과정에서만 사용했다.
- 실행 환경의 `OKF_BUNDLE_DIR`는 설정되지 않았으며 별도 로컬 knowledge bundle을 주입하지 않았다.
- 모델별 snapshot 이름은 [settings.json](results/sqlite135-v1/settings.json)에 고정했다. 각 문항은 독립 세션/메모리로 시작한다.
- 모든 모델에 동일한 최종 SQL 지시문, 총 8 API 호출 제한(보조 추론 포함), 호출당 출력 4096 토큰, 호출 120초 / 문항 480초 / SQL 30초 제한, 최종 결과 최대 100,000행. 자동 재시도나 모델 fallback 없음.
- 전체 실행의 모델별 concurrency는 4이다. 모델 프로필은 순차 실행했다. latency는 토큰 길이, API 상태, 로컬 경합의 영향을 받으므로 순수 모델 속도로 해석하지 않는다.
- 각 문항 1회 생성(pass@1). temperature/seed는 공급자 기본값이며 결정론적 재현이나 통계적 우열을 주장하지 않는다.
- upstream의 공식 `evaluate_single_exec_result_instance`를 직접 호출한다. 오류/출력 누락을 포함해 135개 모두 분모에 넣는다. 예측 실패와 SQL은 실행됐지만 정답 결과와 다른 경우를 구분한다.
- 실행 시작 시 checkout HEAD는 제품 baseline인 `e0a7fb1`이었다. 평가 코드가 아직 커밋 전이었으므로 settings의 runner/provider SHA256으로 실제 실행 코드를 식별한다. 해당 해시는 이 PR의 코드와 일치함을 확인했다.

## 사전 점검과 증거

20문항 예비 실행은 GPT-4.1 mini 2/20, GPT-4.1 3/20이었다. 이를 보고 프롬프트나 제품 코드를 튜닝하지 않았다. 첫 예비 실행은 SQLite 연결 정리 시 스레드 오류로 중단했고, benchmark 연결의 NullPool/check_same_thread 설정을 고친 다음 새 디렉터리에서 재실행했다. 중단된 실행은 점수에 포함하지 않았다. 예비 실행은 순차 실행이며 전체 실행과 latency를 비교하지 않는다.

[원본 설정/manifest/summary](results/sqlite135-v1/)와 각 모델의 `scores.json`, `records.json`, 생성 SQL을 포함했다. `records.json`에는 각 호출의 사용량/응답 모델/소요 시간, 문항 상태와 결과 CSV 해시가 있다. 큰 transcript와 CSV 본문은 로컬 실행 디렉터리에 보존하고 Git에는 포함하지 않았다. SQL을 동일 DB에 재실행하고 공식 evaluator로 다시 채점할 수 있다. 외부 API 오류 원문은 키가 노출될 수 있어 클래스만 기록한다.

예비 실행 오류를 재확인한 예: mini/local004의 `customer_id`, mini/local008의 `playerID`, GPT-4.1/local009의 `a1.longitude`는 실제 DB에 없는 컬럼을 참조했다. 후속 실험에서 스키마 탐색/최종 SQL 검증을 개선할 근거이며, 이번 baseline 점수를 올리려고 수정하지 않았다.

## 검증

- Python 3.10 및 3.13 전체 테스트: 각각 170 passed.
- Black/Ruff(신규 benchmark 코드), git diff whitespace 검사 통과.
- 실제 SQLite 읽기 전용 실행, ATTACH/쓰기 차단, 행 초과 실패, SQL 시간 제한, 실패를 포함한 채점 분모, provider metadata 보존, 호출 예산, 병렬 상한/키 누락 회귀 테스트.
- 실제 upstream evaluator에 정답 CSV 하나와 실패 하나를 넣어 1/2로 채점되는 통합 점검 완료. 이 1/2는 모델 성능이 아니라 채점기 점검이다.
- 기존 제품 mypy는 4개 파일의 19개 오류로 실패한다. `src/` 차이가 없어 이 PR에서 제품 타입 오류를 수정하지 않았다.
- API 키/DB 원본/압축파일은 커밋하지 않는다. LiteLLM benchmark extra 때문에 공유 의존성 lock 일부가 갱신되며 전체 기존 테스트도 실행했다.

## 해석 범위와 다음 실험

이 결과는 공개 SQLite 하위집합에서의 현행 Lang2SQL+각 모델 기준점이다. Oracle SQL 방언, Snowflake/BigQuery, federation 격리, 장기 기억 성능을 입증하지 않는다. 각 API의 키를 설정하면 같은 manifest와 예산으로 추가 공급자를 실행할 수 있다. Lang2SQL의 기여를 주장하려면 이후에 모델 단독/현재 Lang2SQL/문맥 기능을 켠 Lang2SQL을 같은 모델과 문항으로 통제 비교해야 한다.
