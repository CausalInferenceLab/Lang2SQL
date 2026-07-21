# Semantic first-connect

이 브랜치는 기존 `semantic-data-context`의 큰 Semantic Pack/Registry/UI를
Lang2SQL에 그대로 복사하지 않는다. 혼탄 시스템에서 유효했던 원칙만
Lang2SQL의 기존 도구 경계에 맞춰 작은 수직 기능으로 옮긴다.

> 모델은 SQL을 쓰지 않고, 검토 가능한 값만 고른다. SQL은 코드가 만든다.

## 사용 흐름

1. 관리자가 Discord에서 `/setup`으로 DB를 연결한다. raw DSN `/connect`는 UI에 노출하지 않는다.
2. Lang2SQL이 DB catalog의 테이블, 컬럼 타입, PK, FK를 읽는다.
3. 선언된 FK는 child → parent 방향의 안전한 join 후보로 자동 등록한다.
4. 개인정보·credential·검토되지 않은 free-text 컬럼은 기본 차단한다.
5. 숫자 컬럼은 업무 지표 *후보*로만 등록하고, 연결 시 전부 묻지 않는다.
6. 사용자가 실제 질문을 하면 질문 속 표현과 선택된 DB 컬럼의 연결을 보여준다.
7. `/semantic_review`에서 SUM/AVG/MIN/MAX/COUNT, 연결 확인, 또는 거절을 고른다.
8. 원래 질문을 자동 재개하고 `semantic_query`가 결정론적 SQL을 컴파일한다.
9. 컴파일된 SQL도 기존 SafetyPipeline을 통과한 뒤에만 실행된다.

`/semantic_status`는 자동 등록된 구조와 현재 확인 대기 항목을 보여준다.

## 기존 first-connect와 다른 점

| 이전 방식 | 이 브랜치 |
|---|---|
| 모든 table/column/card를 사전 검토 | DB catalog 사실은 자동 등록 |
| 작은 DB도 수십 개 결정 | 연결 시 업무 결정 0개 |
| `/enrich`가 distinct 샘플을 LLM에 전송 | first-connect는 raw 값을 읽지 않음 |
| LLM이 `run_sql(sql=...)` 작성 | LLM은 ID·질문 원문 표현·집계·미지원 의무만 조립 |
| 전체 pack 승인 후 사용 | 현재 질문의 dependency만 확인 |
| join path가 불명확해도 모델 SQL에 의존 | declared FK의 유일한 many-to-one path만 허용 |

## 안전 경계

- semantic catalog가 하나라도 존재하면 `run_sql`은 모델 도구 목록에서 제거된다.
- catalog JSON이 손상돼도 raw SQL 경로로 되돌아가지 않는다.
- PII 의심 컬럼은 metric/dimension 후보에 포함하지 않는다.
- parent → child fan-out, 경로 없음, 동률 경로는 SQL 없이 차단한다.
- 질문이 요청한 group-by가 빠지면 clarification 상태로 멈춘다.
- 모델이 보낸 metric/dimension 표현이 실제 사용자 질문에 없으면 차단한다.
- 처음 보는 업무 표현은 `표현 → 물리 컬럼 → 집계` 연결을 보여준 뒤에만 저장한다.
- 같은 숫자 컬럼도 `amount=SUM`, `average amount=AVG`처럼 표현별 집계를 따로 저장한다.
- 모델이 필터·비교·업무 조건을 미지원 의무로 보고하면 조건을 버리지 않고 멈춘다.
- 기간 기준이나 단위 변환 규칙이 검토되지 않았으면 추측하지 않는다.
- `/enrich`와 `/org_setup`의 sample-to-LLM 경로는 semantic 모드에서 비활성화된다.
- Discord에는 `/setup`만 노출하고, 연결 정보는 암호화 저장 경로를 사용한다.
- Discord 서버의 DB 연결 변경은 관리자에게만 허용한다.
- DSN·extras·catalog는 한 SQLite transaction으로 함께 활성화한다.
- `/semantic_reset confirm:true`로 사람의 검토 이력만 관리자 초기화할 수 있다.

## 현재 지원 범위

첫 수직 기능은 다음을 지원한다.

- 단일 numeric metric의 표현별 SUM/AVG/MIN/MAX
- 선언 PK 기반 source-record count
- categorical dimension group-by
- declared FK를 따라가는 유일한 child → parent 1~N hop join
- SQLite/SQLAlchemy 실제 read-only 결과 검증
- 질문 시점의 지표 검토와 같은 질문 자동 재개

다음은 의도적으로 아직 `ready`가 되지 않는다.

- 검토되지 않은 기간/cohort 기준
- 검토되지 않은 단위 변환
- 자유로운 filter expression
- composite FK
- fan-out 또는 여러 최단 join 경로
- 역할별 row/column policy

이 제한은 조용한 fallback이 아니다. 각 경로는 clarification 또는 blocked로
반환된다. 다음 단계는 이 상태들을 하나씩 typed value로 확장하는 것이며,
raw SQL 생성으로 우회하지 않는다.

## LLM에 남아 있는 역할

이 브랜치는 LLM을 없애는 것이 아니라 역할을 좁힌다. 모델은 사용자 문장에서
metric/dimension 후보와 아직 표현하지 못한 조건을 구조화한다. 서버는 다음을
독립적으로 강제한다.

- ID가 현재 catalog allowlist에 있는가
- 모델이 복사한 표현이 실제 질문에 있는가
- 해당 표현·컬럼·집계 연결을 사람이 확인했는가
- join이 metric grain을 늘리지 않는 유일한 child → parent 경로인가
- 기간·단위·group-by 의무가 누락되지 않았는가
- compiler SQL이 기존 safety gate를 통과했는가

따라서 작은 모델이 후보를 잘못 고르면 임의 SQL이 실행되는 대신 컬럼 연결 확인
또는 차단으로 귀결된다. 다만 모델이 자연어의 숨어 있는 필터를 아예 발견하지
못하는 문제까지 결정론적으로 해결했다고 주장하지 않는다. 현재는 명시적 의무
슬롯과 보수적 기간·단위·group 검사를 함께 쓰며, 이 부분은 실제 소형 모델
cross-domain 검증으로 계속 측정해야 한다.

## 검증

```bash
env -u OPENAI_API_KEY -u LANG2SQL_LLM_BASE_URL \
  .venv/bin/python -m pytest -q
```

핵심 acceptance fixture는 개발 시 미리 보지 않은 형태의 `orders → customers
→ regions` 3테이블 SQLite DB를 테스트 중에 생성한다. 여기에 물류·발전·교육·
고객지원·조위 관측의 서로 다른 컬럼명을 가진 5개 first-connect schema matrix를
더해 특정 도메인 이름에 묶이지 않는 동일 경로를 검증한다. 연결 시 검토 0개,
첫 질문에서 표현·컬럼·집계 연결 1개 확인, 이후 동일 표현 무확인 실행을 검증한다.
같은 컬럼의 SUM과 AVG가 서로 덮어쓰지 않는 것, 잘못된 business phrase 매핑을
거절하면 SQL이 생성되지 않는 것, 표현하지 못한 filter obligation이 남으면
`ready`가 되지 않는 것도 회귀 테스트로 고정한다.
