# Semantic first-connect

이 브랜치는 `semantic-data-context`의 큰 Semantic Pack/Registry/UI를 Lang2SQL에
그대로 복사하지 않는다. 혼탄 시스템에서 유효했던 원칙을 Lang2SQL의 기존 도구
경계에 맞춘다.

> 모델은 SQL을 쓰지 않고 검토 가능한 typed value만 고른다. SQL은 코드가 만든다.

## 사용 흐름

1. 관리자가 Discord `/setup`으로 DB를 연결한다. credential-bearing DSN을 받는
   raw `/connect`는 Discord에 노출하지 않는다.
2. Lang2SQL이 table, column type, PK, FK 메타데이터를 읽고 연결 세대와 함께
   catalog를 원자적으로 활성화한다.
3. 선언된 FK는 child → parent 방향의 안전한 join 후보로 등록한다.
4. 개인정보·credential·서술문·식별자형 문자열 컬럼은 기본 차단한다.
5. 나머지 불확실한 문자열 차원은 값 샘플 없이 공개 검토 후보로 둔다.
6. 불투명한 분류 컬럼은 `/semantic_dimension_candidates`의 15분 mapping
   token으로 업무 분류 표현만 연결할 수 있다. 값 샘플이나 공개 승인은 없다.
7. 관리자는 `/semantic_candidates`의 15분 후보 토큰으로 `/semantic_release`를
   `confirm:false` → 동일 actor·token·tier의 `confirm:true` 두 단계 실행한다.
8. 불투명한 수치 컬럼은 `/semantic_metric_candidates`의 후보 토큰으로 업무
   표현만 같은 두 단계로 연결할 수 있다. 집계 의미는 아직 승인하지 않는다.
9. 사용자가 실제 질문을 하면 모델은 allowlisted ID, 질문 원문에 실제 존재하는
   표현, 집계, 분류, 미지원 의무를 조립한다.
10. 처음 보는 지표 집계와 분류 표현은 서로 독립된 `/semantic_review` 항목이 될 수
   있다. 한 질문에 검토가 두 번 필요할 수도 있다.
11. 원 요청자가 검토를 마치면 승인 당시의 immutable draft를 LLM 재해석 없이
    재개하고 `semantic_query`가 결정론적 SQL을 컴파일한다.
12. 컴파일 SQL은 기존 SafetyPipeline, metric contributor 보호, 차원 공개 제한을
    모두 통과한 뒤에만 실행·표시된다.

관리자가 다른 사용자의 review를 승인하면 연결만 저장된다. 관리자 채널에서 타
사용자의 DB 결과를 실행하거나 표시하지 않으며, 원 요청자가 다시 질문해야 한다.

## 기존 first-connect와 다른 점

| 이전 방식 | 이 브랜치 |
|---|---|
| 모든 table/column/card 사전 검토 | 물리 catalog 사실은 자동 등록 |
| 작은 DB도 수십 개 업무 결정 | 연결 시 업무 결정 0개; 질문 시 필요한 연결만 검토 |
| `/enrich`가 distinct 샘플을 LLM에 전송 | first-connect는 raw 값을 후보 생성에 사용하지 않음 |
| LLM이 `run_sql(sql=...)` 작성 | LLM은 typed ID·질문 표현·집계·미지원 의무만 조립 |
| 전체 pack 승인 후 사용 | 현재 질문 dependency만 확인 |
| 모델 SQL이 join을 자유롭게 선택 | declared FK의 유일한 child-to-parent path만 허용 |

## Discord 접근 및 검토 권한

- DB 연결과 semantic governance 명령은 guild 관리자만 실행한다.
- 자연어 DB 질의는 기본적으로 guild 관리자만 허용한다.
- 일반 구성원은 운영자가 `LANG2SQL_DISCORD_QUERY_CHANNEL_IDS`에 명시한 상위
  채널에서만 질문할 수 있다. thread는 상위 채널 정책을 따른다.
- DM은 사용자별 scope/session으로 격리해 허용한다.
- 잘못된 채널 허용 목록은 일부만 적용하지 않고 봇 시작을 실패시킨다.
- 이 allowlist와 결과 억제 규칙은 DB row/column RBAC를 대신하지 않는다.

현재 역할별 DB 정책은 범위 밖이다. 따라서 초기 실험은 read-only 전용 DB 계정,
최소 권한, 신뢰된 전용 채널을 사용해야 한다.

## 안전 경계

- semantic catalog가 하나라도 존재하면 `run_sql`은 모델 도구 목록에서 제거된다.
- catalog JSON이나 연결 binding이 손상돼도 raw SQL 경로로 되돌아가지 않는다.
- PII 의심 컬럼은 metric/dimension 후보에 포함하지 않는다.
- 문자열 차원의 관리자 공개 승인과 질문 표현 연결 승인을 분리한다.
- 공개 승인 전 후보 ID는 모델 도구 enum에도 포함하지 않는다.
- 모든 후보·행동 토큰은 15분, action, source ID, 연결 세대에 묶인다. 객체 후보
  토큰은 관련 object state/epoch를, catalog-wide public/reset 토큰은 전체 catalog
  revision을 검증한다. metric/dimension map과 dimension release는 추가로 경고를 실행한
  reviewer와 payload에 묶여 경고 없는 실행이나 payload 변경을 차단한다.
- review commit, pending 삭제, receipt, audit은 하나의 SQLite transaction이다.
  같은 reviewer·ID·choice 재시도는 idempotent이고 다른 actor/choice 재사용은 차단된다.
- parent → child fan-out, 경로 없음, 동률 경로는 SQL 없이 차단한다.
- child → parent join은 nullable FK와 orphan fact를 버리지 않도록 `LEFT JOIN`한다.
- 실행 전·실행 후·audit 후 catalog stamp를 확인하고, Discord 렌더 직전에도 같은
  stamp를 재검사한다. revoke/reset/재연결이 감지되면 준비된 행을 폐기한다.
- 요청한 group-by가 빠지거나 모델이 복사한 표현이 질문에 없으면 멈춘다.
- 미지원 filter·기간·단위·업무 조건을 버리고 실행하지 않는다.
- `ask_user`는 단독 tool call로만 허용되고, clarification을 반환하면 해당 turn을
  즉시 중단한다. Discord에는 SQL 형태를 제거한 한 번짜리 transient 상태만 저장한다.
- `/enrich`와 `/org_setup`의 sample-to-LLM 경로는 semantic 모드에서 비활성화된다.
- DSN·extras·catalog는 한 SQLite transaction으로 활성화한다.
- `/semantic_reset`도 15분 action token의 경고/확인 두 단계가 필요하다.

## 결과 공개 정책

두 정책은 별개다.

1. **질의 권한**: 누가 연결 DB에 질문할 수 있는가.
2. **결과 억제**: 허용된 질문의 집계나 범주를 표시해도 되는가.

비공개 기본값에서는 grouped/ungrouped 여부와 무관하게 `SUM`, `AVG`, source-row
`COUNT`의 실제 기여 행이 5개 미만이면 결과 전체를 차단한다. 빈 결과와
NULL-only metric도 차단한다. `MIN`/`MAX`는 contributor 수로 극값 노출을 숨길 수
없으므로 컴파일하지 않는다.

`controlled_grouped` 차원은 각 결과 그룹의 실제 metric contributor 최소 5를
유지한다. `public_grouped`는 먼저 `/semantic_public_data`로 현재 연결 전체가
공개·비개인 데이터임을 확인해야 하며 최소 그룹 보호를 해제한다. 다만 최대
50범주와 표시 라벨 128자 제한은 두 등급 모두 유지한다. 공개 데이터 범위에서도
하나라도 controlled 차원이 포함되면 contributor 보호와 `MIN`/`MAX` 차단이
다시 적용된다.

최소 contributor 5는 보수적인 출력 억제 규칙이지 k-anonymity, differential
privacy, row-level security 또는 사용자 권한 보장이 아니다.

## 현재 지원 범위

지원:

- 숫자 컬럼의 표현별 `SUM`/`AVG`
- 공개 데이터 범위이며 controlled 차원이 없는 경우의 `MIN`/`MAX`
- 모든 테이블의 명시적 physical source-record `COUNT(*)` (PK 불필요)
- categorical dimension group-by
- declared FK를 따라가는 유일한 child → parent 1~N hop join
- SQLite/SQLAlchemy read-only 실행과 결과 비교
- metadata-only 문자열 공개 후보 및 수치 지표 후보
- 모든 비차단 dimension의 metadata-only phrase mapping과 conflict 검증
- 질문 시점의 metric/dimension review와 immutable draft 재개

의도적으로 차단 또는 clarification:

- 검토되지 않은 기간/cohort 기준과 단위 변환
- 자유로운 filter expression과 row projection
- composite FK
- parent-to-child fan-out 또는 여러 최단 join path
- PII/credential/서술문/식별자형 문자열 컬럼
- 숫자형 calendar/code/identifier의 자동 역할 분류
- 역할별 row/column policy

이 제한은 조용한 fallback이 아니다. 각 경로는 typed blocker로 끝나며 raw SQL
생성으로 우회하지 않는다. 현재 실제 안전 실행 증거는 SQLite뿐이다. 다른 DB를
연결할 수 있다는 사실과 같은 compiler·timeout·취소 동작이 검증됐다는 주장은
구분한다.

## LLM에 남아 있는 역할

모델은 사용자 문장에서 shortlist 안의 metric/dimension 후보와 아직 표현하지
못한 의무를 구조화한다. 서버는 다음을 독립적으로 강제한다.

- ID가 질문별 bounded allowlist에 있는가
- 모델이 복사한 표현이 실제 질문에 있는가
- 표현·컬럼·집계 연결이 현재 source/revision에 대해 확인됐는가
- join이 metric grain을 늘리지 않는 유일한 child-to-parent path인가
- 기간·단위·group-by 의무가 누락되지 않았는가
- SQL과 결과가 safety/disclosure gate를 통과했는가

따라서 작은 모델이 후보를 잘못 고르면 임의 SQL 대신 검토·clarification·block으로
귀결될 가능성이 커진다. 다만 모델이 자연어의 숨은 필터를 아예 발견하지 못하는
문제까지 결정론적으로 해결했다고 주장하지 않는다. 자연어 planner 정확도는
별도 local-model 평가로 측정해야 한다.

## 검증 경계

```bash
env -u OPENAI_API_KEY -u LANG2SQL_LLM_BASE_URL \
  .venv/bin/python -m pytest -q
```

회귀 검증에는 테스트 중 생성하는 다중 테이블 SQLite fixture와 물류·발전·교육·
고객지원·조위 schema matrix를 사용한다. 별도 공개 평가는 Spider, BIRD Mini-Dev,
정부·과학 CSV에서 만든 21개 SQLite DB의 28개 case(dev 11 DB, holdout 10 DB)를
사용한다. 17개는 현재 typed query 범위이고 11개는 미지원 의무를 안전하게
차단해야 하는 case다. dataset 고유 mapping과 gold SQL은 `bench/**` 밖의 제품
코드나 모델 입력으로 나가지 않는다.

oracle-plan 실행은 compiler, join, disclosure, 실제 결과 동일성을 검증한다. gold
semantic plan을 사용하므로 자연어 planner 정확도를 증명하지 않는다. nullable FK를
보존하는 production `LEFT JOIN`이 frozen oracle의 `INNER JOIN`보다 orphan fact를
하나 더 보존하는 경우도 별도 coverage-policy difference로 보고하며 gold를 제품
동작에 맞게 고치지 않는다.
