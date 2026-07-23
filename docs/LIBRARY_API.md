# 모델이 SQL을 작성하지 않는 애플리케이션 통합 API

`Lang2SQLRuntime`은 Discord가 아닌 호스트가 내부 semantic service나 SQL 객체를
직접 다루지 않고 검토형 질의를 사용할 수 있게 하는 비동기 진입점이다.

| 단계 | 호스트가 보내는 것 | 성공 응답 |
|---|---|---|
| `connect` | DB 연결 정보와 `CallContext` | metadata scan 결과와 초기 검토 항목 |
| `candidates` | 원 질문 | bounded metric/group/filter/DATE 후보 |
| `feedback` | 사람이 고른 허용 선택지 | 저장된 검토 결정과 재개 가능한 다음 단계 |
| `plan` | 후보 ID로 조립한 `QueryDraft` | 일회용 `PreparedPlan` 또는 `ReviewRequired` |
| `execute` | 같은 사용자·대화·DB에 묶인 plan | typed columns와 rows |

`connect`는 물리명과 실제 DB comment를 후보로 자동 준비한다. 같은 source와 전체
물리 fingerprint로 재연결할 때만 기존 Enrich 설명 캐시를 다시 사용한다.
`LANG2SQL_AUTO_METADATA_ENRICH=auto` 또는 `llm`이면 metadata-only LLM 보강을
시도하며, `Connected.scan`의 `enrichment_status`, `enriched_object_count`,
`enrichment_reason`으로 성공·제한 사유를 확인할 수 있다. 어느 결과도 승인된
업무 의미, join, 집계 또는 공개 권한을 만들지 않는다.

모델은 SQL을 받거나 반환하지 않는다. 필터 값도 SQL 문자열, candidate DTO, repr,
audit parameter detail 또는 영속 review record에 들어가지 않는다. 같은 프로세스의
검토 자동 재개에 필요한 원 질문과 typed 값은 메모리에만 최대 15분 보관된다.

## 먼저 실행해 보기

다음 예제는 임시 SQLite DB를 직접 만들기 때문에 Discord, 기존 DB, LLM, 네트워크가
필요하지 않다. 이 예제가 직접 만든 공개 fixture에 한해 정해진 검토 선택을 적용한
뒤 공개 API의 전체 흐름을 실행한다.

```bash
uv run python examples/semantic_runtime_quickstart.py
```

예제에서 쓰는 테이블과 값은 실행할 때 임시 디렉터리에 생성되고 종료 시 제거된다.
실서비스에서는 예제의 자동 선택 함수를 그대로 쓰지 말고 사용자의 명시적 결정을
`FeedbackRequest`로 전달해야 한다.

## API 흐름 예제

아래는 각 호출의 연결 관계를 보여 주는 전체 예제다. 실제 서비스에서는
`choose_human_choice()` 자리에 steward UI를 연결한다. 실행 가능한 동일 코드는
[`examples/semantic_runtime_quickstart.py`](../examples/semantic_runtime_quickstart.py)에
있다.

<details>
<summary>Python 전체 예제 펼치기</summary>

```python
import asyncio

from lang2sql import (
    AggregateKind,
    CallContext,
    CandidateRequest,
    CandidateSet,
    Capability,
    ConnectRequest,
    Connected,
    ConnectionInput,
    ExecutionReady,
    ExecuteRequest,
    FeedbackApplied,
    FeedbackRequest,
    FilterInput,
    FilterOperation,
    Lang2SQLRuntime,
    LiteralInput,
    PlanReady,
    PlanRequest,
    QueryDraft,
    ReviewRequired,
    ValueKind,
)


def choose_human_choice(review) -> str:
    """호스트 UI가 실제 steward에게 선택지를 보여 주는 자리다."""
    print(f"review={review.kind} object={review.object_id}")
    print("allowed:", ", ".join(review.allowed_choices))
    choice = input("human choice: ").strip()
    if choice not in review.allowed_choices:
        raise ValueError("allowed_choices 중 하나를 정확히 선택해야 합니다.")
    return choice


async def run() -> None:
    runtime = Lang2SQLRuntime.local(path="lang2sql-runtime.db")
    context = CallContext(
        scope="acme-demo",
        actor_id="steward-1",
        conversation_id="discord-thread-42",
        capabilities=frozenset(
            {Capability.CONNECT, Capability.QUERY, Capability.REVIEW_ANY}
        ),
    )

    connected = await runtime.connect(
        ConnectRequest(
            context,
            ConnectionInput("sqlite:////absolute/path/orders.sqlite"),
        )
    )
    if not isinstance(connected, Connected):
        raise RuntimeError(connected.message)

    # 공개 데이터/차원 여부를 코드가 자동 승인하면 안 된다. 호스트 UI가
    # allowed_choices를 사람에게 보여 주고, 그 명시적 선택만 feedback으로 적용한다.
    for review in connected.reviews:
        applied = await runtime.feedback(
            FeedbackRequest(context, review.review_id, choose_human_choice(review))
        )
        if not isinstance(applied, FeedbackApplied):
            raise RuntimeError(applied.message)

    question = "total amount where status is paid"
    discovered = await runtime.candidates(CandidateRequest(context, question))
    if not isinstance(discovered, CandidateSet):
        raise RuntimeError(discovered.message)

    metric = next(item for item in discovered.metrics if item.grounded_phrase == "amount")
    status = next(
        item for item in discovered.filter_dimensions
        if item.grounded_phrase == "status"
    )
    assert ValueKind.STRING in status.allowed_value_kinds

    draft = QueryDraft(
        question=question,
        source=discovered.source,
        candidate_token=discovered.candidate_token,
        metric_id=metric.metric_id,
        metric_phrase=metric.grounded_phrase,
        aggregate=AggregateKind.SUM,
        filters=(
            FilterInput(
                dimension_id=status.dimension_id,
                dimension_phrase=status.grounded_phrase,
                operator=FilterOperation.EQ,
                operator_phrase="is",
                values=(LiteralInput(ValueKind.STRING, "paid", "paid"),),
            ),
        ),
    )

    result = await runtime.plan(PlanRequest(context, draft))
    while isinstance(result, ReviewRequired):
        applied = await runtime.feedback(
            FeedbackRequest(
                context,
                result.review.review_id,
                choose_human_choice(result.review),
            )
        )
        if not isinstance(applied, FeedbackApplied):
            raise RuntimeError(applied.message)
        if applied.next is None:
            raise RuntimeError(
                "검토 결정은 저장됐습니다. 프로세스가 재시작되었으므로 "
                "같은 질문을 candidates 단계부터 다시 제출해 주세요."
            )
        result = applied.next

    if not isinstance(result, PlanReady):
        raise RuntimeError(result.message)
    executed = await runtime.execute(ExecuteRequest(context, result.plan))
    if not isinstance(executed, ExecutionReady):
        raise RuntimeError(executed.message)
    print(executed.columns, executed.rows)
    runtime.close()


asyncio.run(run())
```

</details>

## Token과 재시작 경계

`CandidateSet.source`는 진단용 표시가 아니라 안전 경계다. 재연결 뒤 과거 source로
만든 `QueryDraft`는 `candidate_source_stale`로 차단된다. `ReviewRequired`도 source,
catalog fingerprint/version, 분류 정책, 객체 revision과 15분 유효시간에 묶인다.
`candidate_token`은 scope·사용자·대화·source·연결 세대·원 질문 hash를 묶은
opaque HMAC이다.
host는 `CandidateSet`에서 받은 token과 원 질문을 그대로 `QueryDraft`에 주입해야 하며,
모델이 질문을 바꾸거나 다른 후보 응답의 token을 재사용하면
`candidate_question_mismatch`로 차단된다.
이 token은 15분짜리 Discord action token이나 일회용 실행 권한이 아니다. 같은 runtime
인스턴스와 source 안에서는 별도 만료시각 없이 질문 binding을 증명하지만, `plan`은
매번 현재 catalog와 shortlist를 다시 검증한다. Host는 한 질의가 끝나면 token을
폐기하고, 새 사용자 turn에는 `candidates`를 다시 호출한다.
프로세스가 재시작되면 사람의 검토 결정은 저장할 수 있지만 민감 draft는 복원하지
않고 후보 HMAC 서명키도 회전한다. 이때 `FeedbackApplied.next`는 비어 있으며 host는
사용자에게 같은 질문을 다시 받아 `candidates`부터 호출하고, 새 `source`와
`candidate_token`으로 `QueryDraft`를 다시 조립해야 한다. 과거 draft/token을 그대로
재사용하면 `candidate_question_mismatch`로 차단되는 것이 정상이다.
사람 검토 변경은 catalog와 audit를 같은 SQLite transaction에 기록하므로, 현재
`ContextConcierge`에 외부 audit port를 주입한 구성에서는 feedback이
`semantic_audit_not_atomic`으로 차단된다. 실행 audit의 외부 port 지원과 의미 변경의
원자적 audit 지원을 같은 것으로 간주하지 않는다.

## 후보와 사람 검토

- `MetricCandidate`: ID, 안전하게 제한한 label, 질문에 실제 있는
  `grounded_phrase`, 허용 집계를 제공한다.
- `FilterCandidate`: 위 필드와 `allowed_value_kinds`를 제공한다. host는 이 타입과
  맞지 않는 literal을 보내지 않아야 하며 서버도 다시 검증한다.
- `TimeCandidate`: native `DATE` 차원만 제공하고 endpoint kind는 `DATE`로 고정한다.
- `ReviewCandidate`: 아직 선택할 수 없는 차원과 필요한 조치만 알린다. review token,
  DB 값, 질문의 필터 값은 포함하지 않는다.
- 후보에 없거나 의미가 모호한 항목을 host가 임의 ID로 채우면 plan이 차단한다.

연결 응답은 첫 20개 검토만 표시한다. 더 많은 차원이 있어도 질문에 정확히 맞는
항목은 `candidates`의 `review_required_dimensions`에 나타나고, typed draft가 실제로
그 항목을 참조할 때 하나의 on-demand review가 발급된다.

## 현재 실행 계약

지원:

- 기존 file-backed SQLite와 DuckDB의 read-only governed execution
- `SUM`/`AVG`/검토된 공개 범위의 `MIN`/`MAX`, source-record `COUNT(*)`
- categorical group-by와 유일한 declared child-to-parent FK 경로
- 최대 8개 AND 필터, exact `EQ` 또는 최대 20개 값의 `IN`
- 문자열·정수·소수·불리언 bound parameter
- native `DATE`의 ISO date `[start, end)` 기간창

의도적으로 차단:

- raw SQL, row projection, OR/NOT, 부분 문자열·자유 검색
- relative time, timestamp timezone, fiscal/cohort calendar 추정
- 자유 수식과 파생 지표 실행
- composite FK, fan-out, 동률 join path
- 사람 검토 전 공개 값, PII/credential/서술문 컬럼
- 검증된 timeout/cancel 계약이 없는 원격 dialect의 governed execution

파생 지표용 AST/DAG·grain·unit·NULL/0 나눗셈 계약은 내부 IR에 정의돼 있지만,
compiler는 정책 전파와 dialect 증거가 추가될 때까지 실행을 fail-closed한다.

## DuckDB 설치

```bash
python -m pip install -e ".[duckdb]"
```

DuckDB는 존재하는 로컬 파일만 허용한다. 연결은 read-only이고 외부 접근,
extension 자동 설치/로드와 community extension을 끈 뒤 설정을 잠근다. `:memory:`와
존재하지 않는 경로는 governed execution으로 승격하지 않는다.
