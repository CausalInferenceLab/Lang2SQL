> **레거시 유틸리티**: `RunContext`는 현재 레거시 유틸리티로 유지됩니다.
> 새 코드에서는 명시적 Python 인자를 사용하는 것을 권장합니다.
> 컴포넌트 I/O는 `RunContext` 대신 구체적인 타입(str, list 등)으로 표현하세요.

## RunContext

`RunContext`는 define-by-run 파이프라인에서 **상태(state)를 운반하는 State Carrier**입니다.
레거시 파이프라인이나 직접 상태를 조합할 때 유용합니다.

### 설계 원칙

* **최소 루트 필드 5개만 고정**: `inputs / artifacts / outputs / error / metadata`
* 루트는 전부 `dict` 기반(스키마 락인 방지)
* 자주 쓰는 값은 **alias 프로퍼티**로 제공하여 UX 개선 (`run.query`, `run.sql` 등)

---

## 데이터 구조 트리

아래는 `RunContext`가 담는 데이터 구조를 "트리 형태"로 나타낸 것입니다.

```
RunContext
├─ inputs: dict
│  └─ "query": str
│
├─ artifacts: dict
│  └─ "schema": dict
│     ├─ "catalog": Any
│     │    (예: list[TableSchema] | provider | None)
│     ├─ "selected": Any
│     │    (예: list[TableCandidate] | None)
│     └─ "context": str
│          (prompt에 넣을 스키마 컨텍스트)
│
├─ outputs: dict
│  ├─ "sql": str
│  └─ "validation": Any
│
├─ error: dict | None
│  └─ (구조화된 에러 정보. 형식은 프로젝트 정책에 따라 확장 가능)
│
└─ metadata: dict
   └─ (로그/추적/히스토리/실험용 값. 표준 스키마 강제 없음)
      예)
      ├─ "events": list[Event]
      ├─ "sql_drafts": list[str]
      ├─ "attempt": int
      └─ ...
```

---

## Root fields (고정 5개)

* `inputs: dict[str, Any]` — 사용자 입력
* `artifacts: dict[str, Any]` — 중간 산출물
* `outputs: dict[str, Any]` — 최종 산출물
* `error: Optional[dict[str, Any]]` — 구조화된 에러(선택)
* `metadata: dict[str, Any]` — 로그/추적/히스토리(선택)

---

## 권장 키 컨벤션 (Minimal Standard)

### inputs

* `inputs["query"]`: 자연어 질의

### artifacts["schema"]

* `catalog`: 스키마 카탈로그(테이블/컬럼 목록 등)
* `selected`: 선택된 테이블 후보
* `context`: 프롬프트에 들어갈 스키마 컨텍스트 문자열

### outputs

* `outputs["sql"]`: 최종 SQL
* `outputs["validation"]`: 검증 결과(구조는 구현체 자유)

---

## Alias (Beginner-friendly API)

키 문자열 접근을 줄이기 위해 alias를 제공합니다.

* `run.query` ↔ `inputs["query"]`
* `run.sql` ↔ `outputs["sql"]`
* `run.validation` ↔ `outputs["validation"]`

스키마 관련 alias:

* `run.schema` ↔ `artifacts["schema"]` *(항상 dict로 보정)*
* `run.schema_catalog` ↔ `run.schema["catalog"]`
* `run.schema_selected` ↔ `run.schema["selected"]`
* `run.schema_context` ↔ `run.schema["context"]`

---

## 파이프라인 예시 (Text2SQL) — 새 API

새 API에서는 각 컴포넌트가 명시적 인자를 주고받습니다.

```python
query = "지난달 매출"

schemas = retriever(query)                  # str → list[CatalogEntry]
context = builder(query, schemas)           # str, list → str
sql     = generator(query, context)         # str, str → str
result  = validator(sql)                    # str → ValidationResult
```

또는 `SequentialFlow`로 조합:

```python
flow = SequentialFlow(steps=[retriever, builder, generator, validator])
result = flow.run(query)
```

---

## RunContext 직접 사용 (레거시)

기존 코드나 직접 상태를 조합할 때만 사용합니다.

```python
from lang2sql.core.context import RunContext

run = RunContext(query="지난달 매출")
# run을 직접 조작하거나 레거시 컴포넌트에 전달
run.metadata["session_id"] = "abc123"
```
