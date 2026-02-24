# lang2sql Quickstart — 처음 사용자를 위한 튜토리얼

이 문서를 위에서 아래로 따라가면 **설치 → API 키 → 샘플 DB → 기본 파이프라인 → Hook 트레이싱 → 커스터마이징**까지 모두 체험할 수 있습니다.

---

## 목차

1. [설치](#1-설치)
2. [API 키 설정](#2-api-키-설정)
3. [샘플 DB 세팅](#3-샘플-db-세팅)
4. [SQLAlchemyDB 연결 설정](#4-sqlalchemydb-연결-설정)
5. [예제 카탈로그](#5-예제-카탈로그)
6. [기본 사용 — BaselineNL2SQL](#6-기본-사용--baselinenl2sql)
   - 6-A. Anthropic Claude + SQLite
   - 6-B. OpenAI GPT + SQLite
   - 6-C. PostgreSQL 연결
   - 6-D. 지원 DB 방언(dialect) 목록
7. [Hook으로 실행 추적하기](#7-hook으로-실행-추적하기)
8. [고급 사용 — 컴포넌트 직접 조합](#8-고급-사용--컴포넌트-직접-조합)
9. [커스터마이징](#9-커스터마이징)
   - 9-A. 시스템 프롬프트 교체
   - 9-B. 나만의 LLM 연결
   - 9-C. 나만의 DB 연결
   - 9-D. 커스텀 컴포넌트 만들기
   - 9-E. 커스텀 플로우 만들기
10. [에러 처리](#10-에러-처리)
11. [전체 기능 체크리스트](#11-전체-기능-체크리스트)

---

## 1. 설치

```bash
pip install lang2sql
```

> 개발 환경에서 uv를 사용하는 경우:
> ```bash
> uv sync
> ```

`anthropic`, `sqlalchemy`는 기본 의존성에 포함되어 있어 별도 설치가 필요 없습니다.

> **패키지 업데이트 후** `pyproject.toml`이 변경되었다면 반드시 `uv sync`를 다시 실행하세요.

---

## 2. API 키 설정

OpenAI, Anthropic SDK는 **환경변수를 자동으로 읽습니다.**
`api_key`를 코드에 직접 쓰지 않아도 됩니다.

### 방법 A — 환경변수 (권장)

```bash
# Anthropic
export ANTHROPIC_API_KEY="sk-ant-..."

# OpenAI
export OPENAI_API_KEY="sk-..."
```

### 방법 B — `.env` 파일

프로젝트 루트에 `.env` 파일을 만들고:

```env
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...
```

Python 코드에서 로드:

```python
from dotenv import load_dotenv
load_dotenv()
```

### 방법 C — 키 이름이 다를 때

환경변수 이름이 다른 경우(예: `OPEN_AI_KEY`) `api_key`로 직접 전달하거나 표준 이름으로 복사합니다:

```python
import os

# 방법 1 — api_key로 직접 전달
llm = OpenAILLM(model="gpt-4o", api_key=os.environ["OPEN_AI_KEY"])

# 방법 2 — SDK가 읽는 표준 이름으로 복사 (이후 api_key 생략 가능)
os.environ["OPENAI_API_KEY"] = os.environ["OPEN_AI_KEY"]
llm = OpenAILLM(model="gpt-4o")
```

---

## 3. 샘플 DB 세팅

튜토리얼 코드를 실제 DB로 바로 실행해볼 수 있도록 샘플 데이터를 제공합니다.
고객 10명, 상품 12개, 주문 44건, 주문항목 83개가 포함됩니다.

### SQLite (서버 불필요 — 바로 시작 가능)

```bash
# 프로젝트 루트에서 실행 — sample.db 파일이 현재 디렉토리에 생성됩니다
python scripts/setup_sample_db.py
```

```
연결 중: sqlite:///sample.db
테이블 생성 완료: customers, products, orders, order_items
  고객:       10명
  상품:       12개
  주문:       44건
  주문 항목:  83개

─── 검증 쿼리 결과 ───────────────────────────────
  전체 주문 수:          44건
  gold 등급 고객 수:     3명
  재고 10개 미만 상품:   5개
    - 무선 마우스: 3개
    - 후드 집업: 4개
    ...
─────────────────────────────────────────────────
완료! 아래 URL로 quickstart.md를 따라해 보세요:
  sqlite:///sample.db
```

> `sample.db`는 스크립트를 실행한 디렉토리에 생성됩니다.
> Python 코드도 같은 디렉토리에서 실행해야 `sqlite:///sample.db`로 연결됩니다.

### PostgreSQL (Docker 사용)

```bash
# 1. 컨테이너 기동 (처음 한 번만)
docker compose -f docker/docker-compose-postgres.yml up -d

# 2. 샘플 데이터 삽입
python scripts/setup_sample_db.py --db postgres
```

> **커스텀 URL**을 사용하려면:
> ```bash
> python scripts/setup_sample_db.py --url "postgresql://myuser:mypass@myhost:5432/mydb"
> ```

---

## 4. SQLAlchemyDB 연결 설정

`SQLAlchemyDB`는 lang2sql이 SQL을 실제 DB에서 실행할 때 사용하는 DB 어댑터입니다.
SQLAlchemy URL만 넘기면 연결이 완료됩니다.

```python
from lang2sql.integrations.db import SQLAlchemyDB

# SQLite — 3번에서 생성한 sample.db에 바로 연결
db = SQLAlchemyDB("sqlite:///sample.db")

# PostgreSQL
db = SQLAlchemyDB("postgresql://postgres:postgres@localhost:5432/postgres")

# MySQL
db = SQLAlchemyDB("mysql+mysqlconnector://user:pass@localhost:3306/dbname")

# DuckDB (인메모리)
db = SQLAlchemyDB("duckdb:///:memory:")
```

### 연결 확인

```python
# execute()를 직접 호출해서 확인
rows = db.execute("SELECT name, grade FROM customers WHERE grade = 'gold'")
print(rows)
# [{'name': '김철수', 'grade': 'gold'}, {'name': '박영희', 'grade': 'gold'}, ...]

# 테이블 목록 확인 (SQLite)
print(db.execute("SELECT name FROM sqlite_master WHERE type='table'"))
# [{'name': 'customers'}, {'name': 'products'}, {'name': 'orders'}, {'name': 'order_items'}]
```

---

## 5. 예제 카탈로그

카탈로그는 **어떤 테이블이 있는지** 알려주는 메타데이터 목록입니다.
`KeywordRetriever`가 이 카탈로그를 BM25로 검색해 질문과 관련된 테이블을 찾습니다.

### 현재 방식 — 직접 정의

`setup_sample_db.py`로 만든 DB와 컬럼 구조가 일치하도록 아래와 같이 정의합니다.

```python
from lang2sql import CatalogEntry

CATALOG: list[CatalogEntry] = [
    {
        "name": "orders",
        "description": "고객 주문 정보 테이블. 주문 건수, 금액, 날짜 조회에 사용.",
        "columns": {
            "order_id":    "주문 고유 ID (PK)",
            "customer_id": "주문한 고객 ID (FK → customers)",
            "order_date":  "주문 일시 (TIMESTAMP)",
            "amount":      "주문 금액 (DECIMAL)",
            "status":      "주문 상태: pending / confirmed / shipped / cancelled",
        },
    },
    {
        "name": "customers",
        "description": "고객 마스터 데이터. 고객 이름, 가입일, 등급 조회에 사용.",
        "columns": {
            "customer_id":  "고객 고유 ID (PK)",
            "name":         "고객 이름",
            "email":        "이메일 주소",
            "joined_at":    "가입 일시 (TIMESTAMP)",
            "grade":        "고객 등급: bronze / silver / gold",
        },
    },
    {
        "name": "products",
        "description": "상품 정보 테이블. 상품명, 카테고리, 가격 조회에 사용.",
        "columns": {
            "product_id":  "상품 고유 ID (PK)",
            "name":        "상품명",
            "category":    "카테고리: electronics / clothing / food",
            "price":       "판매 가격 (DECIMAL)",
            "stock":       "현재 재고 수량 (INTEGER)",
        },
    },
    {
        "name": "order_items",
        "description": "주문별 상품 구성 테이블. 주문에 포함된 상품과 수량 조회에 사용.",
        "columns": {
            "item_id":    "항목 고유 ID (PK)",
            "order_id":   "주문 ID (FK → orders)",
            "product_id": "상품 ID (FK → products)",
            "quantity":   "주문 수량 (INTEGER)",
            "unit_price": "주문 당시 단가 (DECIMAL)",
        },
    },
]
```

### 향후 방향 — 카탈로그 자동 생성 (미구현, 제안)

**방법 A — SQLAlchemy inspect로 DB 스키마 읽기**
```python
# (현재 미구현, 아이디어 예시)
from sqlalchemy import create_engine, inspect

engine = create_engine("sqlite:///sample.db")
insp = inspect(engine)

catalog = []
for table_name in insp.get_table_names():
    columns = {
        col["name"]: str(col["type"])
        for col in insp.get_columns(table_name)
    }
    catalog.append({"name": table_name, "columns": columns})
```

**방법 B — DataHub 등 메타데이터 플랫폼 연동**
- 이미 `utils/data/` 아래에 DataHub 연동 코드가 있습니다.
- 향후 `CatalogEntry` 형식으로 변환하는 어댑터를 추가할 예정입니다.

---

## 6. 기본 사용 — BaselineNL2SQL

가장 빠른 사용법입니다. LLM과 DB만 연결하면 자연어 → SQL → 실행 결과를 얻습니다.

> **`db_dialect` 파라미터를 반드시 지정하세요.**
> SQLite는 `MONTH()`, `YEAR()` 같은 MySQL/PostgreSQL 함수를 지원하지 않습니다.
> `db_dialect`를 지정하면 해당 DB에 맞는 SQL 함수를 사용하는 프롬프트가 자동으로 적용됩니다.

### 6-A. Anthropic Claude + SQLite

```python
from lang2sql import BaselineNL2SQL
from lang2sql.integrations.llm import AnthropicLLM
from lang2sql.integrations.db import SQLAlchemyDB

pipeline = BaselineNL2SQL(
    catalog=CATALOG,
    llm=AnthropicLLM(model="claude-sonnet-4-6"),
    db=SQLAlchemyDB("sqlite:///sample.db"),
    db_dialect="sqlite",   # ← DB 방언 지정
)

result = pipeline.run("이번달에 10만원 이상 주문한 고객 이름과 주문 금액을 알려줘")
print(result)
# 예시 출력: [{'name': '김철수', 'amount': Decimal('320000')}, ...]

result2 = pipeline.run("gold 등급 고객 목록을 이름 순으로 보여줘")
print(result2)
# 예시 출력: [{'name': '김철수'}, {'name': '박영희'}, {'name': '이민준'}]
```

### 6-B. OpenAI GPT + SQLite

```python
from lang2sql import BaselineNL2SQL
from lang2sql.integrations.llm import OpenAILLM
from lang2sql.integrations.db import SQLAlchemyDB

pipeline = BaselineNL2SQL(
    catalog=CATALOG,
    llm=OpenAILLM(model="gpt-4o"),
    db=SQLAlchemyDB("sqlite:///sample.db"),
    db_dialect="sqlite",
)

result = pipeline.run("재고가 10개 미만인 상품 목록")
print(result)
# 예시 출력: [{'name': '무선 마우스', 'stock': 3}, {'name': '후드 집업', 'stock': 4}, ...]
```

### 6-C. PostgreSQL 연결

```python
pipeline = BaselineNL2SQL(
    catalog=CATALOG,
    llm=AnthropicLLM(model="claude-sonnet-4-6"),
    db=SQLAlchemyDB("postgresql://postgres:postgres@localhost:5432/postgres"),
    db_dialect="postgresql",
)

result = pipeline.run("gold 등급 고객의 총 주문 금액")
```

### 6-D. 지원 DB 방언(dialect) 목록

| `db_dialect` | 대상 DB | 적용 내용 |
|---|---|---|
| `"sqlite"` | SQLite | `strftime()` 사용, `MONTH()`/`YEAR()` 사용 안 함 |
| `"postgresql"` | PostgreSQL | `DATE_TRUNC`, `EXTRACT`, `INTERVAL` |
| `"mysql"` | MySQL | `MONTH()`, `YEAR()`, `DATE_FORMAT()` |
| `"bigquery"` | Google BigQuery | `DATE_TRUNC`, `EXTRACT`, `FORMAT_DATE` |
| `"duckdb"` | DuckDB | `DATE_TRUNC`, `EXTRACT`, `INTERVAL` |
| `None` / 생략 | 방언 무관 | 기본 프롬프트 (날짜 함수 미지정) |

---

## 7. Hook으로 실행 추적하기

`MemoryHook`을 달면 각 컴포넌트가 **언제, 얼마나 걸렸는지, 무엇을 받고 반환했는지** 전부 기록됩니다.

```python
from lang2sql import BaselineNL2SQL, MemoryHook
from lang2sql.integrations.llm import AnthropicLLM
from lang2sql.integrations.db import SQLAlchemyDB

hook = MemoryHook()

pipeline = BaselineNL2SQL(
    catalog=CATALOG,
    llm=AnthropicLLM(model="claude-sonnet-4-6"),
    db=SQLAlchemyDB("sqlite:///sample.db"),
    db_dialect="sqlite",
    hook=hook,
)

result = pipeline.run("주문 건수")

for event in hook.snapshot():
    dur = f" {event.duration_ms:.1f}ms" if event.duration_ms else ""
    print(f"[{event.name}] {event.component:20s} phase={event.phase}{dur}")
```

**예상 출력:**

```
[flow.run]      BaselineNL2SQL       phase=start
[component.run] KeywordRetriever     phase=start
[component.run] KeywordRetriever     phase=end    1.2ms
[component.run] SQLGenerator         phase=start
[component.run] SQLGenerator         phase=end    843.5ms
[component.run] SQLExecutor          phase=start
[component.run] SQLExecutor          phase=end    12.3ms
[flow.run]      BaselineNL2SQL       phase=end    857.0ms
```

### 이벤트 상세 정보

```python
events = hook.snapshot()

gen_events = [e for e in events if e.component == "SQLGenerator"]
for e in gen_events:
    print(f"  phase       : {e.phase}")
    print(f"  duration_ms : {e.duration_ms}")
    print(f"  input       : {e.input_summary}")
    print(f"  output      : {e.output_summary}")
```

### 에러 추적

```python
hook.clear()

try:
    pipeline.run("...")
except Exception:
    pass

error_events = [e for e in hook.snapshot() if e.phase == "error"]
for e in error_events:
    print(f"컴포넌트: {e.component}")
    print(f"에러:     {e.error}")
```

### 커스텀 Hook 만들기

`on_event(event)` 하나만 구현하면 됩니다.

```python
class PrintHook:
    def on_event(self, event):
        if event.phase == "start":
            print(f"▶ {event.component} 시작")
        elif event.phase == "end":
            print(f"✓ {event.component} 완료 ({event.duration_ms:.0f}ms)")
        elif event.phase == "error":
            print(f"✗ {event.component} 오류: {event.error}")


pipeline = BaselineNL2SQL(
    catalog=CATALOG,
    llm=AnthropicLLM(model="claude-sonnet-4-6"),
    db=SQLAlchemyDB("sqlite:///sample.db"),
    db_dialect="sqlite",
    hook=PrintHook(),
)

result = pipeline.run("재고 부족 상품 목록")
```

**예상 출력:**

```
▶ KeywordRetriever 시작
✓ KeywordRetriever 완료 (1ms)
▶ SQLGenerator 시작
✓ SQLGenerator 완료 (921ms)
▶ SQLExecutor 시작
✓ SQLExecutor 완료 (8ms)
```

---

## 8. 고급 사용 — 컴포넌트 직접 조합

파이프라인을 쓰지 않고 컴포넌트를 직접 사용할 수 있습니다.
각 단계 결과를 중간에 확인하거나 조건 분기를 넣고 싶을 때 유용합니다.

```python
from lang2sql import KeywordRetriever, SQLGenerator, SQLExecutor
from lang2sql.integrations.llm import AnthropicLLM
from lang2sql.integrations.db import SQLAlchemyDB

retriever = KeywordRetriever(catalog=CATALOG, top_n=3)
generator = SQLGenerator(
    llm=AnthropicLLM(model="claude-sonnet-4-6"),
    db_dialect="sqlite",
)
executor  = SQLExecutor(db=SQLAlchemyDB("sqlite:///sample.db"))

query = "gold 등급 고객의 이번 달 주문 총액"

# Step 1 — 관련 스키마 검색
schemas = retriever.run(query)
print("검색된 스키마:")
for s in schemas:
    print(f"  - {s['name']}: {s.get('description', '')}")

# Step 2 — SQL 생성
sql = generator.run(query, schemas)
print(f"\n생성된 SQL:\n{sql}")

# Step 3 — 실행
rows = executor.run(sql)
print(f"\n결과: {rows}")
```

---

## 9. 커스터마이징

### 9-A. 시스템 프롬프트 교체

`db_dialect`에 없는 DB(Snowflake, Trino 등)나 특별한 SQL 스타일이 필요할 때
`system_prompt`로 직접 지정합니다. `system_prompt`는 `db_dialect`보다 우선합니다.

```python
from lang2sql import SQLGenerator
from lang2sql.integrations.llm import AnthropicLLM

MY_PROMPT = """
You are a Snowflake SQL expert.
- Use DATEADD, DATEDIFF for date arithmetic
- Use TO_DATE() for date casting
- Use CURRENT_DATE() for today
- Return ONLY the SQL inside a ```sql ... ``` block
"""

generator = SQLGenerator(
    llm=AnthropicLLM(model="claude-sonnet-4-6"),
    system_prompt=MY_PROMPT,  # db_dialect 대신 직접 지정
)

sql = generator.run("이번 달 주문 건수", schemas)
```

프롬프트 파일은 `src/lang2sql/components/generation/prompts/` 아래에 있습니다.
새로운 dialect를 추가하려면 해당 경로에 `{dialect}.md` 파일을 만들면 됩니다.

### 9-B. 나만의 LLM 연결

`invoke(messages) -> str` 하나만 구현하면 어떤 LLM이든 연결됩니다.

```python
# 예: LangChain 모델 그대로 사용
from langchain_openai import ChatOpenAI

class LangChainLLM:
    def __init__(self, model: str):
        self._llm = ChatOpenAI(model=model)

    def invoke(self, messages: list[dict]) -> str:
        from langchain_core.messages import HumanMessage, SystemMessage
        lc_msgs = []
        for m in messages:
            if m["role"] == "system":
                lc_msgs.append(SystemMessage(content=m["content"]))
            else:
                lc_msgs.append(HumanMessage(content=m["content"]))
        return self._llm.invoke(lc_msgs).content


pipeline = BaselineNL2SQL(
    catalog=CATALOG,
    llm=LangChainLLM("gpt-4o"),
    db=SQLAlchemyDB("sqlite:///sample.db"),
    db_dialect="sqlite",
)
```

### 9-C. 나만의 DB 연결

`execute(sql) -> list[dict]` 하나만 구현하면 됩니다.

```python
# 예: pandas DataFrame을 DuckDB로 쿼리
class PandasDB:
    def __init__(self, dataframes: dict):
        import duckdb
        self._conn = duckdb.connect()
        for name, df in dataframes.items():
            self._conn.register(name, df)

    def execute(self, sql: str) -> list[dict]:
        result = self._conn.execute(sql).fetchdf()
        return result.to_dict(orient="records")


import pandas as pd

pipeline = BaselineNL2SQL(
    catalog=CATALOG,
    llm=AnthropicLLM(model="claude-sonnet-4-6"),
    db=PandasDB({"orders": pd.read_csv("orders.csv")}),
    db_dialect="duckdb",
)
```

### 9-D. 커스텀 컴포넌트 만들기

`BaseComponent`를 상속하고 `_run()`만 구현합니다.
Hook, 에러 처리, 타이밍은 자동으로 처리됩니다.

```python
from lang2sql.core.base import BaseComponent
from lang2sql.core.exceptions import ComponentError


class SQLValidator(BaseComponent):
    """생성된 SQL에 위험한 키워드가 있으면 ComponentError를 발생시킵니다."""

    FORBIDDEN = {"DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT", "ALTER"}

    def _run(self, sql: str) -> str:
        tokens = set(sql.upper().split())
        bad = tokens & self.FORBIDDEN
        if bad:
            raise ComponentError(self.name, f"위험한 키워드 감지: {bad}")
        return sql


validator = SQLValidator()
safe_sql = validator.run("SELECT COUNT(*) FROM orders")   # OK
# validator.run("DROP TABLE orders")                      # ComponentError 발생
```

### 9-E. 커스텀 플로우 만들기

`BaseFlow`를 상속하고 `_run()`에서 **순수 Python 제어흐름**으로 컴포넌트를 조합합니다.

```python
from lang2sql.core.base import BaseFlow
from lang2sql.core.exceptions import ComponentError


class SafeNL2SQL(BaseFlow):
    """검증 단계를 추가한 파이프라인."""

    def __init__(self, *, catalog, llm, db, hook=None):
        super().__init__(name="SafeNL2SQL", hook=hook)
        self._retriever = KeywordRetriever(catalog=catalog, hook=hook)
        self._generator = SQLGenerator(llm=llm, db_dialect="sqlite", hook=hook)
        self._validator = SQLValidator(hook=hook)
        self._executor  = SQLExecutor(db=db, hook=hook)

    def _run(self, query: str):
        schemas = self._retriever.run(query)
        sql     = self._generator.run(query, schemas)
        sql     = self._validator.run(sql)   # 검증 통과 시에만 실행
        return  self._executor.run(sql)


pipeline = SafeNL2SQL(
    catalog=CATALOG,
    llm=AnthropicLLM(model="claude-sonnet-4-6"),
    db=SQLAlchemyDB("sqlite:///sample.db"),
)
result = pipeline.run("주문 건수")
```

#### 재시도 로직 예시

```python
class RetryNL2SQL(BaseFlow):
    """SQL 생성 실패 시 최대 3번 재시도합니다."""

    def __init__(self, *, catalog, llm, db, hook=None):
        super().__init__(name="RetryNL2SQL", hook=hook)
        self._retriever = KeywordRetriever(catalog=catalog, hook=hook)
        self._generator = SQLGenerator(llm=llm, db_dialect="sqlite", hook=hook)
        self._executor  = SQLExecutor(db=db, hook=hook)

    def _run(self, query: str):
        schemas = self._retriever.run(query)

        last_error = None
        for attempt in range(3):
            try:
                sql = self._generator.run(query, schemas)
                return self._executor.run(sql)
            except ComponentError as e:
                last_error = e
                print(f"  시도 {attempt + 1} 실패: {e}")

        raise last_error
```

---

## 10. 에러 처리

```python
from lang2sql import ComponentError, IntegrationMissingError, Lang2SQLError

try:
    result = pipeline.run("주문 건수")
except ComponentError as e:
    # 특정 컴포넌트에서 발생한 에러
    print(f"컴포넌트 오류: {e.component}")
    print(f"메시지: {e}")
    if e.cause:
        print(f"원인: {e.cause}")

except IntegrationMissingError as e:
    # 패키지 미설치 (anthropic, sqlalchemy 등)
    print(f"패키지 미설치: {e}")
    # → uv sync 또는 pip install lang2sql 재실행

except Lang2SQLError as e:
    # 그 외 lang2sql 도메인 에러
    print(f"도메인 오류: {e}")
```

**에러 계층:**

```
Lang2SQLError
├── ComponentError          — 컴포넌트 실행 실패 (component, cause 속성)
├── IntegrationMissingError — 선택적 패키지 미설치
└── ValidationError         — 검증 실패
```

**자주 발생하는 에러:**

| 에러 | 원인 | 해결 |
|------|------|------|
| `IntegrationMissingError: anthropic` | anthropic 미설치 | `uv sync` |
| `OpenAIError: api_key must be set` | 환경변수 미설정 | `export OPENAI_API_KEY=...` |
| `no such table: products` | DB URL 오타 또는 sample.db 미생성 | `python scripts/setup_sample_db.py` 실행 |
| `no such function: MONTH` | SQLite에서 MySQL 함수 사용 | `db_dialect="sqlite"` 지정 |

---

## 11. 전체 기능 체크리스트

실제 API 키나 DB 없이도 FakeLLM/FakeDB로 전체 흐름을 확인할 수 있습니다.

```python
"""
lang2sql 전체 기능 체크리스트
아래 코드를 순서대로 실행하면 모든 기능을 테스트할 수 있습니다.
"""

# ── 0. 의존성 없는 Fake 구현 ──────────────────────────────────────────────────

class FakeLLM:
    """실제 API 키 없이 테스트할 수 있는 가짜 LLM."""
    def invoke(self, messages):
        user_msg = next(m["content"] for m in reversed(messages) if m["role"] == "user")
        if "주문" in user_msg:
            return "```sql\nSELECT COUNT(*) AS cnt FROM orders\n```"
        if "고객" in user_msg:
            return "```sql\nSELECT name FROM customers ORDER BY name\n```"
        return "```sql\nSELECT 1\n```"

class FakeDB:
    """실제 DB 없이 테스트할 수 있는 가짜 DB."""
    _data = {
        "SELECT COUNT(*) AS cnt FROM orders": [{"cnt": 44}],
        "SELECT name FROM customers ORDER BY name": [{"name": "김철수"}, {"name": "박영희"}],
    }
    def execute(self, sql):
        return self._data.get(sql, [{"result": "ok"}])


# ── 1. 카탈로그 정의 ──────────────────────────────────────────────────────────

from lang2sql import CatalogEntry

catalog: list[CatalogEntry] = [
    {
        "name": "orders",
        "description": "주문 정보 테이블",
        "columns": {"order_id": "PK", "customer_id": "FK", "amount": "금액"},
    },
    {
        "name": "customers",
        "description": "고객 마스터 데이터",
        "columns": {"customer_id": "PK", "name": "이름", "grade": "등급"},
    },
]


# ── 2. KeywordRetriever 단독 테스트 ───────────────────────────────────────────

from lang2sql import KeywordRetriever

retriever = KeywordRetriever(catalog=catalog, top_n=2)
schemas = retriever.run("주문 건수 조회")
print("✓ KeywordRetriever")
print(f"  검색 결과: {[s['name'] for s in schemas]}")
# 예상: ['orders']


# ── 3. SQLGenerator 단독 테스트 ───────────────────────────────────────────────

from lang2sql import SQLGenerator

generator = SQLGenerator(llm=FakeLLM())
sql = generator.run("주문 건수", schemas)
print("\n✓ SQLGenerator")
print(f"  생성 SQL: {sql}")
# 예상: SELECT COUNT(*) AS cnt FROM orders


# ── 4. SQLExecutor 단독 테스트 ────────────────────────────────────────────────

from lang2sql import SQLExecutor

executor = SQLExecutor(db=FakeDB())
rows = executor.run(sql)
print("\n✓ SQLExecutor")
print(f"  결과: {rows}")
# 예상: [{'cnt': 44}]


# ── 5. BaselineNL2SQL (기본 파이프라인) 테스트 ────────────────────────────────

from lang2sql import BaselineNL2SQL

pipeline = BaselineNL2SQL(catalog=catalog, llm=FakeLLM(), db=FakeDB())
result = pipeline.run("주문 건수")
print("\n✓ BaselineNL2SQL")
print(f"  결과: {result}")
# 예상: [{'cnt': 44}]


# ── 6. db_dialect 테스트 ──────────────────────────────────────────────────────

pipeline_sqlite = BaselineNL2SQL(
    catalog=catalog, llm=FakeLLM(), db=FakeDB(), db_dialect="sqlite"
)
result = pipeline_sqlite.run("주문 건수")
print("\n✓ db_dialect='sqlite'")
print(f"  결과: {result}")


# ── 7. MemoryHook 트레이싱 테스트 ─────────────────────────────────────────────

from lang2sql import MemoryHook

hook = MemoryHook()
pipeline_traced = BaselineNL2SQL(catalog=catalog, llm=FakeLLM(), db=FakeDB(), hook=hook)
pipeline_traced.run("주문 건수")

events = hook.snapshot()
print("\n✓ MemoryHook 이벤트")
for e in events:
    dur = f" {e.duration_ms:.1f}ms" if e.duration_ms else ""
    print(f"  [{e.name}] {e.component:20s} phase={e.phase}{dur}")

component_starts = [e for e in events if e.name == "component.run" and e.phase == "start"]
print(f"\n  component start 이벤트 수: {len(component_starts)}  (예상: 3)")


# ── 8. 에러 처리 테스트 ────────────────────────────────────────────────────────

from lang2sql import ComponentError

class BrokenLLM:
    def invoke(self, messages):
        return "SQL 없이 일반 텍스트만 반환"  # 코드블록 없음

try:
    bad_pipeline = BaselineNL2SQL(catalog=catalog, llm=BrokenLLM(), db=FakeDB())
    bad_pipeline.run("주문")
except ComponentError as e:
    print(f"\n✓ ComponentError 정상 발생")
    print(f"  컴포넌트: {e.component}")
    print(f"  메시지:   {e}")


# ── 9. 커스텀 Hook 테스트 ─────────────────────────────────────────────────────

class PrintHook:
    def on_event(self, event):
        if event.phase == "start":
            print(f"  ▶ {event.component}")
        elif event.phase == "end":
            print(f"  ✓ {event.component} ({event.duration_ms:.0f}ms)")

print("\n✓ 커스텀 PrintHook")
BaselineNL2SQL(catalog=catalog, llm=FakeLLM(), db=FakeDB(), hook=PrintHook()).run("고객 목록")


# ── 10. 커스텀 컴포넌트 테스트 ─────────────────────────────────────────────────

from lang2sql.core.base import BaseComponent

class UpperCaseSQL(BaseComponent):
    """SQL을 대문자로 변환하는 후처리 컴포넌트."""
    def _run(self, sql: str) -> str:
        return sql.upper()

upper = UpperCaseSQL()
print(f"\n✓ 커스텀 BaseComponent")
print(f"  결과: {upper.run('select 1')}")
# 예상: SELECT 1


# ── 11. public API import 확인 ────────────────────────────────────────────────

from lang2sql import (
    CatalogEntry, LLMPort, DBPort,
    KeywordRetriever, SQLGenerator, SQLExecutor,
    BaselineNL2SQL,
    TraceHook, MemoryHook, NullHook,
    Lang2SQLError, ComponentError, IntegrationMissingError,
)
print("\n✓ 모든 public import 성공")

print("\n" + "="*50)
print("모든 체크리스트 통과! lang2sql 준비 완료.")
print("="*50)
```

---

## 참고: 아키텍처 한눈에 보기

```
BaselineNL2SQL.run("자연어 질문")
│
├── KeywordRetriever.run(query)
│   └── BM25 키워드 검색 → list[CatalogEntry]
│
├── SQLGenerator.run(query, schemas)
│   ├── _load_prompt(db_dialect)  → prompts/{dialect}.md 로드
│   ├── _build_context(schemas)   → 스키마 텍스트 구성
│   ├── llm.invoke(messages)      → LLM 호출
│   └── _extract_sql(response)    → ```sql...``` 파싱
│
└── SQLExecutor.run(sql)
    └── db.execute(sql)           → list[dict]

모든 단계에서 Hook이 start / end / error 이벤트를 기록합니다.
```

**컴포넌트 확장 포인트:**

| 인터페이스 | 구현할 메서드 | 용도 |
|-----------|-------------|------|
| `LLMPort` | `invoke(messages) -> str` | LLM 백엔드 교체 |
| `DBPort`  | `execute(sql) -> list[dict]` | DB 백엔드 교체 |
| `BaseComponent` | `_run(*args) -> Any` | 새 컴포넌트 추가 |
| `BaseFlow` | `_run(*args) -> Any` | 새 파이프라인 조합 |
| `TraceHook` | `on_event(event) -> None` | 커스텀 모니터링 |

**프롬프트 파일 위치:**

```
src/lang2sql/components/generation/prompts/
├── default.md      ← db_dialect 미지정 시
├── sqlite.md
├── postgresql.md
├── mysql.md
├── bigquery.md
└── duckdb.md
```
