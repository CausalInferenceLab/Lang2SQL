## utils.llm 개요

Lang2SQL 파이프라인에서 LLM, 검색(RAG), 그래프 워크플로우, DB 실행, 시각화 등 보조 유틸리티를 모아둔 패키지입니다. 이 문서는 depth(계층)별로 기능과 통합 흐름을 정리합니다.

## 디렉토리 구조

```
utils/llm/
├── README.md                    # 이 파일
├── chains.py                    # LangChain 체인 생성 모듈
├── retrieval.py                 # 테이블 메타 검색 및 재순위화
├── llm_response_parser.py       # LLM 응답에서 SQL 블록 추출
├── chatbot/                   # LangGraph ChatBot 패키지
│   ├── __init__.py
│   ├── core.py                # ChatBot 핵심 로직
│   └── README.md                # [상세 문서](./chatbot/README.md)
├── core/                        # LLM/Embedding 팩토리 모듈
│   ├── __init__.py
│   ├── factory.py               # LLM 및 Embedding 모델 생성 팩토리
│   └── README.md                # [상세 문서](./core/README.md)
├── graph_utils/                 # LangGraph 워크플로우 모듈
│   ├── __init__.py
│   ├── base.py                  # 공통 상태 및 노드 함수
│   ├── basic_graph.py           # 기본 워크플로우 그래프
│   ├── enriched_graph.py        # 확장된 워크플로우 그래프
│   ├── profile_utils.py         # 프로파일 유틸리티 함수
│   └── README.md                # [상세 문서](./graph_utils/README.md)
├── vectordb/                    # 벡터 데이터베이스 모듈
│   ├── __init__.py
│   ├── factory.py               # VectorDB 팩토리
│   ├── faiss_db.py              # FAISS 벡터DB 구현
│   ├── pgvector_db.py           # pgvector 벡터DB 구현
│   └── README.md                # [상세 문서](./vectordb/README.md)
├── tools/                       # DataHub 메타데이터 및 ChatBot 도구
│   ├── __init__.py
│   ├── datahub.py               # DataHub 메타데이터 수집
│   ├── chatbot_tool.py          # LangGraph ChatBot용 Tool 함수들
│   └── README.md                # [상세 문서](./tools/README.md)
└── output_schema/               # LLM 구조화 출력 Pydantic 모델
    ├── document_suitability.py  # 문서 적합성 평가 모델
    ├── question_suitability.py  # 질문 적합성 판단 모델
    └── README.md                # [상세 문서](./output_schema/README.md)
```

## 모듈 상세 설명

### 최상위 유틸리티 파일

#### `chains.py`
**목적**: LangChain 기반 체인 생성 모듈

**주요 함수:**
- `create_query_maker_chain(llm)`: SQL 쿼리 생성 체인
- `create_profile_extraction_chain(llm)`: 질문 프로파일 추출 체인
- `create_query_enrichment_chain(llm)`: 질문 컨텍스트 보강 체인
- `create_question_gate_chain(llm)`: SQL 적합성 판별 체인
- `create_document_suitability_chain(llm)`: 문서 적합성 평가 체인

**내부 모델:**
- `QuestionProfile`: 자연어 질문의 특징을 구조화하는 Pydantic 모델

**의존성:**
- `utils.llm.core.get_llm`: LLM 인스턴스 생성
- `utils.llm.output_schema`: 구조화 출력 모델

**사용처:**
- `utils/llm/graph_utils/base.py`: 각 노드에서 체인 호출

#### `retrieval.py`
**목적**: 테이블 메타데이터 검색 및 재순위화

**주요 함수:**
- `search_tables(query, retriever_name, top_n, device)`: 테이블 메타데이터 검색
  - `retriever_name`: "기본" 또는 "Reranker"
  - `top_n`: 반환할 상위 결과 개수
- `get_retriever(retriever_name, top_n, device)`: 검색기 생성
- `load_reranker_model(device)`: 한국어 reranker 모델 로드

**의존성:**
- `utils.llm.vectordb.get_vector_db`: 벡터DB 인스턴스
- `ko-reranker`: 한국어 재순위화 모델

**사용처:**
- `utils/llm/graph_utils/base.py`: `get_table_info_node`에서 호출
- `utils/llm/tools/chatbot_tool.py`: `search_database_tables`에서 호출

#### `llm_response_parser.py`
**목적**: LLM 응답에서 `<SQL>`, `<해석>` 블록 추출

**사용처:**
- `engine/query_executor.py`: SQL 추출 함수

### 하위 디렉토리 모듈

#### `core/` - LLM/Embedding 팩토리
**목적**: 다양한 제공자(OpenAI, Azure, Bedrock, Gemini, Ollama, HuggingFace)의 LLM과 Embedding 모델을 통일된 인터페이스로 사용

**주요 기능:**
- `get_llm(**kwargs)`: 환경변수 기반 LLM 인스턴스 생성
- `get_embeddings()`: 환경변수 기반 Embedding 인스턴스 생성
- 제공자별 전용 함수들 제공

**사용처:**
- `utils/llm/chains.py`: `get_llm()` 호출
- `utils/llm/vectordb/faiss_db.py`: `get_embeddings()` 호출
- `utils/llm/vectordb/pgvector_db.py`: `get_embeddings()` 호출

**상세 문서**: [core/README.md](./core/README.md)

#### `graph_utils/` - LangGraph 워크플로우
**목적**: LangGraph를 사용하여 자연어 질문을 SQL 쿼리로 변환하는 워크플로우 구현

**주요 컴포넌트:**
- `base.py`: 공통 상태(`QueryMakerState`) 및 노드 함수들
- `basic_graph.py`: 기본 워크플로우 (QUESTION_GATE → GET_TABLE_INFO → EVALUATE_DOCUMENT_SUITABILITY → QUERY_MAKER)
- `enriched_graph.py`: 확장 워크플로우 (PROFILE_EXTRACTION, CONTEXT_ENRICHMENT 추가)

**워크플로우 노드:**
- `question_gate_node`: SQL 적합성 판별
- `get_table_info_node`: 벡터 검색으로 테이블 정보 수집
- `document_suitability_node`: 문서 적합성 평가
- `profile_extraction_node`: 질문 프로파일 추출
- `context_enrichment_node`: 질문 컨텍스트 보강
- `query_maker_node`: SQL 쿼리 생성

**사용처:**
- `engine/query_executor.py`: 그래프 선택 및 실행
- `interface/core/session_utils.py`: Streamlit 세션 그래프 초기화

**상세 문서**: [graph_utils/README.md](./graph_utils/README.md)

#### `vectordb/` - 벡터 데이터베이스
**목적**: 테이블 메타데이터를 벡터화하여 저장하고 검색

**주요 기능:**
- `get_vector_db()`: 환경변수 기반 벡터DB 인스턴스 반환
- FAISS: 로컬 디스크 기반 벡터DB
- pgvector: PostgreSQL 기반 벡터DB

**사용처:**
- `utils/llm/retrieval.py`: 테이블 검색 시 벡터DB 사용

**상세 문서**: [vectordb/README.md](./vectordb/README.md)

#### `tools/` - DataHub 메타데이터 및 ChatBot 도구
**목적**: DataHub 메타데이터 수집 및 LangGraph ChatBot용 Tool 함수 제공

**주요 기능:**
- `get_info_from_db()`: DataHub에서 테이블 메타데이터를 LangChain Document로 수집
- `get_metadata_from_db()`: 전체 메타데이터 딕셔너리 반환
- `search_database_tables()`: 벡터 검색 기반 테이블 정보 검색 Tool
- `get_glossary_terms()`: 용어집 정보 조회 Tool
- `get_query_examples()`: 쿼리 예제 조회 Tool

**사용처:**
- `utils/llm/vectordb/faiss_db.py`: 벡터DB 초기화 시 메타데이터 수집
- `utils/llm/vectordb/pgvector_db.py`: 벡터DB 초기화 시 메타데이터 수집
- `utils/llm/chatbot/`: ChatBot 도구로 사용

**상세 문서**: [tools/README.md](./tools/README.md)

#### `output_schema/` - 구조화 출력 모델
**목적**: LLM 구조화 출력을 위한 Pydantic 모델 정의

**주요 모델:**
- `QuestionSuitability`: SQL 생성 적합성 판단 결과
- `DocumentSuitability`: 단일 테이블 적합성 평가 결과
- `DocumentSuitabilityList`: 문서 적합성 평가 결과 리스트

**사용처:**
- `utils/llm/chains.py`: 체인 생성 시 구조화 출력 모델로 사용

**상세 문서**: [output_schema/README.md](./output_schema/README.md)

### 통합 흐름(End-to-End)

1. **사용자 질문 입력** → `engine/query_executor.execute_query()` 호출
2. **그래프 선택 및 컴파일** → `graph_utils/basic_graph` 또는 `graph_utils/enriched_graph` 선택
3. **QUESTION_GATE 노드** → `chains.create_question_gate_chain()`으로 SQL 적합성 판별
4. **GET_TABLE_INFO 노드** → `retrieval.search_tables()` 호출
   - `vectordb.get_vector_db()`로 벡터DB 로드
   - 유사도 검색 또는 Reranker로 재순위화
   - 관련 테이블/컬럼 메타데이터 반환
5. **EVALUATE_DOCUMENT_SUITABILITY 노드** → `chains.create_document_suitability_chain()`으로 문서 적합성 평가
6. **PROFILE_EXTRACTION 노드** (확장 그래프만) → `chains.create_profile_extraction_chain()`으로 질문 특성 추출
   - `graph_utils/profile_utils.profile_to_text()`로 텍스트 변환
7. **CONTEXT_ENRICHMENT 노드** (확장 그래프만) → `chains.create_query_enrichment_chain()`으로 질문 컨텍스트 보강
8. **QUERY_MAKER 노드** → `chains.create_query_maker_chain()`으로 SQL 생성
   - DB 가이드/메타데이터 기반으로 SQL 생성 (`<SQL>` 코드블록 포함)
9. **SQL 추출** → `llm_response_parser.extract_sql()`로 최종 SQL 추출
10. **실행 및 시각화** (선택적) → `infra/db/connect_db.run_sql()` 실행, `utils/visualization/display_chart`로 시각화

### 환경 변수 요약

#### LLM 관련
- **`LLM_PROVIDER`**: LLM 제공자 선택 (`openai`, `azure`, `bedrock`, `gemini`, `ollama`, `huggingface`)
- 제공자별 환경변수 (상세는 [core/README.md](./core/README.md) 참고)
  - OpenAI: `OPEN_AI_KEY`, `OPEN_AI_LLM_MODEL`
  - Azure: `AZURE_OPENAI_LLM_KEY`, `AZURE_OPENAI_LLM_ENDPOINT`, `AZURE_OPENAI_LLM_MODEL`, `AZURE_OPENAI_LLM_API_VERSION`
  - AWS Bedrock: `AWS_BEDROCK_LLM_MODEL`, `AWS_BEDROCK_LLM_ACCESS_KEY_ID`, `AWS_BEDROCK_LLM_SECRET_ACCESS_KEY`, `AWS_BEDROCK_LLM_REGION`
  - Gemini: `GEMINI_LLM_MODEL`
  - Ollama: `OLLAMA_LLM_MODEL`, `OLLAMA_LLM_BASE_URL`
  - HuggingFace: `HUGGING_FACE_LLM_MODEL`, `HUGGING_FACE_LLM_REPO_ID`, `HUGGING_FACE_LLM_ENDPOINT`, `HUGGING_FACE_LLM_API_TOKEN`

#### Embedding 관련
- **`EMBEDDING_PROVIDER`**: Embedding 제공자 선택 (`openai`, `azure`, `bedrock`, `gemini`, `ollama`, `huggingface`)
- 제공자별 환경변수 (상세는 [core/README.md](./core/README.md) 참고)

#### VectorDB 관련
- **`VECTORDB_TYPE`**: 벡터DB 타입 선택 (`faiss` 또는 `pgvector`, 기본: `faiss`)
- **FAISS**: `VECTORDB_LOCATION` (기본: `./dev/table_info_db`)
- **pgvector**: `PGVECTOR_HOST`, `PGVECTOR_PORT`, `PGVECTOR_USER`, `PGVECTOR_PASSWORD`, `PGVECTOR_DATABASE`, `PGVECTOR_COLLECTION`
- 상세는 [vectordb/README.md](./vectordb/README.md) 참고

#### DataHub 관련
- **`DATAHUB_SERVER`**: DataHub GMS 서버 URL (예: `http://localhost:8080`)
- 상세는 [tools/README.md](./tools/README.md) 참고

#### ClickHouse 관련 (SQL 실행 시)
- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`

### 핵심 사용 예시

#### 1. 기본 쿼리 실행

```python
from engine.query_executor import execute_query, extract_sql_from_result

res = execute_query(
    query="지난달 매출 추이 보여줘",
    database_env="postgres",
    retriever_name="Reranker",
    top_n=5,
    device="cpu",
    use_enriched_graph=True,
)

sql = extract_sql_from_result(res)
```

#### 2. 체인 직접 사용

```python
from utils.llm.core import get_llm
from utils.llm.chains import create_query_maker_chain

llm = get_llm()
chain = create_query_maker_chain(llm)

result = chain.invoke({
    "user_input": "지난달 매출",
    "user_database_env": "postgres",
    "searched_tables": {...}
})
```

#### 3. 벡터 검색 직접 사용

```python
from utils.llm.retrieval import search_tables

tables = search_tables(
    query="고객 정보",
    retriever_name="Reranker",
    top_n=5,
    device="cpu"
)
```

#### 4. 그래프 빌더 직접 사용

```python
from utils.llm.graph_utils import basic_builder, enriched_builder

# 기본 그래프
graph = basic_builder.compile()

# 확장 그래프
graph = enriched_builder.compile()

# 그래프 실행
result = graph.invoke({
    "messages": [HumanMessage(content="지난달 매출 보여줘")],
    "user_database_env": "postgres",
    # ... 기타 상태 정보
})
```

### 파일간 의존 관계

#### 상위 레벨 의존성
```
engine/query_executor.py
├── utils/llm/graph_utils/
│   ├── basic_graph.py
│   ├── enriched_graph.py
│   └── base.py
│       ├── utils/llm/chains.py
│       │   ├── utils/llm/core/get_llm()
│       │   └── utils/llm/output_schema/
│       └── utils/llm/retrieval.py
│           └── utils/llm/vectordb/get_vector_db()
│               ├── utils/llm/core/get_embeddings()
│               └── utils/llm/tools/get_info_from_db()
└── utils/llm/llm_response_parser.py
```

#### 주요 import 관계

**graph_utils 모듈:**
- `graph_utils/base.py` → `chains.py`, `retrieval.py` 사용
- `graph_utils/basic_graph.py` → `graph_utils/base.py` 사용
- `graph_utils/enriched_graph.py` → `graph_utils/base.py` 사용

**chains 모듈:**
- `chains.py` → `core/get_llm()`, `output_schema/` 사용

**retrieval 모듈:**
- `retrieval.py` → `vectordb/get_vector_db()` 사용

**vectordb 모듈:**
- `vectordb/faiss_db.py` → `core/get_embeddings()`, `tools/get_info_from_db()` 사용
- `vectordb/pgvector_db.py` → `core/get_embeddings()`, `tools/get_info_from_db()` 사용

**tools 모듈:**
- `tools/datahub.py` → DataHub 메타데이터 수집
- `tools/chatbot_tool.py` → `retrieval/search_tables()` 사용

**외부 의존성:**
- `engine/query_executor.py` → `graph_utils/` 사용
- `interface/core/session_utils.py` → `graph_utils/` 사용
- `interface/app_pages/graph_builder.py` → `graph_utils/base.py` 사용
