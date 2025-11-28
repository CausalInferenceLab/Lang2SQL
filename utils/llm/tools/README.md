## utils.llm.tools 개요

Lang2SQL 파이프라인에서 DataHub 메타데이터 수집 및 검색, 그리고 LangGraph ChatBot에서 사용하는 도구(Tool) 함수들을 제공하는 모듈입니다. 병렬 처리 기반의 효율적인 메타데이터 수집과 LangChain Tool 인터페이스를 통한 챗봇 기능을 지원합니다.

### 파일 구조

```
utils/llm/tools/
├── __init__.py          # 모듈 진입점, 6개 함수 export
├── datahub.py           # DataHub 메타데이터 수집 및 처리
└── chatbot_tool.py      # LangGraph ChatBot용 Tool 함수들
```

### 각 파일 상세 설명

#### __init__.py

**목적**: tools 모듈의 공개 인터페이스 정의

**Export 함수들**:

**datahub 모듈에서**:
- `set_gms_server`: GMS 서버 설정
- `get_table_schema`: LangChain Document 리스트로 테이블/컬럼 정보 반환
- `get_metadata_from_db`: 전체 메타데이터 딕셔너리 리스트 반환

**chatbot_tool 모듈에서**:
- `search_database_tables`: 벡터 검색 기반 테이블 정보 검색
- `get_glossary_terms`: 용어집 정보 조회
- `get_query_examples`: 쿼리 예제 조회

#### datahub.py

**목적**: DataHub에서 테이블 및 컬럼 메타데이터를 수집하고 LangChain Document 형식으로 변환

**주요 함수**:

1. **`set_gms_server(gms_server: str)`**
   - 환경변수 `DATAHUB_SERVER`를 설정하고 DatahubMetadataFetcher 초기화
   - 유효하지 않은 서버 URL 시 ValueError 발생

2. **`get_table_schema(max_workers: int = 8) -> List[Document]`**
   - DataHub에서 모든 테이블 메타데이터를 수집하여 LangChain Document 리스트 반환
   - 각 Document에는 테이블명, 설명, 컬럼 정보가 포함
   - 형식: `"{테이블명}: {설명}\nColumns:\n {컬럼명}: {컬럼설명}"`
   - `parallel_process()`로 병렬 처리 (기본 8 워커)
   - 반환: LangChain Document 리스트

3. **`get_metadata_from_db() -> List[Dict]`**
   - DataHub에서 전체 메타데이터를 딕셔너리 형태로 수집
   - 각 테이블의 상세 메타데이터 반환
   - 반환: 메타데이터 딕셔너리 리스트

**내부 헬퍼 함수**:

- **`parallel_process()`**: ThreadPoolExecutor 기반 병렬 처리 유틸리티
  - tqdm 진행률 표시 지원
  - `max_workers`: 동시 실행 워커 수 (기본: 8)

- **`_get_fetcher()`**: 환경변수 기반 DatahubMetadataFetcher 인스턴스 생성

- **`_process_urn()`**: URN에서 테이블명과 설명 추출

- **`_get_table_info()`**: 병렬 처리로 모든 테이블 정보 수집

- **`_get_column_info()`**: 특정 테이블의 컬럼 정보 수집

- **`_extract_dataset_name_from_urn()`**: URN에서 데이터셋 이름 추출

**의존성**:
- `langchain.schema.Document`: LangChain Document 타입
- `utils.data.datahub_source.DatahubMetadataFetcher`: DataHub 메타데이터 페처
- `concurrent.futures.ThreadPoolExecutor`: 병렬 처리
- `tqdm`: 진행률 표시

**사용처**:
- `utils/llm/vectordb/faiss_db.py`: FAISS 벡터DB 초기화 시 메타데이터 수집
- `utils/llm/vectordb/pgvector_db.py`: pgvector 벡터DB 초기화 시 메타데이터 수집

**특징**:
- 병렬 처리로 성능 최적화
- tqdm 진행률 표시
- 자동 재시도/에러 핸들링
- LangChain Document 형식 지원

#### chatbot_tool.py

**목적**: LangGraph ChatBot에서 사용하는 LangChain Tool 함수들

**주요 함수 (모두 `@tool` 데코레이터 사용)**:

1. **`search_database_tables(query, retriever_name, top_n, device)`**
   - **목적**: 자연어 쿼리를 기반으로 관련 테이블 정보 검색
   - **파라미터**:
     - `query`: 검색할 자연어 질문
     - `retriever_name`: "기본" 또는 "Reranker" (기본: "기본")
     - `top_n`: 반환할 테이블 개수 (기본: 5)
     - `device`: "cpu" 또는 "cuda" (기본: "cpu")
   - **반환**: `{테이블명: {table_description, 컬럼명: 컬럼설명, ...}}`
   - **내부**: `utils.llm.retrieval.search_tables()` 호출
   - **사용 시기**:
     - SQL 쿼리 생성 전 스키마 정보 필요
     - "어떤 테이블을 사용해야 해?"
     - "고객 관련 테이블 정보를 알려줘"

2. **`get_glossary_terms(gms_server)`**
   - **목적**: DataHub 용어집(Glossary) 정보 조회
   - **파라미터**:
     - `gms_server`: DataHub GMS 서버 URL (기본: "http://35.222.65.99:8080")
   - **반환**: `[{name, description, children: [...]}, ...]`
   - **내부**: `GlossaryService.get_glossary_data()` 호출
   - **특징**:
     - 조직 특화 용어/비즈니스 정의 제공
     - `_simplify_glossary_data()`로 필수 필드만 추출
   - **사용 시기**:
     - 모호한 용어 이해 필요
     - "용어집을 보여줘"
     - 조직 내부 용어 확인

3. **`get_query_examples(gms_server, start, count, query)`**
   - **목적**: DataHub에 저장된 SQL 쿼리 예제 조회
   - **파라미터**:
     - `gms_server`: DataHub GMS 서버 URL (기본: "http://35.222.65.99:8080")
     - `start`: 시작 위치 (기본: 0)
     - `count`: 반환 개수 (기본: 10)
     - `query`: 검색 쿼리 (기본: "*")
   - **반환**: `[{name, description, statement}, ...]`
   - **내부**: `QueryService.get_query_data()` 호출
   - **특징**: 조직 내 검증된 쿼리 패턴 참고
   - **사용 시기**:
     - 복잡한 쿼리 생성 시 참고
     - "쿼리 예제를 보여줘"
     - "비슷한 쿼리 있어?"

**내부 헬퍼 함수**:

- **`_simplify_glossary_data()`**: 용어집 데이터를 name, description, children만 포함하는 간단한 형태로 변환

**의존성**:
- `langchain_core.tools.tool`: LangChain Tool 데코레이터
- `utils.data.datahub_services.base_client.DataHubBaseClient`: DataHub 기본 클라이언트
- `utils.data.datahub_services.glossary_service.GlossaryService`: 용어집 서비스
- `utils.data.datahub_services.query_service.QueryService`: 쿼리 서비스
- `utils.llm.retrieval.search_tables`: 벡터 검색 함수

**사용처**:
- `utils/llm/chatbot.py`: ChatBot 클래스에서 LLM에 바인딩하여 사용

**특징**:
- LangChain Tool 인터페이스 준수
- LLM이 자동으로 필요 시 호출
- 상세한 docstring으로 LLM 이해도 향상
- 에러 처리 및 fallback 제공

### 사용 방법

#### 1. DataHub 메타데이터 수집 (vectorDB 초기화)

```python
from utils.llm.tools import get_table_schema

# 모든 테이블 메타데이터를 LangChain Document로 수집
documents = get_table_schema(max_workers=8)

# 각 document는 다음과 같은 형식:
# "테이블명: 설명\nColumns:\n 컬럼1: 설명1\n 컬럼2: 설명2"
```

#### 2. GMS 서버 설정

```python
from utils.llm.tools import set_gms_server

# DataHub 서버 설정
set_gms_server("http://localhost:8080")
```

#### 3. ChatBot에서 Tool 사용

```python
from utils.llm.chatbot import ChatBot
from utils.llm.tools import (
    search_database_tables,
    get_glossary_terms,
    get_query_examples
)

# ChatBot 초기화 (도구들이 자동으로 바인딩됨)
chatbot = ChatBot(
    openai_api_key="your-key",
    model_name="gpt-4o-mini",
    gms_server="http://localhost:8080"
)

# LLM이 필요시 자동으로 tool 호출
response = chatbot.invoke("고객 테이블 정보를 알려줘")
# LLM이 search_database_tables를 자동으로 호출
```

#### 4. 직접 Tool 함수 호출

```python
from utils.llm.tools import search_database_tables, get_glossary_terms, get_query_examples

# 테이블 검색
tables = search_database_tables(
    query="고객 정보",
    retriever_name="기본",
    top_n=5
)

# 용어집 조회
glossary = get_glossary_terms(gms_server="http://localhost:8080")

# 쿼리 예제 조회
queries = get_query_examples(
    gms_server="http://localhost:8080",
    start=0,
    count=10
)
```

### import 관계

**import하는 파일**:
- `utils/llm/chatbot.py`: `from utils.llm.tools import search_database_tables, get_glossary_terms, get_query_examples`
- `utils/llm/vectordb/faiss_db.py`: `from utils.llm.tools import get_table_schema`
- `utils/llm/vectordb/pgvector_db.py`: `from utils.llm.tools import get_table_schema`
- `interface/core/config/settings.py`: `from utils.llm.tools import set_gms_server`

**내부 의존성**:
- `utils.data.datahub_source.DatahubMetadataFetcher`: DataHub 메타데이터 페처
- `utils.data.datahub_services.*`: DataHub 서비스 레이어
- `utils.llm.retrieval.search_tables`: 벡터 검색 기능

**외부 의존성**:
- `langchain.schema.Document`: LangChain Document 타입
- `langchain_core.tools.tool`: LangChain Tool 데코레이터
- `concurrent.futures.ThreadPoolExecutor`: 병렬 처리
- `tqdm`: 진행률 표시

### 환경 변수 요약

- **`DATAHUB_SERVER`**: DataHub GMS 서버 URL (예: "http://localhost:8080")
- **`VECTORDB_TYPE`**: 벡터DB 타입 (datahub.py 사용 시 간접적으로 영향)
- **`EMBEDDING_PROVIDER`**: 임베딩 공급자 (retrieval 기능 사용 시 필요)

### 주요 특징

1. **병렬 처리**: 8개 워커로 메타데이터 수집 속도 향상
2. **LangChain 통합**: Document 및 Tool 인터페이스 지원
3. **LangGraph ChatBot 연동**: LLM이 자동으로 도구 호출
4. **진행률 표시**: tqdm을 통한 실시간 진행 상황 표시
5. **에러 처리**: 유효한 GMS 서버 확인 및 예외 처리
6. **데이터 정규화**: 조직 특화 데이터를 표준 형식으로 변환

### 통합 흐름

#### 메타데이터 수집 흐름 (벡터DB 초기화 시)

1. `get_table_schema()` 호출
2. `_get_fetcher()`로 DatahubMetadataFetcher 인스턴스 생성
3. `parallel_process()`로 병렬 테이블 정보 수집
4. 각 테이블별로 컬럼 정보 추가 수집
5. LangChain Document 리스트로 변환하여 반환
6. vectordb가 이를 임베딩하여 벡터DB에 저장

#### ChatBot 도구 사용 흐름

1. ChatBot 초기화 시 `@tool` 데코레이터 함수들을 tools 리스트에 추가
2. LLM에 `bind_tools(tools)`로 바인딩
3. 사용자 질문 처리 중 LLM이 필요한 도구 판단
4. ToolNode가 자동으로 해당 함수 호출
5. 결과를 LLM에 전달하여 최종 응답 생성

### 개선 가능 영역

- 추가 메타데이터 타입 지원 (용어집, 관계 등)
- 캐싱 메커니즘으로 성능 향상
- 재시도 로직 강화
- 더 많은 Tool 함수 추가
- 도구 사용 통계 및 모니터링
- 비동기 처리 지원

