## llm_utils 개요

Lang2SQL 파이프라인에서 LLM, 검색(RAG), 그래프 워크플로우, DB 실행, 시각화 등 보조 유틸리티를 모아둔 패키지입니다. 이 문서는 depth(계층)별로 기능과 통합 흐름을 정리합니다.

### Depth 0: 최상위 유틸리티

- (Moved) `engine/query_executor.py`: Lang2SQL 그래프 선택/컴파일/실행 진입점.
- (Moved) `viz/display_chart.py`: LLM 활용 Plotly 시각화 유틸.
- (Moved) `infra/monitoring/check_server.py`: GMS 헬스체크.
- (Moved) `infra/db/connect_db.py`: ClickHouse 연결/실행.
- (Moved) `infra/observability/token_usage.py`: LLM 메시지의 `usage_metadata` 합산 토큰 집계.
- **`llm_response_parser.py`**: LLM 응답에서 `<SQL>`, `<해석>` 블록 추출.
- **`prompts_class.py`**: LangChain SQL 프롬프트를 로컬 YAML로 오버라이드.
- (Moved) `graph_utils/profile_utils.py`: `profile_to_text(profile)` 등 그래프 관련 포맷 유틸.

### Depth 1: LLM/임베딩/검색

- **`llm_factory.py`**: 환경변수로 LLM/임베딩 공급자 선택 팩토리.
  - LLM: `openai`, `azure`, `bedrock`, `gemini`, `ollama`, `huggingface`
  - Embeddings: `openai`, `azure`, `bedrock`, `gemini`, `ollama`, `huggingface`
  - 사용처: 체인/그래프 전반 및 `vectordb` 인덱싱/검색.
- **`retrieval.py`**: 테이블 메타 검색 및 재순위화.
  - `search_tables(query, retriever_name, top_n, device)`
  - 기본: FAISS/pgvector에서 similarity_search.
  - `Reranker`: ko-reranker(CrossEncoder)로 재순위.

### Depth 1.5: 벡터DB

- **`vectordb/factory.py` → `get_vector_db()`**: `VECTORDB_TYPE`(`faiss`|`pgvector`)에 따라 인스턴스 반환.
- **`vectordb/faiss_db.py`**: 로컬 디스크 `dev/table_info_db` 로드/없으면 `tools.get_info_from_db()`로 빌드 후 저장.
- **`vectordb/pgvector_db.py`**: PGVector 컬렉션 연결, 없거나 비면 `from_documents`로 재구성.

### Depth 2: 데이터 소스/메타 수집

- **`tools.py`**: DataHub 기반 메타데이터 수집.
  - `set_gms_server(gms_server)`로 GMS 설정.
  - `get_info_from_db()` → `langchain.schema.Document` 리스트: 테이블 설명, 컬럼, 예시 쿼리, 용어집을 포맷.
  - `get_metadata_from_db()` → 풍부한 전체 메타데이터(dict) 목록.

### Depth 2.5: 체인(Chains)

- **`chains.py`**: LangChain ChatPromptTemplate로 구성된 체인.
  - `create_query_maker_chain`, `create_profile_extraction_chain`, `create_query_enrichment_chain`
  - `QuestionProfile` Pydantic 모델로 질의 특성 구조화 추출.

### Depth 3: 그래프(Graph) 워크플로우

- **`graph_utils/base.py`**: 공통 상태(`QueryMakerState`)와 노드 함수 집합.
  - 노드: `get_table_info_node`(RAG), `profile_extraction_node`, `context_enrichment_node`, `query_maker_node`, `query_maker_node_without_refiner`.
  - 각 노드는 `chains.py`와 `retrieval.py`, `utils.profile_to_text` 등을 호출하며 상태를 갱신.
- **`graph_utils/basic_graph.py`**: GET_TABLE_INFO → QUERY_MAKER → END
- **`graph_utils/enriched_graph.py`**: GET_TABLE_INFO → PROFILE_EXTRACTION → CONTEXT_ENRICHMENT → QUERY_MAKER → END
- **`graph_utils/simplified_graph.py`**: GET_TABLE_INFO → PROFILE_EXTRACTION → CONTEXT_ENRICHMENT → QUERY_MAKER(without refiner) → END

### 통합 흐름(End-to-End)

1) 사용자가 자연어 질문 입력 → `query_executor.execute_query()` 호출
2) 그래프 선택(`basic`/`enriched`/`simplified`) 및 컴파일
3) `GET_TABLE_INFO`에서 `retrieval.search_tables()`로 관련 테이블/컬럼/예시쿼리/용어집 수집
4) `PROFILE_EXTRACTION`(선택)에서 `chains.profile_extraction_chain`으로 질문 특성 추출 → `utils.profile_to_text`
5) `QUERY_REFINER`(선택) 또는 `CONTEXT_ENRICHMENT`로 질문을 정교화/보강
6) `QUERY_MAKER`에서 DB 가이드/메타를 바탕으로 SQL 생성 (`<SQL>` 코드블록 포함 권장)
7) 반환 `messages`에서 `llm_response_parser.extract_sql()`로 SQL 추출
8) 필요 시 `connect_db.run_sql()`로 실행, `display_chart`로 결과 시각화

### 환경 변수 요약

- **LLM 관련**: `LLM_PROVIDER`, `OPEN_AI_KEY`, `OPEN_AI_LLM_MODEL`, `AZURE_*`, `AWS_BEDROCK_*`, `GEMINI_*`, `OLLAMA_*`, `HUGGING_FACE_*`
- **임베딩 관련**: `EMBEDDING_PROVIDER`, 각 공급자별 키/모델
- **VectorDB**: `VECTORDB_TYPE`(faiss|pgvector), `VECTORDB_LOCATION`, `PGVECTOR_*`
- **DataHub**: `DATAHUB_SERVER`
- **ClickHouse**: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`

### 핵심 사용 예시

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

```python
from viz.display_chart import DisplayChart

chart = DisplayChart(question="지난달 매출 추이", sql=sql, df_metadata=str(df.dtypes))
code = chart.generate_plotly_code()
fig = chart.get_plotly_figure(code, df)
```

### 파일간 의존 관계(요약)

- `query_executor.py` → `graph_utils/*` → `chains.py`, `retrieval.py`, `llm_factory.py`, `utils.py`
- `retrieval.py` → `vectordb/*` → `llm_factory.get_embeddings()`, `tools.get_info_from_db()`
- `display_chart.py` → OpenAI LLM(선택적)로 코드 생성 → Plotly 실행
- `connect_db.py` → ClickHouse 클라이언트로 SQL 실행
- `llm_response_parser.py` → 결과 파서


