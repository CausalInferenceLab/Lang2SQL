## utils.llm.vectordb 개요

Lang2SQL 파이프라인에서 테이블 메타데이터를 벡터화하여 저장하고 검색하기 위한 벡터 데이터베이스 모듈입니다. FAISS와 pgvector 두 가지 백엔드를 지원하며, 환경변수를 통해 선택이 가능합니다.

### 파일 구조

```
utils/llm/vectordb/
├── __init__.py          # 모듈 진입점, get_vector_db 함수 export
├── factory.py           # VectorDB 팩토리 - 타입에 따라 적절한 인스턴스 생성
├── faiss_db.py          # FAISS 벡터 데이터베이스 구현
└── pgvector_db.py       # pgvector (PostgreSQL) 벡터 데이터베이스 구현
```

### 각 파일 상세 설명

#### __init__.py

**목적**: 벡터DB 모듈의 공개 인터페이스 정의

**Export 함수**:
- `get_vector_db`: 환경변수 기반으로 적절한 벡터DB 인스턴스 반환

#### factory.py

**목적**: VectorDB 타입과 위치에 따라 적절한 VectorDB 인스턴스를 생성하는 팩토리

**주요 함수**:

1. **`get_vector_db(vectordb_type=None, vectordb_location=None)`**
   - 환경변수 또는 파라미터로 VectorDB 타입과 위치를 받아 적절한 인스턴스 반환
   - `vectordb_type`: "faiss" 또는 "pgvector" (기본: 환경변수 `VECTORDB_TYPE`, fallback: "faiss")
   - `vectordb_location`: 
     - FAISS: 디렉토리 경로
     - pgvector: PostgreSQL 연결 문자열
     - 기본: 환경변수 `VECTORDB_LOCATION`
   - 반환: FAISS 또는 PGVector 인스턴스
   - 에러: 지원하지 않는 타입 시 ValueError 발생

**의존성**:
- `utils.llm.vectordb.faiss_db.get_faiss_vector_db`: FAISS 인스턴스 생성
- `utils.llm.vectordb.pgvector_db.get_pgvector_db`: PGVector 인스턴스 생성

#### faiss_db.py

**목적**: FAISS 벡터 데이터베이스 구현 (로컬 디스크 기반)

**주요 함수**:

1. **`get_faiss_vector_db(vectordb_path=None)`**
   - FAISS 벡터 데이터베이스를 로드하거나 새로 생성
   - `vectordb_path`: 저장 경로 (기본: `dev/table_info_db`)
   - 동작 방식:
     - 기존 DB가 있으면 `FAISS.load_local()`로 로드
     - 없으면 `get_table_schema()`로 문서 수집 후 `FAISS.from_documents()` 생성 및 저장
   - 반환: FAISS 벡터스토어 인스턴스

**의존성**:
- `langchain_community.vectorstores.FAISS`: LangChain FAISS 래퍼
- `utils.llm.core.get_embeddings`: 임베딩 모델 로드
- `utils.llm.tools.get_table_schema`: DataHub에서 테이블 메타데이터 수집

**특징**:
- 로컬 디스크에 저장되어 네트워크 연결 불필요
- 빠른 검색 성능
- 싱글 머신 환경에 최적화

#### pgvector_db.py

**목적**: pgvector를 활용한 PostgreSQL 벡터 데이터베이스 구현

**주요 함수**:

1. **`get_pgvector_db(connection_string=None, collection_name=None)`**
   - pgvector 벡터 데이터베이스를 로드하거나 새로 생성
   - `connection_string`: PostgreSQL 연결 문자열 (기본: 환경변수 조합)
   - `collection_name`: 컬렉션 이름 (기본: `lang2sql_table_info_db`)
   - 환경변수 (기본값):
     - `PGVECTOR_HOST`: "localhost"
     - `PGVECTOR_PORT`: "5432"
     - `PGVECTOR_USER`: "postgres"
     - `PGVECTOR_PASSWORD`: "postgres"
     - `PGVECTOR_DATABASE`: "postgres"
     - `PGVECTOR_COLLECTION`: "lang2sql_table_info_db"
   - 동작 방식:
     - 기존 컬렉션이 있고 비어있지 않으면 로드
     - 없거나 비어있으면 `get_table_schema()`로 문서 수집 후 `PGVector.from_documents()` 생성
   - 반환: PGVector 벡터스토어 인스턴스

2. **`_check_collection_exists(connection_string, collection_name)`**
   - PostgreSQL에서 컬렉션 존재 여부 확인
   - `langchain_pg_embedding` 테이블에서 collection_name 조회
   - 반환: bool (존재 여부)

**의존성**:
- `langchain_postgres.vectorstores.PGVector`: LangChain pgvector 래퍼
- `psycopg2`: PostgreSQL 연결
- `utils.llm.core.get_embeddings`: 임베딩 모델 로드
- `utils.llm.tools.get_table_schema`: DataHub에서 테이블 메타데이터 수집

**특징**:
- PostgreSQL 데이터베이스에 저장되어 다중 서버 환경에 적합
- ACID 트랜잭션 지원
- 확장 가능한 인프라
- 네트워크 연결 필요

### 사용 방법

#### 1. 기본 사용법 (retrieval.py에서 실제 사용)

```python
from utils.llm.vectordb import get_vector_db

# 환경변수 기반으로 적절한 벡터DB 로드
db = get_vector_db()

# 유사도 검색
documents = db.similarity_search("고객 테이블", k=5)

# Retriever 인터페이스 사용
retriever = db.as_retriever(search_kwargs={"k": 5})
results = retriever.invoke("매출 관련 테이블")
```

#### 2. FAISS 명시적 사용

```python
from utils.llm.vectordb.factory import get_vector_db

# FAISS 타입 지정
db = get_vector_db(vectordb_type="faiss", vectordb_location="./my_faiss_db")

# 검색
results = db.similarity_search("사용자 정보", k=10)
```

#### 3. pgvector 명시적 사용

```python
from utils.llm.vectordb.factory import get_vector_db

# pgvector 타입 지정
connection_string = "postgresql://user:password@localhost:5432/mydb"
db = get_vector_db(vectordb_type="pgvector", vectordb_location=connection_string)

# 검색
results = db.similarity_search("주문 테이블", k=5)
```

#### 4. 통합 흐름 (Lang2SQL 파이프라인 내)

`utils/llm/retrieval.py`의 `search_tables()` 함수에서 사용:

1. `get_vector_db()`로 벡터DB 로드 (환경변수 기반)
2. `similarity_search()` 또는 `retriever.invoke()`로 유사도 기반 검색
3. 결과를 테이블/컬럼 정보 딕셔너리로 파싱 및 반환

**경로**: `utils/llm/retrieval.py` (60-104번째 줄)

#### 5. CLI 환경변수 설정

```bash
# FAISS 사용
export VECTORDB_TYPE=faiss
export VECTORDB_LOCATION=./dev/table_info_db  # 선택사항

# pgvector 사용
export VECTORDB_TYPE=pgvector
export PGVECTOR_HOST=localhost
export PGVECTOR_PORT=5432
export PGVECTOR_USER=postgres
export PGVECTOR_PASSWORD=postgres
export PGVECTOR_DATABASE=postgres
export PGVECTOR_COLLECTION=lang2sql_table_info_db
```

### import 관계

**import하는 파일**:
- `utils/llm/retrieval.py`: `from utils.llm.vectordb import get_vector_db`

**내부 의존성**:
- `utils/llm/core/factory.py`: `get_embeddings()` - 임베딩 모델 로드
- `utils/llm/tools/datahub.py`: `get_table_schema()` - DataHub 메타데이터 수집

**외부 의존성**:
- `langchain_community.vectorstores.FAISS`: FAISS 벡터스토어
- `langchain_postgres.vectorstores.PGVector`: pgvector 벡터스토어
- `psycopg2`: PostgreSQL 연결 (pgvector 전용)

### 환경 변수 요약

#### VectorDB 타입 선택
- **`VECTORDB_TYPE`**: "faiss" 또는 "pgvector" (기본: "faiss")

#### FAISS 환경변수
- **`VECTORDB_LOCATION`**: FAISS 저장 디렉토리 경로 (기본: `./dev/table_info_db`)

#### pgvector 환경변수
- **`PGVECTOR_HOST`**: PostgreSQL 호스트 (기본: "localhost")
- **`PGVECTOR_PORT`**: PostgreSQL 포트 (기본: "5432")
- **`PGVECTOR_USER`**: PostgreSQL 사용자 (기본: "postgres")
- **`PGVECTOR_PASSWORD`**: PostgreSQL 비밀번호 (기본: "postgres")
- **`PGVECTOR_DATABASE`**: PostgreSQL 데이터베이스 (기본: "postgres")
- **`PGVECTOR_COLLECTION`**: 컬렉션 이름 (기본: "lang2sql_table_info_db")
- **`EMBEDDING_PROVIDER`**: 임베딩 모델 공급자 (필수, 모든 타입 공통)

### 주요 특징

1. **이중 백엔드 지원**: FAISS(로컬) 및 pgvector(PostgreSQL) 자유 선택
2. **자동 초기화**: 벡터DB가 없으면 DataHub에서 자동으로 생성
3. **환경변수 기반 설정**: 코드 수정 없이 실행 시점에 선택 가능
4. **LangChain 통합**: 표준 VectorStore 인터페이스 제공
5. **유사도 검색**: 테이블 메타데이터에 대한 의미 기반 검색

### 개선 가능 영역

- 다른 벡터DB 지원 (Qdrant 등)
- 증분 인덱싱 지원
- 벡터DB 버전 관리
- 성능 최적화 (인덱스 튜닝)
- 모니터링 및 로깅 강화

