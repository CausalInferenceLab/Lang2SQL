## DataHub 없이 시작하기 (튜토리얼)

이 문서는 DataHub 없이도 Lang2SQL을 바로 사용하기 위한 최소 절차를 설명합니다. CSV로 테이블/컬럼 설명을 준비해 FAISS 또는 pgvector에 적재한 뒤 Lang2SQL을 실행합니다.

### 0) 준비

```bash
# 소스 클론
git clone https://github.com/CausalInferenceLab/lang2sql.git
cd lang2sql

# (권장) uv 사용
# uv 설치가 되어 있다면 아래 두 줄로 개발 모드 설치
uv venv --python 3.11
source .venv/bin/activate
uv pip install -e .

# (대안) pip 사용
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 1) .env 최소 설정 (OpenAI 기준)

```bash
# LLM/임베딩
LLM_PROVIDER=openai
OPEN_AI_KEY=sk-...                # OpenAI API Key (주의: OPENAI_API_KEY가 아니라 OPEN_AI_KEY)
OPEN_AI_LLM_MODEL=gpt-4o          # 또는 gpt-4.1 등
EMBEDDING_PROVIDER=openai
OPEN_AI_EMBEDDING_MODEL=text-embedding-3-large  # 권장

# VectorDB (선택: 명시하지 않으면 기본값 동작)
VECTORDB_TYPE=faiss
VECTORDB_LOCATION=dev/table_info_db  # FAISS 디렉토리 경로

# (pgvector를 쓰는 경우)
# VECTORDB_TYPE=pgvector
# VECTORDB_LOCATION=postgresql://pgvector:pgvector@localhost:5432/postgres
# PGVECTOR_COLLECTION=table_info_db

# DB 타입
DB_TYPE=clickhouse
```

중요: 코드상 OpenAI 키는 `OPEN_AI_KEY` 환경변수를 사용합니다. `.example.env`의 `OPENAI_API_KEY`는 사용되지 않으니 혼동에 주의하세요.

### 2) 테이블/컬럼 메타데이터 준비 (CSV 예시)
- dev/table_catalog.csv 파일을 생성합니다.
```csv
table_name,table_description,column_name,column_description
customers,고객 정보 테이블,customer_id,고객 고유 ID
customers,고객 정보 테이블,name,고객 이름
customers,고객 정보 테이블,created_at,가입 일시
orders,주문 정보 테이블,order_id,주문 ID
orders,주문 정보 테이블,customer_id,주문 고객 ID
orders,주문 정보 테이블,amount,결제 금액
orders,주문 정보 테이블,status,주문 상태
```

### 3) FAISS 인덱스 생성 (로컬)
- dev/create_faiss.py 파일을 실행합니다.
- `python dev/create_faiss.py`
```python
"""
dev/create_faiss.py

CSV 파일에서 테이블과 컬럼 정보를 불러와 OpenAI 임베딩으로 벡터화한 뒤,
FAISS 인덱스를 생성하고 로컬 디렉토리에 저장한다.

환경 변수:
    OPEN_AI_KEY: OpenAI API 키
    OPEN_AI_EMBEDDING_MODEL: 사용할 임베딩 모델 이름

출력:
    지정된 OUTPUT_DIR 경로에 FAISS 인덱스 저장
"""

import csv
import os
from collections import defaultdict

from dotenv import load_dotenv
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings

load_dotenv()
# CSV 파일 경로
CSV_PATH = "./dev/table_catalog.csv"
# .env의 VECTORDB_LOCATION과 동일하게 맞추세요
OUTPUT_DIR = "./dev/table_info_db"

tables = defaultdict(lambda: {"desc": "", "columns": []})
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        t = row["table_name"].strip()
        tables[t]["desc"] = row["table_description"].strip()
        col = row["column_name"].strip()
        col_desc = row["column_description"].strip()
        tables[t]["columns"].append((col, col_desc))

docs = []
for t, info in tables.items():
    cols = "\n".join([f"{c}: {d}" for c, d in info["columns"]])
    page = f"{t}: {info['desc']}\nColumns:\n {cols}"
    from langchain.schema import Document

    docs.append(Document(page_content=page))

emb = OpenAIEmbeddings(
    model=os.getenv("OPEN_AI_EMBEDDING_MODEL"), openai_api_key=os.getenv("OPEN_AI_KEY")
)
db = FAISS.from_documents(docs, emb)
os.makedirs(OUTPUT_DIR, exist_ok=True)
db.save_local(OUTPUT_DIR)
print(f"FAISS index saved to: {OUTPUT_DIR}")
```

### 4) 실행

```bash
# Streamlit UI
lang2sql --vectordb-type faiss --vectordb-location ./dev/table_info_db run-streamlit

# CLI 예시
lang2sql query "주문 수를 집계하는 SQL을 만들어줘" --vectordb-type faiss --vectordb-location ./dev/table_info_db

# CLI 예시 (pgvector)
lang2sql query "주문 수를 집계하는 SQL을 만들어줘" --vectordb-type pgvector --vectordb-location "postgresql://pgvector:pgvector@localhost:5432/postgres"
```

### 5) (선택) pgvector로 적재하기
- dev/create_pgvector.py 파일을 실행합니다.
- `python dev/create_pgvector.py`

```python
"""
dev/create_pgvector.py

CSV 파일에서 테이블과 컬럼 정보를 불러와 OpenAI 임베딩으로 벡터화한 뒤,
pgvector에 적재한다.

환경 변수:
    OPEN_AI_KEY: OpenAI API 키
    OPEN_AI_EMBEDDING_MODEL: 사용할 임베딩 모델 이름
    VECTORDB_LOCATION: pgvector 연결 문자열
    PGVECTOR_COLLECTION: pgvector 컬렉션 이름
"""

import csv
import os
from collections import defaultdict

from langchain.schema import Document
from langchain_openai import OpenAIEmbeddings
from langchain_postgres.vectorstores import PGVector

# CSV 파일 경로
CSV_PATH = "./dev/table_catalog.csv"
# .env의 VECTORDB_LOCATION과 동일하게 맞추세요
CONN = (
    os.getenv("VECTORDB_LOCATION") or "postgresql://pgvector:pgvector@localhost:5432/postgres"
)
COLLECTION = os.getenv("PGVECTOR_COLLECTION", "table_info_db")

tables = defaultdict(lambda: {"desc": "", "columns": []})
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        t = row["table_name"].strip()
        tables[t]["desc"] = row["table_description"].strip()
        col = row["column_name"].strip()
        col_desc = row["column_description"]
        tables[t]["columns"].append((col, col_desc))

docs = []
for t, info in tables.items():
    cols = "\n".join([f"{c}: {d}" for c, d in info["columns"]])
    docs.append(Document(page_content=f"{t}: {info['desc']}\nColumns:\n {cols}"))

emb = OpenAIEmbeddings(
    model=os.getenv("OPEN_AI_EMBEDDING_MODEL"), openai_api_key=os.getenv("OPEN_AI_KEY")
)
PGVector.from_documents(
    documents=docs, embedding=emb, connection=CONN, collection_name=COLLECTION
)
print(f"pgvector collection populated: {COLLECTION}")
```

주의: FAISS 디렉토리가 없으면 현재 코드는 DataHub에서 메타데이터를 가져와 인덱스를 생성하려고 시도합니다. DataHub를 사용하지 않는 경우 위 절차로 사전에 VectorDB를 만들어 두세요.
