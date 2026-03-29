# Lang2SQL

<div align="center">
<a href="https://pseudo-lab.com"><img src="https://img.shields.io/badge/PseudoLab-S10-3776AB" alt="PseudoLab"/></a>
<a href="https://discord.gg/EPurkHVtp2"><img src="https://img.shields.io/badge/Discord-BF40BF" alt="Discord Community"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/stargazers"><img src="https://img.shields.io/github/stars/CausalInferenceLab/lang2sql" alt="Stars Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/network/members"><img src="https://img.shields.io/github/forks/CausalInferenceLab/lang2sql" alt="Forks Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/pulls"><img src="https://img.shields.io/github/issues-pr/CausalInferenceLab/lang2sql" alt="Pull Requests Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/issues"><img src="https://img.shields.io/github/issues/CausalInferenceLab/lang2sql" alt="Issues Badge"/></a>
<a href="https://github.com/CausalInferenceLab/lang2sql/graphs/contributors"><img alt="GitHub contributors" src="https://img.shields.io/github/contributors/CausalInferenceLab/lang2sql?color=2b9348"></a>
<a href="https://pypi.org/project/lang2sql/"><img src="https://img.shields.io/pypi/v/lang2sql" alt="PyPI version"/></a>
<a href="https://pypi.org/project/lang2sql/"><img src="https://img.shields.io/pypi/dm/lang2sql" alt="PyPI downloads"/></a>
<a href="https://hits.sh/github.com/CausalInferenceLab/lang2sql/"><img alt="Hits" src="https://hits.sh/github.com/CausalInferenceLab/lang2sql.svg"/></a>
</div>

<p align="center">
  <strong>우리는 함께 코드와 아이디어를 나누며 더 나은 데이터 환경을 만들기 위한 오픈소스 여정을 떠납니다. 🌍💡</strong>
</p>

<p align="center">
  <em>"모두가 더 가치 있는 일에 집중할 수 있기를 바랍니다."</em>
</p>

---

## 🚀 Lang2SQL이란?

Lang2SQL은 자연어를 SQL로 변환하는 오픈소스 도구입니다. **define-by-run** 철학에 기반한 모듈러 아키텍처로, 복잡한 데이터베이스 스키마에 대한 깊은 지식 없이도 효율적인 SQL 쿼리를 생성할 수 있도록 도와줍니다.

### 🎯 주요 기능

- **🗣️ 자연어를 SQL로 변환**: 일상 언어를 정확한 SQL 쿼리로 변환
- **📊 스마트 테이블 발견**: BM25 키워드 검색과 벡터 유사도 검색을 결합한 하이브리드 검색
- **🔌 Port 기반 확장**: LLM, DB, 임베딩, 벡터스토어를 Protocol 인터페이스로 자유롭게 교체
- **🛠️ 웹 인터페이스**: Streamlit 기반 대화형 앱 (쿼리 생성 + 챗봇)
- **📈 시각화**: SQL 쿼리 결과를 차트와 그래프로 시각화
- **🗄️ 유연한 VectorDB**: InMemory, FAISS(로컬), pgvector(PostgreSQL) 중 선택

### 🤔 해결하는 문제

새로운 데이터팀 구성원들이 자주 직면하는 문제들:
- 🤯 "테이블이 너무 많아! 어디서부터 시작하지?"
- 🧐 "이 JOIN이 맞나요?"
- 🐌 "이 쿼리 성능이 괜찮을까요?"
- 😰 "어떻게 의미있는 인사이트를 추출하지?"

**Lang2SQL은 다음을 제공하여 이를 해결합니다:**
- ✅ 자연어 입력 → 테이블 추천
- ✅ 적절한 컬럼 조합으로 자동 SQL 생성
- ✅ 모범 사례 기반 성능 최적화

---

## 📦 설치 방법

### 빠른 설치

```bash
# pip
pip install lang2sql

# uv (권장)
uv venv --python 3.11
source .venv/bin/activate
uv add lang2sql
```

### 소스에서 설치

```bash
git clone https://github.com/CausalInferenceLab/lang2sql.git
cd lang2sql

# uv (권장)
uv venv --python 3.11
source .venv/bin/activate
uv sync

# pip
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

---

## ⚡ 빠른 시작 (Python API)

```python
from lang2sql import BaselineNL2SQL
from lang2sql.integrations.db import SQLAlchemyDB
from lang2sql.integrations.llm import OpenAILLM

catalog = [
    {
        "name": "orders",
        "description": "고객 주문 정보",
        "columns": {
            "order_id": "주문 고유 ID",
            "customer_id": "고객 ID",
            "order_date": "주문 일시",
            "amount": "주문 금액",
            "status": "주문 상태",
        },
    },
    {
        "name": "customers",
        "description": "고객 마스터",
        "columns": {
            "customer_id": "고객 ID",
            "name": "고객 이름",
            "grade": "고객 등급: bronze / silver / gold",
        },
    },
]

pipeline = BaselineNL2SQL(
    catalog=catalog,
    llm=OpenAILLM(model="gpt-4o-mini"),
    db=SQLAlchemyDB("sqlite:///sample.db"),
    db_dialect="sqlite",
)

rows = pipeline.run("지난달 주문 건수를 알려줘")
print(rows)
```

### 파이프라인 선택 가이드

| 파이프라인 | 검색 방식 | 적합한 상황 |
|---|---|---|
| `BaselineNL2SQL` | BM25 키워드 | 빠른 시작, 카탈로그 규모 소~중간 |
| `HybridNL2SQL` | BM25 + Vector | 검색 품질 우선, 비즈니스 문서 활용 |
| `EnrichedNL2SQL` | BM25 + Vector + Gate | 운영 환경, 부적합 질문 필터링 필요 |

더 자세한 내용은 [튜토리얼](docs/tutorials/01-quickstart.md)을 참고하세요.

---

## 🛠️ 사용법

### CLI

```bash
# Streamlit 웹 인터페이스 실행
lang2sql run-streamlit
lang2sql run-streamlit -p 8888  # 포트 지정

# 자연어 쿼리 실행
lang2sql query "지난달 주문 건수를 집계해줘"

# enriched 플로우 (질문 검증 + 프로파일링 + 컨텍스트 보강)
lang2sql query "이번 달 순매출 합계" --flow enriched --dialect sqlite --top-n 5

# QuestionGate 비활성화 (enriched 전용)
lang2sql query "이번 달 순매출 합계" --flow enriched --no-gate
```

**CLI 옵션:**

| 옵션 | 기본값 | 설명 |
|------|--------|------|
| `--flow` | `baseline` | `baseline` 또는 `enriched` |
| `--dialect` | `None` | SQL 방언 (sqlite, postgresql, mysql, bigquery, duckdb) |
| `--top-n` | `5` | 검색할 최대 테이블 수 |
| `--no-gate` | `False` | QuestionGate 비활성화 (enriched 전용) |

> CLI는 `.env` 파일의 환경변수(`LLM_PROVIDER`, `DB_TYPE` 등)를 읽어 동작합니다. `.env.example`을 참고하세요.

### Streamlit 웹 UI

Streamlit 앱은 멀티 페이지 구조입니다:

- **🏠 홈** — 프로젝트 소개
- **🔍 Lang2SQL** — 자연어 쿼리 → SQL 생성 및 실행
- **🤖 ChatBot** — 멀티턴 대화 기반 SQL 생성
- **⚙️ 설정** — LLM, DB, 임베딩, 데이터 소스 설정

### 처음 시작하기

튜토리얼은 난이도 순서로 구성되어 있습니다.

| 번호 | 문서 | 내용 |
|------|------|------|
| 01 | [빠른 시작](docs/tutorials/01-quickstart.md) | 5분 안에 NL2SQL 실행 |
| 02 | [Baseline 파이프라인](docs/tutorials/02-baseline.md) | 실제 DB 연결, DB Explorer, Factory 함수 |
| 03 | [벡터 검색](docs/tutorials/03-vector-search.md) | FAISS/pgvector 인덱싱 |
| 04 | [하이브리드 검색](docs/tutorials/04-hybrid.md) | BM25 + Vector, EnrichedNL2SQL |
| 05 | [고급](docs/tutorials/05-advanced.md) | 수동 조합, 커스텀 어댑터, Port 레퍼런스, Hook |

---

## 🏗️ 아키텍처

Lang2SQL v2는 **define-by-run** 철학을 따르는 모듈러 아키텍처입니다. 파이프라인 제어는 프레임워크 DSL이 아닌 순수 Python 코드로 작성합니다.

```
src/lang2sql/
├── core/           # 외부 의존성 0% — 절대 third-party import 금지
│   ├── base.py     # BaseComponent, BaseFlow
│   ├── catalog.py  # CatalogEntry, TextDocument, IndexedChunk
│   ├── ports.py    # LLMPort, DBPort, EmbeddingPort 등 Protocol 인터페이스
│   ├── hooks.py    # TraceHook, MemoryHook (관측성)
│   └── exceptions.py
│
├── components/     # 재사용 가능한 부품
│   ├── retrieval/  # KeywordRetriever, VectorRetriever, HybridRetriever
│   ├── generation/ # SQLGenerator (dialect별 프롬프트)
│   ├── execution/  # SQLExecutor
│   ├── gate/       # QuestionGate, TableSuitabilityEvaluator
│   ├── enrichment/ # QuestionProfiler, ContextEnricher
│   └── loaders/    # MarkdownLoader, PlainTextLoader, DirectoryLoader
│
├── flows/          # 프리셋 파이프라인
│   ├── nl2sql.py   # BaselineNL2SQL (Keyword → SQL → Execute)
│   ├── hybrid.py   # HybridNL2SQL (BM25 + Vector)
│   └── enriched_nl2sql.py  # EnrichedNL2SQL (Gate + Profile + Enrich)
│
└── integrations/   # 외부 의존성 구현체
    ├── llm/        # OpenAI, Anthropic, Azure, Gemini, Bedrock, Ollama, HuggingFace
    ├── embedding/  # OpenAI, Azure, Gemini, Bedrock, Ollama, HuggingFace
    ├── db/         # SQLAlchemyDB, SQLAlchemyExplorer
    ├── vectorstore/ # InMemory, FAISS, pgvector
    ├── catalog/    # DataHubCatalogLoader
    ├── loaders/    # PDFLoader
    └── chunking/   # SemanticChunker
```

### 설계 원칙

- **`core/`는 외부 의존성 0%**: 모든 third-party 연동은 `integrations/`에서 구현
- **Port Protocol**: `LLMPort`, `DBPort`, `EmbeddingPort` 등 메서드 시그니처만 맞추면 어떤 구현체든 연결 가능
- **Hook 기반 관측성**: `MemoryHook`으로 컴포넌트별 실행 시간, 에러를 추적

---

## 🧑‍💻 지원 환경

### LLM

| 프로바이더 | 환경변수 `LLM_PROVIDER` |
|---|---|
| OpenAI | `openai` |
| Anthropic | `anthropic` |
| Azure OpenAI | `azure` |
| Google Gemini | `gemini` |
| AWS Bedrock | `bedrock` |
| Ollama | `ollama` |
| HuggingFace | `huggingface` |

### 데이터베이스

SQLite, PostgreSQL, MySQL, MariaDB, DuckDB, ClickHouse, Snowflake, Oracle, BigQuery, Trino, Hive, Crate

### VectorDB

| 백엔드 | 영속성 | 권장 규모 |
|---|---|---|
| `InMemoryVectorStore` | 없음 | < 50k chunks |
| `FAISSVectorStore` | 로컬 파일 | < 500k chunks |
| `PGVectorStore` | PostgreSQL | 500k+ chunks |

---

## 🤝 기여하기

커뮤니티의 기여를 환영합니다!

### 🔧 개발 환경 설정

```bash
# 1. 저장소 포크 & 클론
git clone https://github.com/YOUR_USERNAME/lang2sql.git
cd lang2sql

# 2. 의존성 설치
uv venv --python 3.11
source .venv/bin/activate
uv sync --group dev

# 3. 브랜치 생성 (네이밍: feature/이슈번호-작업내용)
git checkout -b feature/42-amazing-feature

# 4. 작업 후 커밋 & PR
git commit -m "feat: add amazing feature"
git push origin feature/42-amazing-feature
```

### 📋 개발 가이드라인

- `pre-commit run --all-files`로 포맷팅 확인
- 새로운 기능에 대한 테스트 작성 (`tests/test_<layer>_<component>.py`)
- PR은 `master` 브랜치 대상, 최소 2명 리뷰어 승인 필요
- 커밋 메시지: `feat:`, `fix:`, `docs:`, `refactor:` 등 prefix 사용

### 🐛 이슈 신고

버그를 발견했거나 기능 요청이 있으신가요? 다음 정보와 함께 [이슈를 열어주세요](https://github.com/CausalInferenceLab/lang2sql/issues):
- 문제/기능에 대한 명확한 설명
- 재현 단계 (버그의 경우)
- 예상 동작 vs 실제 동작
- 환경 세부사항

---

## 🎓 학습 자료

- [모두를 위한 게임 데이터 검색 시스템 / if(kakaoAI)2024](https://www.youtube.com/watch?v=8-GerpWVMis&ab_channel=kakaotech)
- [AI 데이터 분석가 '물어보새' 등장 – 1부. RAG와 Text-To-SQL 활용](https://techblog.woowahan.com/18144/)
- [테디노트 LangGraph](https://wikidocs.net/233785)
- [DataHub 문서](https://datahubproject.io/)
- [Vanna.ai](https://github.com/vanna-ai/vanna)

---

## 🏆 Our Team

| Role | Name | Skills | Interests |
|------|------|--------|-----------|
| **Project Manager** | 이동욱 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Open Source, Causal Inference |
| **AI Engineer** | 문찬국 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Agentic RAG, Open Source |
| **Data Engineer** | 박경태 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM-driven Data Engineering |
| **AI Engineer** | 손봉균 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, AI Planning |
| **Data Scientist** | 안재일 | ![Python](https://img.shields.io/badge/Python-Intermediate-FF6C37) | LLM, Data Analysis, RAG |
| **ML Engineer** | 이호민 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | Multi-Agent Systems |
| **AI Engineer** | 최세영 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, Multi-Agent |
| **Full-Stack Developer** | 황윤진 | ![NextJs](https://img.shields.io/badge/NextJs-Expert-3776AB) ![React](https://img.shields.io/badge/React-Expert-3776AB) | LLM Orchestration |
| **AI Engineer** | 김경서 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, FinNLP, FDS, RAG |
| **Data Engineer** | 홍지영 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Data Engineering |
| **Data Operator** | 이화림 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, Data Engineering |
| **AI Engineer** | 남경혜 | ![Python](https://img.shields.io/badge/Python-Expert-3776AB) | LLM, RAG, Multi-Agent |
| **Business Analyst** | 서희진 | ![Python](https://img.shields.io/badge/Python-Intermediate-FF6C37) | LLM, Data Analysis |

## 🚀 배포 및 릴리스

### 자동 배포(GitHub Actions)

`v*` 태그 푸시 시 `.github/workflows/pypi-release.yml`이 실행되어 PyPI에 배포됩니다.

```bash
# 1) 버전 업데이트 (version.py)
git add version.py
git commit -m "chore: bump version to X.Y.Z"

# 2) 태그 생성/푸시
git tag vX.Y.Z
git push origin HEAD
git push origin vX.Y.Z
```

사전 준비: GitHub Secrets에 `PYPI_API_TOKEN` 등록

### 수동 빌드

```bash
uv build
UV_PUBLISH_TOKEN=$PYPI_API_TOKEN uv publish --token $UV_PUBLISH_TOKEN
```

---

## 🙏 감사의 말

Lang2SQL은 **가짜연구소의 인과추론팀**에서 개발중인 프로젝트입니다.

---

## 📄 라이선스

- This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).

---

## 🌍 가짜연구소 소개

[가짜연구소](https://pseudo-lab.com/)는 머신러닝과 AI 기술 발전에 중점을 둔 비영리 조직입니다. **공유, 동기부여, 그리고 협업의 기쁨**이라는 핵심 가치를 바탕으로 영향력 있는 오픈소스 프로젝트를 만들어갑니다.

전 세계 5,000명 이상의 연구자들과 함께, 우리는 AI 지식의 민주화와 열린 협업을 통한 혁신 촉진에 전념하고 있습니다.

**우리 커뮤니티에 참여하세요:**
- 💬 [Discord](https://discord.gg/EPurkHVtp2)

---

## 🎯 기여자들

<a href="https://github.com/CausalInferenceLab/lang2sql/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=CausalInferenceLab/lang2sql" />
</a>

---

<div align="center">
  <p><strong>⭐ 이 저장소가 도움이 되셨다면 스타를 눌러주세요!</strong></p>
  <p><em>"우리는 함께 코드와 아이디어를 나누며 더 나은 데이터 환경을 만들기 위한 오픈소스 여정을 떠납니다. 🌍💡"</em></p>
</div>
