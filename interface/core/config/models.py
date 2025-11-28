"""설정 및 각 레지스트리에서 사용하는 데이터 모델(dataclass) 정의 모듈입니다."""

from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict


@dataclass
class Config:
    datahub_server: str
    vectordb_type: str
    vectordb_location: str
    data_source_mode: str | None = None  # "datahub" | "vectordb" | None


@dataclass
class DataHubSource:
    name: str
    url: str
    faiss_path: Optional[str] = None
    vectordb_type: str = "faiss"
    vectordb_location: Optional[str] = None
    vectordb_api_key: Optional[str] = None
    note: Optional[str] = None


@dataclass
class VectorDBSource:
    name: str
    type: str  # 'faiss' | 'pgvector' | 'qdrant'
    location: str
    api_key: Optional[str] = None
    collection_prefix: Optional[str] = None
    note: Optional[str] = None


@dataclass
class DataSourcesRegistry:
    datahub: List[DataHubSource] = field(default_factory=list)
    vectordb: List[VectorDBSource] = field(default_factory=list)


@dataclass
class DBConnectionProfile:
    name: str
    type: str  # 'postgresql' | 'mysql' | 'mariadb' | 'oracle' | 'clickhouse' | 'duckdb' | 'sqlite' | 'databricks' | 'snowflake' | 'trino'
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None  # non-secret
    note: Optional[str] = None


@dataclass
class DBConnectionsRegistry:
    connections: List[DBConnectionProfile] = field(default_factory=list)


@dataclass
class LLMProfile:
    name: str
    provider: (
        str  # 'openai' | 'azure' | 'bedrock' | 'gemini' | 'ollama' | 'huggingface'
    )
    fields: Dict[str, str] = field(default_factory=dict)  # includes secrets
    note: Optional[str] = None


@dataclass
class LLMRegistry:
    profiles: List[LLMProfile] = field(default_factory=list)


@dataclass
class EmbeddingProfile:
    name: str
    provider: (
        str  # 'openai' | 'azure' | 'bedrock' | 'gemini' | 'ollama' | 'huggingface'
    )
    fields: Dict[str, str] = field(default_factory=dict)
    note: Optional[str] = None


@dataclass
class EmbeddingRegistry:
    profiles: List[EmbeddingProfile] = field(default_factory=list)
