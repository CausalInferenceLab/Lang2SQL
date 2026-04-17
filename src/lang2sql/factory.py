"""환경변수 기반 LLM/Embedding/DB 인스턴스 팩토리.

레거시 utils/llm/core/factory.py를 LangChain 없이 재구현한 것.
CLI와 Streamlit UI 양쪽에서 사용한다.
"""

from __future__ import annotations

import os

from .core.ports import DBExplorerPort, DBPort, EmbeddingPort, LLMPort, ToolCallLLMPort


def build_llm_from_env() -> LLMPort:
    """환경변수 LLM_PROVIDER에 따라 적절한 LLMPort 인스턴스를 생성한다."""
    provider = os.getenv("LLM_PROVIDER", "openai").lower()

    if provider == "openai":
        from .integrations.llm.openai_ import OpenAILLM

        return OpenAILLM(
            model=os.getenv("OPEN_AI_LLM_MODEL", "gpt-4o"),
            api_key=os.getenv("OPEN_AI_KEY"),
        )

    if provider == "anthropic":
        from .integrations.llm.anthropic_ import AnthropicLLM

        return AnthropicLLM(
            model=os.getenv("ANTHROPIC_LLM_MODEL", "claude-sonnet-4-6"),
            api_key=os.getenv("ANTHROPIC_API_KEY"),
        )

    if provider == "azure":
        from .integrations.llm.azure_ import AzureOpenAILLM

        return AzureOpenAILLM(
            azure_deployment=os.environ["AZURE_OPENAI_LLM_MODEL"],
            azure_endpoint=os.environ["AZURE_OPENAI_LLM_ENDPOINT"],
            api_version=os.getenv("AZURE_OPENAI_LLM_API_VERSION", "2023-07-01-preview"),
            api_key=os.getenv("AZURE_OPENAI_LLM_KEY"),
        )

    if provider == "gemini":
        from .integrations.llm.gemini_ import GeminiLLM

        return GeminiLLM(
            model=os.getenv("GEMINI_LLM_MODEL", "gemini-2.0-flash-lite"),
            api_key=os.getenv("GEMINI_API_KEY"),
        )

    if provider == "bedrock":
        from .integrations.llm.bedrock_ import BedrockLLM

        return BedrockLLM(
            model=os.environ["AWS_BEDROCK_LLM_MODEL"],
            aws_access_key_id=os.getenv("AWS_BEDROCK_LLM_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_BEDROCK_LLM_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_BEDROCK_LLM_REGION", "us-east-1"),
        )

    if provider == "ollama":
        from .integrations.llm.ollama_ import OllamaLLM

        return OllamaLLM(
            model=os.environ["OLLAMA_LLM_MODEL"],
            base_url=os.getenv("OLLAMA_LLM_BASE_URL", "http://localhost:11434"),
        )

    if provider == "huggingface":
        from .integrations.llm.huggingface_ import HuggingFaceLLM

        return HuggingFaceLLM(
            repo_id=os.getenv("HUGGING_FACE_LLM_REPO_ID"),
            endpoint_url=os.getenv("HUGGING_FACE_LLM_ENDPOINT"),
            api_token=os.getenv("HUGGING_FACE_LLM_API_TOKEN"),
        )

    raise ValueError(
        f"Unknown LLM_PROVIDER: {provider!r}. "
        "Valid values: openai, anthropic, azure, gemini, bedrock, ollama, huggingface"
    )


def build_tool_call_llm_from_env() -> ToolCallLLMPort:
    """환경변수 LLM_PROVIDER에 따라 적절한 ToolCallLLMPort 인스턴스를 생성한다."""
    provider = os.getenv("LLM_PROVIDER", "openai").lower()

    if provider == "openai":
        from .integrations.llm.openai_ import OpenAIToolCallLLM

        return OpenAIToolCallLLM(
            model=os.getenv("OPEN_AI_LLM_MODEL", "gpt-4o"),
            api_key=os.getenv("OPEN_AI_KEY"),
        )

    if provider == "anthropic":
        from .integrations.llm.anthropic_ import AnthropicToolCallLLM

        return AnthropicToolCallLLM(
            model=os.getenv("ANTHROPIC_LLM_MODEL", "claude-sonnet-4-6"),
            api_key=os.getenv("ANTHROPIC_API_KEY"),
        )

    raise ValueError(
        f"Unsupported LLM_PROVIDER for tool calling: {provider!r}. "
        "Valid values: openai, anthropic"
    )


def build_embedding_from_env() -> EmbeddingPort:
    """환경변수 EMBEDDING_PROVIDER에 따라 EmbeddingPort 인스턴스를 생성한다."""
    provider = os.getenv("EMBEDDING_PROVIDER", "openai").lower().strip("'\"")

    if provider == "openai":
        from .integrations.embedding.openai_ import OpenAIEmbedding

        return OpenAIEmbedding(
            model=os.getenv("OPEN_AI_EMBEDDING_MODEL", "text-embedding-3-small"),
            api_key=os.getenv("OPEN_AI_KEY"),
        )

    if provider == "azure":
        from .integrations.embedding.azure_ import AzureOpenAIEmbedding

        return AzureOpenAIEmbedding(
            azure_deployment=os.environ["AZURE_OPENAI_EMBEDDING_MODEL"],
            azure_endpoint=os.environ["AZURE_OPENAI_EMBEDDING_ENDPOINT"],
            api_version=os.getenv(
                "AZURE_OPENAI_EMBEDDING_API_VERSION", "2023-09-15-preview"
            ),
            api_key=os.getenv("AZURE_OPENAI_EMBEDDING_KEY"),
        )

    if provider == "ollama":
        from .integrations.embedding.ollama_ import OllamaEmbedding

        return OllamaEmbedding(
            model=os.getenv(
                "EMBEDDING_MODEL",
                os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text"),
            ),
            base_url=os.getenv(
                "EMBEDDING_BASE_PATH",
                os.getenv("OLLAMA_EMBEDDING_BASE_URL", "http://localhost:11434"),
            ),
        )

    if provider == "bedrock":
        from .integrations.embedding.bedrock_ import BedrockEmbedding

        return BedrockEmbedding(
            model_id=os.getenv(
                "AWS_BEDROCK_EMBEDDING_MODEL", "amazon.titan-embed-text-v2:0"
            ),
            aws_access_key_id=os.getenv("AWS_BEDROCK_EMBEDDING_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_BEDROCK_EMBEDDING_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_BEDROCK_EMBEDDING_REGION", "us-east-1"),
        )

    if provider == "gemini":
        from .integrations.embedding.gemini_ import GeminiEmbedding

        return GeminiEmbedding(
            model=os.getenv("EMBEDDING_MODEL", "models/embedding-001"),
            api_key=os.getenv("GEMINI_EMBEDDING_API_KEY"),
        )

    if provider == "huggingface":
        from .integrations.embedding.huggingface_ import HuggingFaceEmbedding

        return HuggingFaceEmbedding(
            model=os.getenv(
                "HUGGING_FACE_EMBEDDING_MODEL",
                os.getenv("HUGGING_FACE_EMBEDDING_REPO_ID", ""),
            )
        )

    raise ValueError(
        f"Unknown EMBEDDING_PROVIDER: {provider!r}. "
        "Valid values: openai, azure, ollama, bedrock, gemini, huggingface"
    )


def build_db_from_env(database_env: str = "") -> DBPort:
    """환경변수에서 DB URL을 구성하고 SQLAlchemyDB를 반환한다.

    DB_TYPE 환경변수에 따라 적절한 SQLAlchemy 연결 URL을 구성한다.
    """
    from .integrations.db.sqlalchemy_ import SQLAlchemyDB

    db_type = os.getenv("DB_TYPE", "sqlite").lower()
    url = _build_db_url(db_type)
    return SQLAlchemyDB(url)


def _build_db_url(db_type: str) -> str:
    if db_type == "sqlite":
        path = os.getenv("SQLITE_PATH", "./data/sqlite.db")
        return f"sqlite:///{path}"

    if db_type == "postgresql":
        host = os.getenv("POSTGRESQL_HOST", "localhost")
        port = os.getenv("POSTGRESQL_PORT", "5432")
        user = os.getenv("POSTGRESQL_USER", "postgres")
        password = os.getenv("POSTGRESQL_PASSWORD", "")
        database = os.getenv("POSTGRESQL_DATABASE", "postgres")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    if db_type == "mysql":
        host = os.getenv("MYSQL_HOST", "localhost")
        port = os.getenv("MYSQL_PORT", "3306")
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASSWORD", "")
        database = os.getenv("MYSQL_DATABASE", "")
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    if db_type == "mariadb":
        host = os.getenv("MARIADB_HOST", "localhost")
        port = os.getenv("MARIADB_PORT", "3306")
        user = os.getenv("MARIADB_USER", "root")
        password = os.getenv("MARIADB_PASSWORD", "")
        database = os.getenv("MARIADB_DATABASE", "")
        return f"mariadb+pymysql://{user}:{password}@{host}:{port}/{database}"

    if db_type == "duckdb":
        path = os.getenv("DUCKDB_PATH", "./data/duckdb.db")
        return f"duckdb:///{path}"

    if db_type == "clickhouse":
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = os.getenv("CLICKHOUSE_PORT", "9001")
        user = os.getenv("CLICKHOUSE_USER", "default")
        password = os.getenv("CLICKHOUSE_PASSWORD", "")
        database = os.getenv("CLICKHOUSE_DATABASE", "default")
        return f"clickhouse+native://{user}:{password}@{host}:{port}/{database}"

    if db_type == "snowflake":
        user = os.environ["SNOWFLAKE_USER"]
        password = os.environ["SNOWFLAKE_PASSWORD"]
        account = os.environ["SNOWFLAKE_ACCOUNT"]
        return f"snowflake://{user}:{password}@{account}"

    if db_type == "oracle":
        host = os.getenv("ORACLE_HOST", "localhost")
        port = os.getenv("ORACLE_PORT", "1521")
        user = os.getenv("ORACLE_USER", "")
        password = os.getenv("ORACLE_PASSWORD", "")
        service = os.getenv("ORACLE_SERVICE_NAME", os.getenv("ORACLE_DATABASE", ""))
        return f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}"

    raise ValueError(
        f"Unknown DB_TYPE: {db_type!r}. "
        "Valid values: sqlite, postgresql, mysql, mariadb, duckdb, clickhouse, snowflake, oracle"
    )
