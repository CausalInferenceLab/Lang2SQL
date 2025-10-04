"""config 패키지의 공개 API를 재노출하여 기존 import 호환성을 유지합니다.
모델, 경로/지속성, 레지스트리, 설정 업데이트 유틸을 한 곳에서 제공합니다.
"""

from .models import (
    Config,
    DataHubSource,
    VectorDBSource,
    DataSourcesRegistry,
    DBConnectionProfile,
    DBConnectionsRegistry,
    LLMProfile,
    LLMRegistry,
    EmbeddingProfile,
    EmbeddingRegistry,
)

from .settings import (
    DEFAULT_DATAHUB_SERVER,
    DEFAULT_VECTORDB_TYPE,
    DEFAULT_VECTORDB_LOCATION,
    load_config,
    _get_session_value,
    update_datahub_server,
    update_data_source_mode,
    update_vectordb_settings,
    update_llm_settings,
    update_embedding_settings,
    update_db_settings,
    _put_env,
    _put_session,
)

from .registry_data_sources import (
    get_data_sources_registry,
    _save_registry,
    add_datahub_source,
    update_datahub_source,
    delete_datahub_source,
    add_vectordb_source,
    update_vectordb_source,
    delete_vectordb_source,
)

from .registry_db import (
    get_db_connections_registry,
    _save_db_registry,
    add_db_connection,
    update_db_connection,
    delete_db_connection,
)

from .registry_llm import (
    get_llm_registry,
    save_llm_profile,
    _save_llm_registry,
    get_embedding_registry,
    save_embedding_profile,
    _save_embedding_registry,
)

from .paths import (
    _get_registry_file_path,
    _get_db_registry_file_path,
    _get_llm_registry_file_path,
    _get_embedding_registry_file_path,
    _ensure_parent_dir,
)

from .persist import (
    save_registry_to_disk,
    save_db_registry_to_disk,
    save_llm_registry_to_disk,
    save_embedding_registry_to_disk,
    load_registry_from_disk,
    load_db_registry_from_disk,
    load_llm_registry_from_disk,
    load_embedding_registry_from_disk,
)

__all__ = [
    # Models
    "Config",
    "DataHubSource",
    "VectorDBSource",
    "DataSourcesRegistry",
    "DBConnectionProfile",
    "DBConnectionsRegistry",
    "LLMProfile",
    "LLMRegistry",
    "EmbeddingProfile",
    "EmbeddingRegistry",
    # Defaults
    "DEFAULT_DATAHUB_SERVER",
    "DEFAULT_VECTORDB_TYPE",
    "DEFAULT_VECTORDB_LOCATION",
    # Settings APIs
    "load_config",
    "_get_session_value",
    "update_datahub_server",
    "update_data_source_mode",
    "update_vectordb_settings",
    "update_llm_settings",
    "update_embedding_settings",
    "update_db_settings",
    "_put_env",
    "_put_session",
    # Registries - data sources
    "get_data_sources_registry",
    "_save_registry",
    "add_datahub_source",
    "update_datahub_source",
    "delete_datahub_source",
    "add_vectordb_source",
    "update_vectordb_source",
    "delete_vectordb_source",
    # Registries - db connections
    "get_db_connections_registry",
    "_save_db_registry",
    "add_db_connection",
    "update_db_connection",
    "delete_db_connection",
    # Registries - llm/embedding
    "get_llm_registry",
    "save_llm_profile",
    "_save_llm_registry",
    "get_embedding_registry",
    "save_embedding_profile",
    "_save_embedding_registry",
    # Persistence helpers and paths (for backward compatibility)
    "_get_registry_file_path",
    "_get_db_registry_file_path",
    "_get_llm_registry_file_path",
    "_get_embedding_registry_file_path",
    "_ensure_parent_dir",
    "save_registry_to_disk",
    "save_db_registry_to_disk",
    "save_llm_registry_to_disk",
    "save_embedding_registry_to_disk",
    "load_registry_from_disk",
    "load_db_registry_from_disk",
    "load_llm_registry_from_disk",
    "load_embedding_registry_from_disk",
]
