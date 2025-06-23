from typing import Optional, Dict, TypedDict


class DBConfig(TypedDict):

    host: str
    port: Optional[int]
    user: Optional[str]
    password: Optional[str]
    database: Optional[str]
    extra: Optional[Dict[str, str]]
