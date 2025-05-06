from typing_extensions import TypedDict, Annotated
from langgraph.graph.message import add_messages


# 상태 타입 정의 (추가 상태 정보와 메시지들을 포함)
class QueryMakerState(TypedDict):
    messages: Annotated[list, add_messages]
    user_database_env: str
    searched_tables: dict[str, dict[str, str]]
    best_practice_query: str
    refined_input: str
    generated_query: str
    retriever_name: str
    top_n: int
    device: str