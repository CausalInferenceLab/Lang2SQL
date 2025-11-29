from typing import Any, Dict, List
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.messages import BaseMessage


def filter_relevant_outputs(
    messages: List[BaseMessage],
    table_outputs: List[Dict[str, Any]],
    glossary_outputs: List[Dict[str, Any]],
    query_outputs: List[Dict[str, Any]],
    llm: Any,
) -> Dict[str, Any]:
    """
    LLM을 사용하여 툴 출력 결과 중 사용자 메시지 히스토리와 관련 있는 항목만 필터링합니다.
    """
    if not any([table_outputs, glossary_outputs, query_outputs]):
        return {
            "table_schema_outputs": table_outputs,
            "glossary_outputs": glossary_outputs,
            "query_example_outputs": query_outputs,
        }

    # 메시지 히스토리 포맷팅
    history_text = ""
    for i, msg in enumerate(messages):
        # 마지막 메시지인지 확인
        prefix = "User (Last Message)" if i == len(messages) - 1 else "User"
        history_text += f"{prefix}: {msg.content}\n"

    parser = JsonOutputParser()

    prompt = ChatPromptTemplate.from_template(
        """
        당신은 검색된 데이터베이스 정보 중 사용자의 질문과 **관련성이 낮은 정보**를 선별하는 전문가입니다.
        
        # 대화 히스토리 (User 메시지)
        {history_text}
        
        # 검색된 정보
        1. 테이블 스키마: {table_outputs}
        2. 용어집: {glossary_outputs}
        3. 쿼리 예제: {query_outputs}
        
        # 지침
        - **목표**: 대화의 흐름을 고려하여, 답변에 도움이 될 수 있는 정보는 유지하고 **명확하게 관련 없는 정보**만 제거하세요.
        - **기준**: 
            - 사용자의 질문과 직접적인 관련이 없더라도, 문맥상 유용한 정보라면 유지하세요.
            - 대명사(그거, 저거 등)가 가리키는 대상이나, 질문의 의도를 파악하는 데 필요한 정보는 반드시 유지해야 합니다.
            - 정말로 엉뚱하거나 불필요한 정보라고 확신할 때만 제거하세요.
        - **형식**: 데이터 구조를 변경하지 말고, 리스트 내부의 불필요한 항목만 제거하세요.
        - **결과**: 관련된 정보가 없다면 빈 리스트 `[]`를 반환하세요.
        - 반드시 아래 JSON 형식으로만 응답하세요.
        
        {{
            "table_schema_outputs": [...],
            "glossary_outputs": [...],
            "query_example_outputs": [...]
        }}
        """
    )

    chain = prompt | llm | parser

    try:
        result = chain.invoke(
            {
                "history_text": history_text,
                "table_outputs": str(table_outputs),
                "glossary_outputs": str(glossary_outputs),
                "query_outputs": str(query_outputs),
            }
        )
        return result
    except Exception as e:
        # 에러 발생 시 원본 데이터 반환 (안전장치)
        print(f"Filtering failed: {e}")
        return {
            "table_schema_outputs": table_outputs,
            "glossary_outputs": glossary_outputs,
            "query_example_outputs": query_outputs,
        }
