import os
from langchain_core.prompts import ChatPromptTemplate
from llm_utils.llm_factory import get_llm


def compare_sql_with_llm(generated_sql, ground_truth_sql, user_query):
    """LLM을 사용하여 SQL 평가 (0 ~ 1 점수)"""

    # LLM 초기화
    llm = get_llm(
        model_type="openai",
        model_name="gpt-4o-mini",
        openai_api_key=os.getenv("OPENAI_API_KEY"),
    )

    # 프롬프트 템플릿 생성
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                f"""
            당신은 SQL 전문가입니다. 다음 두 SQL 쿼리의 정확성과 유사성을 비교해주세요.
            유사성을 0(완전히 다름)에서 1(동일함) 사이의 척도로 평가해주세요.
            
            입력 설명 (SQL이 수행해야 할 작업):
            {user_query}
            
            정답 SQL:
            {ground_truth_sql}

            생성된 SQL:
            {generated_sql}

            정답과의 유사성과 생성된 SQL이 입력 설명을 올바르게 처리하는지 모두 고려하세요.
            0과 1 사이의 유사성 점수만 반환하고, 소수점 둘째 자리까지 반올림하세요(예: 0.75, 0.42, 1.00).
            설명이나 추가 텍스트를 포함하지 마세요.
            """,
            )
        ]
    )

    # LLM 체인 실행
    chain = prompt | llm
    response = chain.invoke({})

    try:
        score = float(response.content.strip())
        return max(0, min(score, 1))  # 0~1 사이 값으로 정규화
    except:
        return 0.0  # 오류 발생 시 0점 처리
