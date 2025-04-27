import os

from dotenv import load_dotenv
from langchain_core.prompts import (
    ChatPromptTemplate,
    MessagesPlaceholder,
    SystemMessagePromptTemplate,
)

from prompt.template_loader import get_prompt_template

from .llm_factory import get_llm

env_path = os.path.join(os.getcwd(), ".env")

if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    print(f"⚠️  환경변수 파일(.env)이 {os.getcwd()}에 없습니다!")

llm = get_llm()


def create_query_refiner_chain(llm):
    prompt = get_prompt_template("query_refiner_prompt")
    tool_choice_prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessagePromptTemplate.from_template(prompt),
            MessagesPlaceholder(variable_name="user_input"),
            SystemMessagePromptTemplate.from_template(
                """
                위 사용자의 입력을 바탕으로
                분석 관점에서 **충분히 답변 가능한 형태**로
                "구체화된 질문"을 작성하고,
                필요한 경우 가정이나 전제 조건을 함께 제시해 주세요.
                """,
            ),
        ]
    )

    return tool_choice_prompt | llm


# QueryMakerChain
def create_query_maker_chain(llm):
    # SystemPrompt만 yaml 파일로 불러와서 사용
    prompt = get_prompt_template("query_maker_prompt")
    query_maker_prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessagePromptTemplate.from_template(prompt),
            (
                "system",
                "아래는 사용자의 질문 및 구체화된 질문입니다:",
            ),
            MessagesPlaceholder(variable_name="user_input"),
            MessagesPlaceholder(variable_name="refined_input"),
            (
                "system",
                "다음은 사용자의 db 환경정보와 사용 가능한 테이블 및 컬럼 정보입니다:",
            ),
            MessagesPlaceholder(variable_name="user_database_env"),
            MessagesPlaceholder(variable_name="searched_tables"),
            (
                "system",
                "위 정보를 바탕으로 사용자 질문에 대한 최적의 SQL 쿼리를 최종 형태 예시와 같은 형태로 생성하세요.",
            ),
        ]
    )
    return query_maker_prompt | llm


query_refiner_chain = create_query_refiner_chain(llm)
query_maker_chain = create_query_maker_chain(llm)

if __name__ == "__main__":
    query_refiner_chain.invoke()
