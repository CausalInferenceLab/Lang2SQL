import os
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

from .llm_factory import get_llm

from dotenv import load_dotenv

env_path = os.path.join(os.getcwd(), ".env")

if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    print(f"⚠️  환경변수 파일(.env)이 {os.getcwd()}에 없습니다!")

llm = get_llm(
    model_type="openai",
    model_name="gpt-4o-mini",
    openai_api_key=os.getenv("OPENAI_API_KEY"),
)


def create_query_refiner_chain(llm):
    tool_choice_prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
                당신은 데이터 분석 전문가(데이터 분석가 페르소나)입니다.
                현재 subscription_activities, contract_activities, marketing_activities, 
                sales_activities, success_activities, support_activities, trial_activities 데이터를 
                보유하고 있으며, 사용자의 질문이 모호할 경우에도 우리가 가진 데이터를 기반으로 
                충분히 답변 가능한 형태로 질문을 구체화해 주세요.

                주의:
                - 사용자에게 추가 정보를 요구하는 ‘재질문(추가 질문)’을 하지 마세요.
                - 질문에 포함해야 할 요소(예: 특정 기간, 대상 유저 그룹, 분석 대상 로그 종류 등)가
                  불충분하더라도, 합리적으로 추론해 가정한 뒤
                  답변에 충분한 질문 형태로 완성해 주세요.

                예시:
                사용자가 "유저 이탈 원인이 궁금해요"라고 했다면,
                재질문 형식이 아니라
                "접속·결제 로그를 기준으로,
                주로 어떤 사용자가 어떤 과정을 거쳐 이탈하는지를 분석해야 한다"처럼
                분석 방향이 명확해진 질문 한 문장(또는 한 문단)으로 정리해 주세요.

                최종 출력 형식 예시:
                ------------------------------
                구체화된 질문:
                "고액 결제 경험이 있는 유저가 
                행동 로그에서 이탈 전 어떤 패턴을 보였는지 분석"

                가정한 조건:
                - 행동 로그와 결제 로그 중심
                - 고액 결제자(월 결제액 10만 원 이상) 그룹 대상으로 한정
                ------------------------------
                """,
            ),
            MessagesPlaceholder(variable_name="user_input"),
            (
                "system",
                "다음은 사용자의 실제 사용 가능한 테이블 및 컬럼 정보입니다:",
            ),
            MessagesPlaceholder(variable_name="searched_tables"),
            (
                "system",
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
    query_maker_prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
                당신은 데이터 분석 전문가(데이터 분석가 페르소나)입니다.
                사용자의 질문을 기반으로, 주어진 테이블과 컬럼 정보를 활용하여 적절한 SQL 쿼리를 생성하세요.
                
                주의사항:
                - 사용자의 질문이 다소 모호하더라도, 주어진 데이터를 참고하여 합리적인 가정을 통해 SQL 쿼리를 완성하세요.
                - 불필요한 재질문 없이, 가능한 가장 명확한 분석 쿼리를 만들어 주세요.
                - 최종 출력 형식은 반드시 아래와 같아야 합니다.
                
                최종 형태 예시:
                
                <SQL>
                ```sql
                    SELECT COUNT(DISTINCT user_id) 
                    FROM stg_users 
                ```
                
                <해석>
                ```plaintext (max_length_per_line=100)
                    이 쿼리는 stg_users 테이블에서 고유한 사용자의 수를 계산합니다.
                    사용자는 유니크한 user_id를 가지고 있으며
                    중복을 제거하기 위해 COUNT(DISTINCT user_id)를 사용했습니다.
                ```

                """,
            ),
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
