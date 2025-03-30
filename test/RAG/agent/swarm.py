import os
import sys
from dotenv import load_dotenv

# LangSmith 트레이싱 완전히 비활성화
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_TRACING"] = "false"
os.environ["LANGCHAIN_SESSION"] = "false"

# 데이터베이스 경로 설정 - 실행 디렉토리 기준으로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(os.path.dirname(current_dir), "db", "olympics.db")

# 디버깅 정보 출력 제거
# print(f"데이터베이스 경로: {db_path}")
# print(f"파일 존재 여부: {os.path.exists(db_path)}")

# 환경 변수로 설정
os.environ["DB_PATH"] = db_path

load_dotenv()

# 두 레벨 위 디렉토리까지 경로 추가 (test 디렉토리)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 올바른 경로로 가져오기
from apis.api_gpt import api_call_gpt
from agent.sql_db_toolkit import toolkit

from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from langgraph_swarm import create_swarm


@tool
def get_db_description():
    """get description about database"""
    return """
    <About-Database>
    The Paris 2024 Olympic Summer Games database provides comprehensive information about the Summer Olympics held in 2024. It covers various aspects of the event, including participating countries, athletes, sports disciplines, medal standings, and key event details. More about the Olympic Games on the official site Olympics Paris 2024 and Wiki.
    </About-Database>

    <Table-inform>
    table_name : description

    athletes : personal information about all athletes
    coaches : personal information about all coaches
    events : all events that had a place
    medals : all medal holders
    medals_total : all medals (grouped by country)
    medalists : all medalists
    nocs : all nocs (code, country, country_long )
    schedule : day-by-day schedule of all events
    schedule_preliminary : preliminary schedule of all events
    teams : all teams
    technical_officials : all technical_officials (referees, judges, jury members)
    </Table-inform>
"""


llm = api_call_gpt(model_name="gpt-4o-mini")

# 전문가 에이전트 생성
agent = create_react_agent(
    llm,
    toolkit.get_tools() + [get_db_description],  # database 정보를 얻기 위한 도구 추가
    prompt="You are a professional Database Administrator. You should answer as data driven and this database only",
    name="db_description",
)

# 에이전트 스웜 생성
workflow = create_swarm(agents=[agent], default_active_agent="db_description")

app = workflow.compile()

try:
    # 결과 출력 - 최종 답변만 출력
    result = app.invoke({"user_input": "What is the total number of medals in the database?"})
    
    # 마지막 AI 메시지 찾기
    if isinstance(result, dict) and "messages" in result:
        messages = result["messages"]
        for msg in reversed(messages):
            if hasattr(msg, "content") and msg.content and msg.content.strip():
                print("\n최종 답변:")
                print(msg.content)
                break
    else:
        print("결과를 찾을 수 없습니다.")
except Exception as e:
    print(f"\n오류 발생: {e}")
