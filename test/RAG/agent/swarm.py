import os
import sys

# 두 레벨 위 디렉토리까지 경로 추가 (test 디렉토리)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 올바른 경로로 가져오기
from apis.api_gpt import api_call_gpt
from agent.sql_db_toolkit import toolkit

from langchain_core.tools import tool
from langgraph_prebuilt import create_react_agent
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

# StateGraph 객체는 invoke가 아닌 run 메서드를 사용합니다
# workflow.invoke({"user_input": "What is the total number of medals in the database?"})
workflow.run({"user_input": "What is the total number of medals in the database?"})
