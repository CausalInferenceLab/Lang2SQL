import sqlalchemy
import sys
import os

# 두 레벨 위 디렉토리까지 경로 추가 (test 디렉토리)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 올바른 경로로 가져오기
from apis.api_gpt import api_call_gpt   

from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit

db_path = "../db/olympics.db"


def load_db(db_path):
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
    return engine


engine = load_db(db_path)

db = SQLDatabase(engine=engine)
llm = api_call_gpt(model_name="gpt-4o-mini")
toolkit = SQLDatabaseToolkit(llm=llm, db=db)
