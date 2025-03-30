# import sqlalchemy
# import sys
# import os

# # 두 레벨 위 디렉토리까지 경로 추가 (test 디렉토리)
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# # 올바른 경로로 가져오기
# from apis.api_gpt import api_call_gpt   

# from langchain_community.utilities.sql_database import SQLDatabase
# from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit

# # 단순 상대 경로 사용
# db_path = "../db/olympics.db"


# def load_db(db_path):
#     engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
#     return engine


# engine = load_db(db_path)

# db = SQLDatabase(engine=engine)
# llm = api_call_gpt(model_name="gpt-4o-mini")
# toolkit = SQLDatabaseToolkit(llm=llm, db=db)


import sqlalchemy
import sys
import os

# 두 레벨 위 디렉토리까지 경로 추가 (test 디렉토리)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 올바른 경로로 가져오기
from apis.api_gpt import api_call_gpt   

from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit

# 경로 계산 (swarm.py가 test/RAG에서 실행됨)
# 현재 디렉토리 기준 상대 경로 지정
try:
    # 스크립트 위치 기준 상대 경로
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "..", "db", "olympics.db")
    
    # 경로 출력 (디버깅용)
    print(f"DB 경로: {db_path}")
    print(f"파일 존재 여부: {os.path.exists(db_path)}")
except Exception as e:
    print(f"경로 계산 중 오류: {e}")

def load_db(db_path):
    engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
    return engine


engine = load_db(db_path)

db = SQLDatabase(engine=engine)
llm = api_call_gpt(model_name="gpt-4o-mini")
toolkit = SQLDatabaseToolkit(llm=llm, db=db)