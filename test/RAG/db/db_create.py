import os
import pandas as pd

from sqlalchemy import create_engine
from glob import glob


def create_sqlite_engine(db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    return engine


def create_sqlite_db(csv_files, engine):
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        file_name = os.path.basename(csv_file)  # 파일 이름만 추출
        table_name = os.path.splitext(file_name)[0]  # 확장자 제거
        df.to_sql(table_name, engine, if_exists="replace", index=False)


csv_files = glob("../db/data/*.csv")
db_path = "../db/olympics.db"
engine = create_sqlite_engine(db_path)
create_sqlite_db(csv_files, engine)
