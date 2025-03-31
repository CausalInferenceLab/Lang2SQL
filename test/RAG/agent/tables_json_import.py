# categorized_tables.py
import os
import json

CURRENT_FILE_PATH = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE_PATH))))
TABLES_DIR = os.path.join(PROJECT_ROOT, "test", "RAG", "categorized_tables")  # 실제 파일 위치로 수정

def load_json_table(file_name):
    file_path = os.path.join(TABLES_DIR, file_name)
    # print(f"불러오려는 파일 경로: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_name}: {e}")
        return {}



# print(load_json_table("client_subscription.json"))