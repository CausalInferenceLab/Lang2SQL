import os

from langchain.chains.sql_database.prompt import SQL_PROMPTS
from langchain_core.prompts import load_prompt


class SQLPrompt:
    def __init__(self):
        # os library를 확인해서 SQL_PROMPTS key에 해당하는 prompt가 있으면, 이를 교체
        self.sql_prompts = SQL_PROMPTS
        self.target_db_list = list(SQL_PROMPTS.keys())
        self.prompt_path = "../prompt"

    def update_prompt_from_path(self):
        if os.path.exists(self.prompt_path):
            path_list = os.listdir(self.prompt_path)
            # yaml 파일만 가져옴
            file_list = [file for file in path_list if file.endswith(".yaml")]
            key_path_dict = {
                key.split(".")[0]: os.path.join(self.prompt_path, key)
                for key in file_list
                if key.split(".")[0] in self.target_db_list
            }
            # file_list에서 sql_prompts의 key에 해당하는 파일이 있는 것만 가져옴
            for key, path in key_path_dict.items():
                self.sql_prompts[key] = load_prompt(path, encoding="utf-8")
        else:
            raise FileNotFoundError(f"Prompt file not found in {self.prompt_path}")
        return False
