import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from functioncall.function_descriptions.search_functions_description import (
    Users_Search_Function,
)

from RAG.vector_store.video_clipID_processor import video_clip_ids

# from RAG.vector_store.character_video_clipID import character_video_clip_ids
# from vector_store.summary_video_clipID import summary_and_keyword_extract_video_clip_ids
from functools import partial

all_functions = Users_Search_Function


# function이 실행될때 어떤 기능 함수가 실행될지 정하는 함수
def update_available_functions():
    functions = all_functions
    available_functions = {}
    for function in functions:
        function_name = function["function"]["name"]

        if function_name == "character_search":
            available_functions[function_name] = partial(
                video_clip_ids, vector_name="Character", k=25
            )

        elif function_name == "character_action_search":
            available_functions[function_name] = partial(
                video_clip_ids, vector_name="CharacterAction", k=25
            )

    return available_functions
