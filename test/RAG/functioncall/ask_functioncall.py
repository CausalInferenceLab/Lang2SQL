import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from pprint import pprint
from functioncall.available_functions import update_available_functions, all_functions
from functioncall.function_to_call import tool_call_function
from functioncall.apis.gpt_api import gpt_4o, gpt_4o_mini, client
from test.RAG.prompt.prompt_setup import SYSTEM_SETUP


# input이 들어왔을때 gpt가 정해진 function에서 해당 input에 맞는 function 작동하는 함수
def ask_gpt_functioncall(query):
    # last_questions = Input_memory.last_questions()
    try:

        messages = [
            {"role": "system", "content": SYSTEM_SETUP},
        ]

        messages.append({"role": "user", "content": query})

        tools = all_functions

        first_response = client.chat.completions.create(
            model=gpt_4o,
            messages=messages,
            temperature=0.0,
            tools=tools,
            tool_choice="auto",  # default: "auto"
        )

        first_response_message = first_response.choices[0].message

        tool_calls = first_response_message.tool_calls

        if tool_calls:
            available_functions = update_available_functions()

            for tool_call in tool_calls:
                tool_call_reponse = tool_call_function(tool_call, available_functions)

            return tool_call_reponse

    except Exception as e:

        print(f"에러메시지: {e}")
        return None