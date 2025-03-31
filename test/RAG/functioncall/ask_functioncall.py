import os
import sys
from pprint import pprint
from dotenv import load_dotenv
from openai import OpenAI
load_dotenv()

# 현재 파일의 경로를 기준으로 상위 디렉토리들을 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

# 상대 경로로 import
from available_functions import update_available_functions
from function_descriptions import TABLE_CATEGORY
from tool_call_function import tool_call_function
from prompt.prompt_setup import SYSTEM_SETUP

client = OpenAI()

def ask_gpt_functioncall(query):
    try:
        messages = [
            {"role": "system", "content": SYSTEM_SETUP},
            {"role": "user", "content": query}
        ]


        first_response = client.chat.completions.create(
            model="gpt-4o",  # gpt-4o -> gpt-4로 수정
            messages=messages,
            temperature=0.0,
            tools=TABLE_CATEGORY,
            tool_choice="auto",
        )

        first_response_message = first_response.choices[0].message
        messages.append(first_response_message)  # assistant의 응답을 메시지에 추가

        tool_calls = first_response_message.tool_calls
        print("tool 이름: ", tool_calls)

        if tool_calls:
            available_functions = update_available_functions()

            # 각 tool call에 대한 응답을 처리
            for tool_call in tool_calls:
                tool_call_response = tool_call_function(tool_call, available_functions)
                
                if tool_call_response:
                    # tool 응답을 올바른 형식으로 메시지에 추가
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": tool_call.function.name,
                        "content": tool_call_response,
                    })

            # 두 번째 응답 생성
            second_response = client.chat.completions.create(
                model="gpt-4o",  # gpt-4o -> gpt-4로 수정
                messages=messages,
            )

            return second_response.choices[0].message.content

        return first_response_message.content

    except Exception as e:
        print(f"에러메시지: {e}")
        return None

def main():
    try:
        while True:
            print("궁금한것을 물어보세요")
            user_quote = input("엔터를 눌러 입력하거나 끝내시려면 ctrl + c를 입력하세요: ")
            result = ask_gpt_functioncall(user_quote)
            if result:
                print("\n응답:", result)
                print("\n" + "="*20 + "\n")
    except KeyboardInterrupt:
        print("\n종료중...")

if __name__ == "__main__":
    main()