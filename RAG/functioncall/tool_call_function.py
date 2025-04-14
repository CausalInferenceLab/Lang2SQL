import json


# 해당 function의 어떤 argument를 넘겨줄건지 정하는 함수
def tool_call_function(tool_call, available_functions):
    function_name = tool_call.function.name

    function_to_call = available_functions.get(function_name)

    if not function_to_call:
        return None

    function_args = json.loads(tool_call.function.arguments)

    if function_name == ("text-to-sql"):
        return function_to_call(text=function_args.get("text"))
    
    # elif function_name == ("client_support"):
    #     return function_to_call(text=function_args.get("text"))
    
    

    else:
        # 다른 함수들은 모든 args를 그대로 전달하여 호출합니다.
        return function_to_call(**function_args)
    

