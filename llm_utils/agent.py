from langchain_core.messages import AIMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.messages import SystemMessage
from .state import QueryMakerState
from .llm_factory import get_llm
from prompt.template_loader import get_prompt_template

llm = get_llm()

# JSON 스키마 정의
main_agent_schema = {
    "type": "object",
    "properties": {
        "intent": {
            "type": "string",
            "description": "유저의 의도 (search, end 등)"
        },
        "user_input": {
            "type": "string",
            "description": "유저의 입력"
        },
        "intent_reason": {
            "type": "string",
            "description": "유저의 의도 파악 이유"
        },
    },
    "required": ["intent", "user_input"]
}
main_agent_parser = JsonOutputParser(schema=main_agent_schema)

def manager_agent(state: QueryMakerState) -> dict:
    """
    가장 처음 시작하는 agent로 질문의 유무를 판단해서 적절한 Agent를 호출합니다.
    추후, 가드레일 등에 detecting될 경우에도 해당 노드를 통해 대응이 가능합니다
    """
    manager_agent_prompt = get_prompt_template("manager_agent_prompt")
    messages = [SystemMessage(content=manager_agent_prompt), state["messages"][-1]]
    response = llm.invoke(messages)
    
    try:
        parsed_output = main_agent_parser.parse(response.content)
        state.update({
            "messages": state["messages"] + [response], # 기록용
            "intent": parsed_output.get("intent", "end"), # 분기용
            "user_input": parsed_output.get("user_input",state['messages'][-1].content), # SQL 쿼리 변환 대상 질문
            "intent_reason": parsed_output.get("intent_reason", "") # 분기 이유
        })
        return state
    
    except Exception as e:
        print(f"<<error main-agent: {e} >>")
        state.update({
            "messages": state["messages"] + [AIMessage(content=response.content)],
            "intent": "end",
            "intent_reason": response.content
        })
        return state
    

def manager_agent_edge(state: QueryMakerState) -> str:
    """
    Condition for main_agent
    """
    print("=== In condition: main_edge ===")
    if state.get("intent") == "make_query":
        return "make_query"
    else:
        return "end" # end 시 최종 출력 값 반환
    
