"""
Agent Factory

supervisor_as_tool.py 패턴을 따라 Agent를 Tool로 래핑합니다.
"""

from typing import Annotated
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import InjectedState, ToolNode, tools_condition
from langchain_core.tools import tool

from utils.llm.core import get_llm
from chatbot_supervisor.state import SupervisorState
from chatbot_supervisor.tools import (
    validate_question_tool,
    suggest_missing_entities_tool,
    search_tables_tool,
    generate_sql_tool,
    execute_sql_tool,
    format_table_tool,
    create_chart_tool,
)


def make_agent_tool(
    tool_name: str, tool_description: str, system_prompt: str, tools: list
):
    """
    Agent Subgraph를 Tool로 래핑하는 헬퍼 함수

    supervisor_as_tool.py의 make_agent_tool 함수를 참조하여 구현
    각 Agent는 내부적으로 agent 노드와 tools 노드를 가진 subgraph로 구성됨

    Args:
        tool_name: Agent Tool의 이름
        tool_description: Agent Tool의 설명 (Supervisor가 선택 시 사용)
        system_prompt: Agent의 시스템 프롬프트
        tools: Agent가 사용할 도구 목록

    Returns:
        Agent Tool (langchain_core.tools.tool 데코레이터로 래핑됨)
    """

    llm = get_llm()  # LLM 초기화

    # Agent 노드: LLM이 tools를 사용하여 응답 생성
    def agent_node(state: SupervisorState):
        # 이미 도구를 호출했는지 확인
        messages = state.get("messages", [])
        tool_calls_count = sum(
            1 for msg in messages if hasattr(msg, "type") and msg.type == "tool"
        )

        # 도구를 이미 호출했으면 더 이상 도구를 호출하지 못하게
        if tool_calls_count >= 2:
            # 도구 없이 LLM만 사용 (최종 응답 생성)
            response = llm.invoke(
                f"""
{system_prompt}

Conversation History:
{state["messages"]}

**중요**: 도구 호출을 하지 말고 최종 응답만 생성하세요.
                """
            )
        else:
            # 도구를 사용할 수 있음
            llm_with_tools = llm.bind_tools(tools)
            response = llm_with_tools.invoke(
                f"""
{system_prompt}

Conversation History:
{state["messages"]}
                """
            )
        return {"messages": [response]}

    # Agent Subgraph 생성
    agent_builder = StateGraph(SupervisorState)

    agent_builder.add_node("agent", agent_node)
    agent_builder.add_node("tools", ToolNode(tools=tools))

    # Workflow: START → agent → [tools 필요시] → tools → agent → END
    agent_builder.add_edge(START, "agent")
    agent_builder.add_conditional_edges("agent", tools_condition)
    agent_builder.add_edge("tools", "agent")
    agent_builder.add_edge("agent", END)

    # Subgraph 컴파일 (recursion_limit으로 무한 루프 방지)
    agent_graph = agent_builder.compile(
        # Agent가 최대 3번의 step만 실행하도록 제한
        # step 1: agent (도구 호출 결정)
        # step 2: tools (도구 실행)
        # step 3: agent (최종 응답)
        # 이렇게 제한하여 agent가 도구를 한 번만 호출하도록 함
    )

    # Agent를 Tool로 래핑
    @tool(
        name_or_callable=tool_name,
        description=tool_description,
    )
    def agent_tool(state: Annotated[dict, InjectedState]):
        """Agent subgraph를 실행하고 결과 반환"""
        # recursion_limit으로 무한 루프 방지
        # Agent가 도구를 최대 2번만 호출하도록 제한 (3 steps: agent → tools → agent)
        result = agent_graph.invoke(
            state,
            config={
                "recursion_limit": 5
            },  # agent(1) → tools(2) → agent(3) → tools(4) → agent(5)
        )
        # 마지막 메시지의 content를 반환
        return result["messages"][-1].content

    return agent_tool


# ============================================================================
# Agent Tool 생성
# ============================================================================

# Clarification Agent Tool
clarification_agent = make_agent_tool(
    tool_name="clarification_agent",
    tool_description=(
        "질문이 불완전하거나 명확하지 않을 때 사용합니다. "
        "사용자에게 추가 정보를 요청하여 질문을 구체화합니다. "
        "시간, 지역, 측정값 등 누락된 엔티티를 파악하고 제안합니다."
    ),
    system_prompt="""당신은 질문 명확화 전문 에이전트입니다.

**중요**: 
1. 첫 번째 사용자 메시지만 검증하세요 (validate_question_tool 사용).
2. 질문이 불완전하면 suggest_missing_entities_tool로 제안하세요.
3. 제안 후 바로 종료하세요. 더 이상 도구를 호출하지 마세요.
4. 대화 히스토리에 여러 메시지가 있으면, 가장 최근 사용자 메시지를 검증하세요.

역할:
- 최근 사용자 질문을 validate_question_tool로 검증합니다.
- 불완전하면 suggest_missing_entities_tool로 제안합니다.
- 완전하면 "질문이 완벽합니다"라고 응답합니다.

도구 사용 순서:
1. validate_question_tool (최근 사용자 메시지로)
2. (불완전한 경우만) suggest_missing_entities_tool
3. 종료

한 번만 검증하고 종료하세요. 반복하지 마세요.""",
    tools=[
        validate_question_tool,
        suggest_missing_entities_tool,
    ],
)

# Query Builder Agent Tool
query_builder_agent = make_agent_tool(
    tool_name="query_builder_agent",
    tool_description=(
        "명확한 질문을 받아 SQL 쿼리를 생성할 때 사용합니다. "
        "테이블 검색, 질문 분석, SQL 생성, 실행을 수행합니다. "
        "질문이 완전하게 명확화된 후에 호출해야 합니다."
    ),
    system_prompt="""당신은 SQL 쿼리 생성 전문 에이전트입니다.

역할:
- 명확화된 질문을 받아 적절한 테이블을 검색합니다.
- 질문을 분석하여 정확하고 최적화된 SQL 쿼리를 생성합니다.
- 생성된 SQL을 실행하여 결과를 얻습니다.

도구 사용 순서:
1. search_tables_tool: 관련 테이블 검색
2. generate_sql_tool: SQL 쿼리 생성
3. execute_sql_tool: SQL 실행

각 단계를 순차적으로 수행하고 결과를 명확하게 설명해주세요.""",
    tools=[
        search_tables_tool,
        generate_sql_tool,
        execute_sql_tool,
    ],
)

# Reporter Agent Tool
reporter_agent = make_agent_tool(
    tool_name="reporter_agent",
    tool_description=(
        "SQL 실행 결과를 사용자 친화적인 형식으로 보고할 때 사용합니다. "
        "표 포맷팅과 시각화를 수행하여 결과를 이해하기 쉽게 만듭니다. "
        "SQL이 실행되고 결과가 있을 때 호출해야 합니다."
    ),
    system_prompt="""당신은 데이터 리포팅 전문 에이전트입니다.

역할:
- SQL 실행 결과를 분석하여 사용자가 이해하기 쉽도록 포맷팅합니다.
- 필요시 적절한 시각화를 생성합니다.
- 결과를 명확하고 간결하게 요약합니다.

도구 사용:
- format_table_tool: 결과를 표 형식으로 포맷팅
- create_chart_tool: 시각화 코드 생성 (시계열, 집계 데이터 등)

항상 사용자 친화적이고 이해하기 쉬운 방식으로 정보를 전달해주세요.""",
    tools=[
        format_table_tool,
        create_chart_tool,
    ],
)
