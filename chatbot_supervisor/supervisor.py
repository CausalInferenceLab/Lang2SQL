"""
Supervisor Graph

supervisor_as_tool.py 패턴을 따라 Supervisor를 중심으로 한 Multi-Agent System 생성
"""

from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import tools_condition
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import interrupt

from utils.llm.core import get_llm
from chatbot_supervisor.state import SupervisorState

# Agent는 더 이상 사용하지 않음 - Tools를 직접 사용
# from chatbot_supervisor.agents import (
#     clarification_agent,
#     query_builder_agent,
#     reporter_agent,
# )


def create_supervisor_graph():
    """
    Supervisor를 중심으로 한 Multi-Agent System 생성

    human-in-the-loop.py 패턴 적용:
    - Supervisor가 Agent Tools를 선택하여 호출
    - Clarification Agent 후 interrupt()로 사용자 입력 대기
    - 사용자 입력을 받아 다시 처리 계속

    Returns:
        CompiledGraph: 컴파일된 Supervisor Graph
    """

    llm = get_llm()

    # 모든 Tools를 Supervisor 레벨로 (Agent가 아닌)
    from chatbot_supervisor.tools import (
        validate_question_tool,
        suggest_missing_entities_tool,
        search_tables_tool,
        generate_sql_tool,
        execute_sql_tool,
        format_table_tool,
        create_chart_tool,
    )

    # Supervisor가 직접 사용할 도구들
    all_tools = [
        validate_question_tool,
        suggest_missing_entities_tool,
        search_tables_tool,
        generate_sql_tool,
        execute_sql_tool,
        format_table_tool,
        create_chart_tool,
    ]

    # Supervisor 노드: LLM이 적절한 Agent Tool을 선택
    def supervisor_node(state: SupervisorState):
        """
        Supervisor 노드

        현재 대화 상태를 분석하여 적절한 Agent를 선택합니다.
        """
        print(f"\n[DEBUG] ===== supervisor_node 시작 =====")
        print(f"[DEBUG] 현재 상태 (supervisor 진입 시):")
        print(f"  - original_question: {state.get('original_question', 'N/A')}")
        print(f"  - is_question_complete: {state.get('is_question_complete', False)}")
        print(f"  - error_count: {state.get('error_count', 0)}")
        print(
            f"  - incomplete_question_count: {state.get('incomplete_question_count', 0)}"
        )
        print(f"  - 메시지 수: {len(state.get('messages', []))}")
        if state.get("messages"):
            last_msg = state["messages"][-1]
            msg_type = getattr(
                last_msg, "type", getattr(last_msg, "__class__.__name__", "unknown")
            )
            msg_preview = str(getattr(last_msg, "content", ""))[:50]
            print(f"  - 마지막 메시지: {msg_type} - {msg_preview}...")
        print(f"[DEBUG] ====================================\n")

        # SQL 결과가 있고 리포팅이 완료되었으면 tool 호출 불필요
        # 그 외에는 반드시 tool 호출
        should_require_tool = not (
            state.get("sql_result") is not None and state.get("formatted_output")
        )

        if should_require_tool:
            llm_with_tools = llm.bind_tools(tools=all_tools, tool_choice="required")
        else:
            llm_with_tools = llm.bind_tools(tools=all_tools)

        # Supervisor 시스템 프롬프트
        system_message = """당신은 Text2SQL 시스템의 Supervisor입니다.

**중요**: 반드시 제공된 Tool 중 하나를 선택해야 합니다. 직접 응답하지 마세요.

역할:
- 사용자의 질문과 현재 대화 상태를 분석하여 적절한 Tool을 순차적으로 호출합니다.
- 단계별로 필요한 도구를 선택하여 Text2SQL 워크플로우를 진행합니다.

**Tool 선택 순서**:

**1단계: 질문 검증**
- 처음 질문을 받으면 → `validate_question_tool`
  - 질문이 불완전하면 → `suggest_missing_entities_tool` 
  - 그리고 사용자에게 피드백 후 대기

**2단계: SQL 생성** (질문이 완전한 경우)
- `search_tables_tool`: 관련 테이블 검색
- `generate_sql_tool`: SQL 쿼리 생성
- `execute_sql_tool`: SQL 실행

**3단계: 결과 보고** (SQL 실행 완료 후)
- `format_table_tool`: 결과 포맷팅
- `create_chart_tool`: 시각화 (선택적)

**현재 상태 확인**:
- is_question_complete = False → 1단계 (validate_question_tool)
- is_question_complete = True, searched_tables = {} → 2단계 (search_tables_tool)
- searched_tables 있음, generated_sql = "" → generate_sql_tool
- generated_sql 있음, sql_result = None → execute_sql_tool
- sql_result 있음, formatted_output = "" → 3단계 (format_table_tool)
- formatted_output 있음 → 완료

한 번에 하나의 도구만 호출하세요."""

        # 메시지에 시스템 프롬프트 포함
        messages_with_system = [{"role": "system", "content": system_message}] + state[
            "messages"
        ]

        result = llm_with_tools.invoke(messages_with_system)
        return {"messages": [result]}

    # Tools 노드: 도구 실행 및 상태 업데이트
    def tools_node(state: SupervisorState):
        """
        Tools 노드 (커스텀)

        Supervisor가 선택한 도구를 실행하고 상태를 업데이트합니다.
        ToolNode 대신 커스텀 노드를 사용하여 상태를 제대로 업데이트합니다.
        """
        from langchain_core.messages import ToolMessage

        # 마지막 메시지에서 tool_calls 가져오기
        last_message = state["messages"][-1]

        if not hasattr(last_message, "tool_calls") or not last_message.tool_calls:
            return {}

        # 각 tool_call 실행
        tool_messages = []
        state_updates = {}

        # 에러 추적 초기값 설정
        current_error_count = state.get("error_count", 0)
        current_incomplete_count = state.get("incomplete_question_count", 0)

        for tool_call in last_message.tool_calls:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]
            tool_call_id = tool_call["id"]

            # 도구 실행
            try:
                # 도구 함수 가져오기
                tool_func = None
                for tool in all_tools:
                    if tool.name == tool_name:
                        tool_func = tool.func
                        break

                if tool_func is None:
                    result = f"❌ 도구 '{tool_name}'를 찾을 수 없습니다."
                else:
                    # state 인자가 필요한 도구는 state를 전달
                    import inspect

                    sig = inspect.signature(tool_func)

                    if "state" in sig.parameters:
                        result = tool_func(state=state, **tool_args)
                    else:
                        result = tool_func(**tool_args)

                    # 도구별 상태 업데이트
                    if tool_name == "search_tables_tool":
                        # search_tables 함수를 직접 호출하여 결과 가져오기
                        from utils.llm.retrieval import search_tables

                        retriever_config = state.get("retriever_config", {})
                        tables = search_tables(
                            query=tool_args.get("question", ""),
                            retriever_name=retriever_config.get(
                                "retriever_name", "기본"
                            ),
                            top_n=retriever_config.get("top_n", 5),
                            device=retriever_config.get("device", "cpu"),
                        )
                        if tables:
                            state_updates["searched_tables"] = tables

                    elif tool_name == "generate_sql_tool":
                        # SQL 추출
                        if "```sql" in str(result):
                            import re

                            sql_match = re.search(
                                r"```sql\n(.*?)```", str(result), re.DOTALL
                            )
                            if sql_match:
                                sql = sql_match.group(1).strip()
                                state_updates["generated_sql"] = sql

                    elif tool_name == "execute_sql_tool":
                        # SQL 실행 결과 저장
                        from utils.databases.factory import DatabaseFactory

                        sql = state.get("generated_sql", "")
                        if sql:
                            database_env = state.get("database_env", "clickhouse")
                            try:
                                db = DatabaseFactory.get_connector(db_type=database_env)
                                sql_result = db.run_sql(sql)
                                # DataFrame을 dict로 변환하여 직렬화 가능하게
                                if sql_result is not None:
                                    state_updates["sql_result"] = {
                                        "data": sql_result.to_dict("records"),
                                        "columns": sql_result.columns.tolist(),
                                        "index": sql_result.index.tolist(),
                                    }
                                else:
                                    state_updates["sql_result"] = None
                                state_updates["execution_error"] = None
                            except Exception as exec_error:
                                state_updates["execution_error"] = str(exec_error)

                    elif tool_name == "validate_question_tool":
                        # 질문 완전성 업데이트
                        result_str = str(result)
                        # "불완전"을 먼저 체크해야 함 ("완전"이 "불완전"에 포함되므로)
                        if "불완전" in result_str or "🔍" in result_str:
                            state_updates["is_question_complete"] = False
                            print(
                                f"[DEBUG] validate_question_tool: 질문 불완전 → is_question_complete = False"
                            )
                        elif "완전합니다" in result_str or "✅" in result_str:
                            state_updates["is_question_complete"] = True
                            print(
                                f"[DEBUG] validate_question_tool: 질문 완전 → is_question_complete = True"
                            )
                        else:
                            # 기본값: 불완전으로 간주
                            state_updates["is_question_complete"] = False
                            print(
                                f"[DEBUG] validate_question_tool: 알 수 없는 응답 → is_question_complete = False"
                            )

                    elif tool_name == "suggest_missing_entities_tool":
                        # 누락 정보가 없으면 질문 완전
                        if "모든 필수 정보" in str(result) or "✅" in str(result):
                            state_updates["is_question_complete"] = True
                            print(
                                f"[DEBUG] suggest_missing_entities_tool: 모든 정보 포함 → is_question_complete = True"
                            )

                    elif tool_name == "format_table_tool":
                        # 포맷팅 결과 저장
                        state_updates["formatted_output"] = str(result)

                    elif tool_name == "create_chart_tool":
                        # 차트 코드 저장
                        state_updates["chart_code"] = str(result)

                # 에러 패턴 감지 및 카운트 업데이트
                result_str = str(result)
                last_error_msg = state.get("last_error_message", "")

                # SQL 실행 에러 감지
                if "❌ SQL 실행 중 오류" in result_str:
                    # query_id를 제거하고 에러 타입만 비교
                    import re

                    error_type = re.sub(r"query_id=[^)]+", "query_id=*", result_str)
                    last_error_type = re.sub(
                        r"query_id=[^)]+", "query_id=*", last_error_msg
                    )

                    print(f"[DEBUG] 에러 타입 비교:")
                    print(f"  - 현재 에러: {error_type[:150]}...")
                    print(f"  - 이전 에러: {last_error_type[:150]}...")

                    if error_type == last_error_type and last_error_type:
                        # 동일한 타입의 에러가 반복됨
                        current_error_count += 1
                        print(
                            f"[DEBUG] 동일한 에러 반복 감지! 카운트 증가: {current_error_count}"
                        )
                    else:
                        # 새로운 에러
                        current_error_count = 1
                        print(f"[DEBUG] 새로운 에러 타입 감지, 카운트 리셋: 1")

                    state_updates["error_count"] = current_error_count
                    state_updates["last_error_message"] = result_str
                    print(f"[DEBUG] SQL 실행 에러 감지 (연속 {current_error_count}회)")

                # 질문 불완전 감지
                elif (
                    "🔍 질문이 불완전합니다" in result_str
                    or "질문이 불완전" in result_str
                ):
                    current_incomplete_count += 1
                    state_updates["incomplete_question_count"] = (
                        current_incomplete_count
                    )
                    print(
                        f"[DEBUG] 질문 불완전 감지 (연속 {current_incomplete_count}회)"
                    )

                # 성공적인 실행 시 카운트 리셋
                elif tool_name == "execute_sql_tool" and "❌" not in result_str:
                    # SQL 실행 성공
                    state_updates["error_count"] = 0
                    state_updates["last_error_message"] = ""
                    print(f"[DEBUG] SQL 실행 성공 - 에러 카운트 리셋")

                elif tool_name == "validate_question_tool" and "✅" in result_str:
                    # 질문 검증 성공
                    state_updates["incomplete_question_count"] = 0
                    print(f"[DEBUG] 질문 검증 성공 - 불완전 카운트 리셋")

                # ToolMessage 생성
                tool_messages.append(
                    ToolMessage(
                        content=str(result),
                        name=tool_name,
                        tool_call_id=tool_call_id,
                    )
                )

            except Exception as e:
                import traceback

                error_msg = f"❌ 도구 실행 중 오류: {str(e)}\n{traceback.format_exc()}"
                tool_messages.append(
                    ToolMessage(
                        content=error_msg,
                        name=tool_name,
                        tool_call_id=tool_call_id,
                    )
                )

        # 메시지와 상태 업데이트 반환
        print(f"\n[DEBUG] ===== tools_node 상태 업데이트 =====")
        print(f"[DEBUG] 업데이트할 상태:")
        for key, value in state_updates.items():
            if key in [
                "error_count",
                "incomplete_question_count",
                "is_question_complete",
                "last_error_message",
            ]:
                print(f"  - {key}: {value}")
        print(f"[DEBUG] =====================================\n")

        return {
            "messages": tool_messages,
            **state_updates,
        }

    # Human-in-the-Loop 노드: 사용자 입력 대기
    def human_feedback_node(state: SupervisorState):
        """
        Human-in-the-Loop 노드

        질문이 불완전하거나 에러가 반복되는 경우 사용자에게 추가 정보를 요청합니다.
        interrupt()를 사용하여 사용자 입력을 기다립니다.
        """
        print(f"[DEBUG] human_feedback_node 시작")
        print(
            f"[DEBUG] is_question_complete: {state.get('is_question_complete', False)}"
        )
        print(f"[DEBUG] error_count: {state.get('error_count', 0)}")
        print(
            f"[DEBUG] incomplete_question_count: {state.get('incomplete_question_count', 0)}"
        )

        # 에러 상황 판단
        error_count = state.get("error_count", 0)
        incomplete_count = state.get("incomplete_question_count", 0)

        # 사용자에게 보여줄 컨텍스트 구성
        last_message = state["messages"][-1].content if state["messages"] else ""

        # 에러 유형에 따른 질문 메시지 구성
        if error_count >= 2:
            question_prompt = "⚠️ SQL 실행이 반복적으로 실패했습니다. 질문을 다시 입력하거나 추가 정보를 제공해주세요:"
            context = f"{last_message}\n\n연속 에러 횟수: {error_count}회"
        elif incomplete_count >= 3:
            question_prompt = "⚠️ 질문이 불완전하다는 판단이 반복되고 있습니다. 질문을 다시 입력해주세요:"
            context = f"{last_message}\n\n불완전 판단 횟수: {incomplete_count}회"
        else:
            question_prompt = "추가 정보를 입력해주세요:"
            context = last_message

        print(f"[DEBUG] interrupt() 호출 전")

        # interrupt로 사용자 입력 대기
        user_input = interrupt(
            {
                "question": question_prompt,
                "context": context,
                "missing_info": state.get("missing_info", []),
                "error_count": error_count,
                "incomplete_count": incomplete_count,
            }
        )

        print(f"[DEBUG] interrupt() 호출 후, user_input: {user_input}")

        # 사용자 입력을 메시지에 추가하고, 상태 초기화
        if user_input and isinstance(user_input, str):
            print(f"[DEBUG] 사용자 입력 처리 중: '{user_input}'")
            print(
                f"[DEBUG] 상태 리셋: error_count=0, incomplete_question_count=0, is_question_complete=False"
            )

            # 사용자가 새 질문을 입력했으므로 상태를 리셋
            reset_state = {
                "messages": [{"role": "user", "content": user_input}],
                "original_question": user_input,  # 새 질문으로 완전히 대체
                "is_question_complete": False,  # 다시 검증 필요
                "missing_info": [],  # 초기화
                "error_count": 0,  # 에러 카운트 리셋
                "last_error_message": "",  # 에러 메시지 리셋
                "incomplete_question_count": 0,  # 불완전 카운트 리셋
                # 이전 SQL 관련 상태도 초기화
                "searched_tables": {},
                "generated_sql": "",
                "sql_result": None,
                "execution_error": None,
            }

            print(f"[DEBUG] human_feedback_node 반환 값:")
            print(f"  - original_question: {reset_state['original_question']}")
            print(f"  - is_question_complete: {reset_state['is_question_complete']}")
            print(f"  - error_count: {reset_state['error_count']}")
            print(
                f"  - incomplete_question_count: {reset_state['incomplete_question_count']}"
            )

            return reset_state

        return {}

    # SQL 에러 자동 수정 노드
    def fix_sql_error_node(state: SupervisorState):
        """
        SQL 실행 에러 자동 수정 노드

        LLM에게 DB 환경, 에러 메시지, 실행한 쿼리를 제공하여
        수정된 쿼리를 생성합니다.
        """
        print(f"\n[DEBUG] ===== fix_sql_error_node 시작 =====")

        error_count = state.get("error_count", 0)
        last_error = state.get("last_error_message", "")
        generated_sql = state.get("generated_sql", "")
        database_env = state.get("database_env", "unknown")
        searched_tables = state.get("searched_tables", {})

        print(f"[DEBUG] 에러 수정 시도:")
        print(f"  - error_count: {error_count}")
        print(f"  - database: {database_env}")
        print(f"  - SQL: {generated_sql[:100]}...")

        # LLM 프롬프트 구성
        error_fix_prompt = f"""당신은 SQL 에러 수정 전문가입니다.

**데이터베이스 환경**: {database_env}

**실행한 SQL 쿼리**:
```sql
{generated_sql}
```

**발생한 에러**:
{last_error}

**사용 가능한 테이블 정보**:
{chr(10).join([f"- {table_name}: {info.get('columns', [])}" for table_name, info in searched_tables.items()])}

**지시사항**:
1. 에러 원인을 분석하세요
2. 데이터베이스 환경({database_env})에 맞는 올바른 SQL 구문을 사용하세요
3. 특히 날짜/타입 변환, 함수 사용법을 정확히 확인하세요
4. 수정된 쿼리만 반환하세요 (설명 없이 SQL만)

**출력 형식**:
```sql
수정된 SQL 쿼리
```"""

        print(f"[DEBUG] LLM 호출 - SQL 에러 수정 요청")

        try:
            response = llm.invoke([{"role": "user", "content": error_fix_prompt}])
            fixed_sql_response = response.content

            print(f"[DEBUG] LLM 응답: {fixed_sql_response[:200]}...")

            # SQL 추출
            import re

            sql_match = re.search(r"```sql\n(.*?)```", fixed_sql_response, re.DOTALL)

            if sql_match:
                fixed_sql = sql_match.group(1).strip()
                print(f"[DEBUG] 수정된 SQL 추출 성공")
                print(f"[DEBUG] 수정된 SQL: {fixed_sql}")

                return {
                    "messages": [
                        {
                            "role": "assistant",
                            "content": f"🔧 SQL 에러 자동 수정:\n\n**원본 SQL**:\n```sql\n{generated_sql}\n```\n\n**수정된 SQL**:\n```sql\n{fixed_sql}\n```\n\n**에러**: {last_error[:200]}...",
                        }
                    ],
                    "generated_sql": fixed_sql,  # 수정된 SQL로 업데이트
                }
            else:
                print(f"[DEBUG] SQL 추출 실패")
                return {
                    "messages": [
                        {
                            "role": "assistant",
                            "content": f"⚠️ SQL 자동 수정 실패 (SQL 추출 불가)",
                        }
                    ]
                }

        except Exception as e:
            print(f"[DEBUG] LLM 호출 실패: {e}")
            return {
                "messages": [
                    {
                        "role": "assistant",
                        "content": f"⚠️ SQL 자동 수정 중 오류 발생: {str(e)}",
                    }
                ]
            }

    # 조건부 엣지: tools 후 어디로 갈지 판단
    def should_ask_human(state: SupervisorState) -> str:
        """
        다음 노드 판단 (TOOL 실행 결과 메시지 기반)

        - SQL 실행 에러가 1회 발생 → fix_sql_error (자동 수정 시도)
        - SQL 실행 에러가 2회 이상 반복 → human_feedback
        - 질문 불완전이 3회 이상 반복 → human_feedback
        - 질문이 불완전하면 → human_feedback
        - SQL 결과가 있고 리포팅 완료 → END
        - 그 외 → supervisor
        """
        print(f"\n[DEBUG] ===== should_ask_human 시작 =====")

        # IMPORTANT: TOOL 실행 결과 메시지 기반 판단
        error_count = state.get("error_count", 0)
        incomplete_count = state.get("incomplete_question_count", 0)
        is_question_complete = state.get("is_question_complete", False)

        print(f"[DEBUG] 현재 상태:")
        print(f"  - error_count: {error_count}")
        print(f"  - incomplete_question_count: {incomplete_count}")
        print(f"  - is_question_complete: {is_question_complete}")

        # Case2-1: SQL 실행 에러가 2회 이상 반복 → human-in-the-loop
        if error_count >= 2:
            print(f"[DEBUG] ✋ SQL 실행 에러 {error_count}회 반복 → human_feedback으로")
            print(
                f"[DEBUG] 마지막 에러: {state.get('last_error_message', '')[:100]}..."
            )
            return "human_feedback"

        # Case2-2: SQL 실행 에러가 1회 발생 → 자동 수정 시도
        if error_count == 1:
            # 마지막 메시지가 SQL 에러인지 확인
            if state.get("messages") and len(state["messages"]) > 0:
                last_message = state["messages"][-1]
                if hasattr(last_message, "type") and last_message.type == "tool":
                    tool_name = getattr(last_message, "name", "")
                    tool_content = getattr(last_message, "content", "")

                    if (
                        tool_name == "execute_sql_tool"
                        and "❌ SQL 실행 중 오류" in tool_content
                    ):
                        print(
                            f"[DEBUG] 🔧 SQL 실행 에러 1회 발생 → fix_sql_error로 (자동 수정 시도)"
                        )
                        return "fix_sql_error"

        # Case1: 질문 불완전이 3회 이상 반복되면 human-in-the-loop
        if incomplete_count >= 3:
            print(
                f"[DEBUG] ✋ 질문 불완전 {incomplete_count}회 반복 → human_feedback으로"
            )
            return "human_feedback"

        # 마지막 tool 메시지 확인
        if state.get("messages") and len(state["messages"]) > 0:
            last_message = state["messages"][-1]
            print(
                f"[DEBUG] 마지막 메시지 타입: {getattr(last_message, 'type', 'unknown')}"
            )

            # Tool 메시지인지 확인 (Agent 실행 결과)
            if hasattr(last_message, "type") and last_message.type == "tool":
                tool_name = getattr(last_message, "name", "")
                tool_content = getattr(last_message, "content", "")
                print(f"[DEBUG] 마지막 Tool: {tool_name}")
                print(f"[DEBUG] Tool 결과 미리보기: {tool_content[:100]}...")

                # format_table_tool 실행되었으면 → END
                if tool_name == "format_table_tool":
                    print(f"[DEBUG] ✅ 포맷팅 완료 → END")
                    return END

                # validate_question_tool 또는 suggest_missing_entities_tool 실행 후
                if tool_name in [
                    "validate_question_tool",
                    "suggest_missing_entities_tool",
                ]:
                    print(f"[DEBUG] {tool_name} 실행 완료")

                    # 질문이 불완전하면 → human_feedback (첫 번째 불완전에서 바로)
                    if not is_question_complete:
                        print(
                            f"[DEBUG] ✋ 질문 불완전 ({tool_name}) → human_feedback으로"
                        )
                        print(
                            f"[DEBUG] incomplete_count: {incomplete_count} (3회 이상이면 자동 human_feedback)"
                        )
                        return "human_feedback"
                    else:
                        print(f"[DEBUG] ✅ 질문 완전 ({tool_name}) → supervisor로")
                        return "supervisor"

        # SQL 결과가 있으면 supervisor로 (리포팅 단계로 진행)
        if state.get("sql_result") is not None:
            print(f"[DEBUG] ✅ SQL 결과 있음 → supervisor로 (리포팅)")
            return "supervisor"

        # 질문이 완전하면 supervisor로 (Query Builder로 진행)
        if is_question_complete:
            print(f"[DEBUG] ✅ 질문 완전 → supervisor로 (Query Builder)")
            return "supervisor"

        # 기본값: supervisor로 (예상치 못한 경우)
        print(f"[DEBUG] ⚠️ 기본값 → supervisor로")
        return "supervisor"

    # Supervisor Graph 구성
    graph_builder = StateGraph(SupervisorState)

    graph_builder.add_node("supervisor", supervisor_node)
    graph_builder.add_node("tools", tools_node)  # 커스텀 tools_node 사용
    graph_builder.add_node("human_feedback", human_feedback_node)
    graph_builder.add_node(
        "fix_sql_error", fix_sql_error_node
    )  # SQL 에러 자동 수정 노드

    # Workflow:
    # START → supervisor → [tools_condition] → tools OR END
    # tools → [should_ask_human] → fix_sql_error OR human_feedback OR supervisor OR END
    # fix_sql_error → supervisor (수정된 SQL로 재시도)
    # human_feedback → supervisor
    graph_builder.add_edge(START, "supervisor")
    graph_builder.add_conditional_edges("supervisor", tools_condition)  # tools 또는 END
    graph_builder.add_conditional_edges(
        "tools",
        should_ask_human,
        {
            "fix_sql_error": "fix_sql_error",  # SQL 에러 자동 수정
            "human_feedback": "human_feedback",  # 사용자 개입
            "supervisor": "supervisor",  # 계속 진행
            END: END,  # 모든 처리 완료 시
        },
    )
    graph_builder.add_edge("fix_sql_error", "supervisor")  # 수정 후 supervisor로
    graph_builder.add_edge("human_feedback", "supervisor")

    # 메모리와 함께 컴파일 (interrupt를 위해 필수)
    memory = MemorySaver()
    return graph_builder.compile(checkpointer=memory)


# 싱글톤 그래프 인스턴스
supervisor_graph = create_supervisor_graph()
