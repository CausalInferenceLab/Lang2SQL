"""
Text2SQL Supervisor Chatbot Main

supervisor_as_tool.py 패턴을 적용한 Multi-Agent System
"""

import os
import sys
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

from chatbot_supervisor.supervisor import supervisor_graph
from chatbot_supervisor.state import SupervisorState


def print_separator(char="=", length=60):
    """구분선 출력"""
    print(char * length)


def print_header():
    """헤더 출력"""
    print_separator()
    print("🤖 Text2SQL Supervisor Chatbot (v2)")
    print("Multi-Agent System with Supervisor Architecture")
    print_separator()
    print()


def get_initial_state(user_question: str, database_env: str) -> dict:
    """
    초기 상태 생성

    Args:
        user_question: 사용자 질문
        database_env: 데이터베이스 환경

    Returns:
        dict: 초기 상태
    """
    return {
        "messages": [{"role": "user", "content": user_question}],
        "original_question": user_question,
        "clarified_question": "",
        "is_question_complete": False,
        "missing_info": [],
        "searched_tables": {},
        "question_profile": {},
        "enriched_question": "",
        "generated_sql": "",
        "sql_validation_result": {},
        "sql_result": None,
        "execution_error": None,
        "chart_code": "",
        "formatted_output": "",
        "database_env": database_env,
        "retriever_config": {
            "retriever_name": os.getenv("RETRIEVER_NAME", "Reranker"),
            "top_n": int(os.getenv("RETRIEVER_TOP_N", "5")),
            "device": os.getenv("RETRIEVER_DEVICE", "cpu"),
        },
        "dialect_name": "",
        "supports_ilike": False,
        "dialect_hints": [],
        "current_agent": "",
        "conversation_stage": "clarification",
        # 에러 추적 필드
        "error_count": 0,
        "last_error_message": "",
        "incomplete_question_count": 0,
    }


def main():
    """Supervisor 기반 Text2SQL Chatbot 메인 함수"""

    # 헤더 출력
    print_header()

    # 설정
    thread_config = {"configurable": {"thread_id": "1"}}
    database_env = os.getenv("DATABASE_ENV", "trino")

    print(f"📌 데이터베이스: {database_env}")
    print(f"📌 검색 방식: {os.getenv('RETRIEVER_NAME', 'Reranker')}")
    print()

    # 사용자 질문 입력
    try:
        user_question = input("💬 질문을 입력하세요: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n\n👋 챗봇을 종료합니다.")
        return

    if not user_question:
        print("❌ 질문을 입력해주세요.")
        return

    # 초기 상태
    initial_state = get_initial_state(user_question, database_env)

    print("\n" + "=" * 60)
    print("🎯 Supervisor가 적절한 Agent를 선택합니다...")
    print("=" * 60 + "\n")

    try:
        # Supervisor Graph 실행 (스트리밍 + interrupt 처리)
        while True:
            # 스트리밍 실행 (기본 모드로 노드별 결과 받기)
            interrupted = False
            for event in supervisor_graph.stream(initial_state, thread_config):
                print(f"[DEBUG] Event keys: {event.keys()}")

                # interrupt 발생 확인
                if "__interrupt__" in event:
                    interrupted = True
                    interrupt_data = event["__interrupt__"][0]

                    print("\n" + "=" * 60)
                    print("💬 추가 정보가 필요합니다")
                    print("=" * 60)

                    # 디버깅: interrupt 원인 출력
                    error_cnt = interrupt_data.value.get("error_count", 0)
                    incomplete_cnt = interrupt_data.value.get("incomplete_count", 0)
                    print(f"\n[DEBUG] Interrupt 발생 원인:")
                    print(f"  - SQL 실행 에러 카운트: {error_cnt}")
                    print(f"  - 질문 불완전 카운트: {incomplete_cnt}")

                    print(f"\n{interrupt_data.value.get('context', '')}\n")

                    missing = interrupt_data.value.get("missing_info", [])
                    if missing:
                        print(f"누락된 정보: {', '.join(missing)}")

                    # 사용자 입력 받기
                    try:
                        user_input = input("\n💬 추가 정보를 입력하세요: ").strip()
                    except (EOFError, KeyboardInterrupt):
                        print("\n\n👋 챗봇을 종료합니다.")
                        return

                    if not user_input:
                        print("입력이 없습니다. 종료합니다.")
                        return

                    # 사용자 입력으로 상태 업데이트 및 resume
                    print(f"[DEBUG] 사용자 입력: {user_input}")

                    from langgraph.types import Command

                    # IMPORTANT: interrupt() 이후 코드는 실행되지 않으므로
                    # 여기서 직접 상태를 업데이트해야 함
                    print(f"[DEBUG] update_state 호출 - 상태 리셋 및 새 질문 추가")
                    supervisor_graph.update_state(
                        thread_config,
                        {
                            "messages": [{"role": "user", "content": user_input}],
                            "original_question": user_input,
                            "is_question_complete": False,
                            "error_count": 0,
                            "incomplete_question_count": 0,
                            "last_error_message": "",
                            "missing_info": [],
                            # SQL 관련 상태도 초기화
                            "searched_tables": {},
                            "generated_sql": "",
                            "sql_result": None,
                            "execution_error": None,
                        },
                    )

                    # resume으로 interrupt 해제
                    print(f"[DEBUG] Command(resume=...)로 interrupt 해제")
                    supervisor_graph.update_state(
                        thread_config,
                        Command(resume=user_input),
                    )

                    # stream을 다시 호출하여 재개 (initial_state=None이면 checkpoint에서)
                    print(
                        f"[DEBUG] stream 재개 - initial_state=None (checkpoint에서 이어서)"
                    )
                    initial_state = None
                    break

                # 노드별 실행 결과 출력
                for node_name, node_output in event.items():
                    if node_name == "__interrupt__":
                        continue

                    print(f"[DEBUG] Node: {node_name}")

                    if node_name == "supervisor":
                        if (
                            "messages" in node_output
                            and len(node_output["messages"]) > 0
                        ):
                            last_message = node_output["messages"][-1]
                            if (
                                hasattr(last_message, "tool_calls")
                                and last_message.tool_calls
                            ):
                                tool_name = last_message.tool_calls[0]["name"]
                                print(f"\n🎯 Supervisor: {tool_name} 호출")

                    elif node_name == "tools":
                        if (
                            "messages" in node_output
                            and len(node_output["messages"]) > 0
                        ):
                            last_message = node_output["messages"][-1]
                            print(f"\n🔧 Agent 실행 완료")
                            if hasattr(last_message, "content"):
                                print(f"\n💬 Agent 응답:\n{last_message.content}\n")

                    elif node_name == "human_feedback":
                        print(f"[DEBUG] human_feedback 노드 실행 완료")
                        if "error_count" in node_output:
                            print(
                                f"[DEBUG] human_feedback 출력 - error_count: {node_output.get('error_count')}"
                            )
                        if "incomplete_question_count" in node_output:
                            print(
                                f"[DEBUG] human_feedback 출력 - incomplete_question_count: {node_output.get('incomplete_question_count')}"
                            )
                        if "is_question_complete" in node_output:
                            print(
                                f"[DEBUG] human_feedback 출력 - is_question_complete: {node_output.get('is_question_complete')}"
                            )
                        if "original_question" in node_output:
                            print(
                                f"[DEBUG] human_feedback 출력 - original_question: {node_output.get('original_question')}"
                            )

                    elif node_name == "fix_sql_error":
                        print(f"\n🔧 SQL 자동 수정 실행")
                        if (
                            "messages" in node_output
                            and len(node_output["messages"]) > 0
                        ):
                            last_message = node_output["messages"][-1]
                            if hasattr(last_message, "content"):
                                print(f"\n{last_message.content}\n")
                            elif isinstance(last_message, dict):
                                print(f"\n{last_message.get('content', '')}\n")
                        if "generated_sql" in node_output:
                            print(f"[DEBUG] 수정된 SQL이 state에 업데이트됨")
                            print(
                                f"[DEBUG] SQL: {node_output['generated_sql'][:100]}..."
                            )

            # interrupt가 발생하지 않았으면 종료
            if not interrupted:
                print("\n" + "=" * 60)
                print("✅ 처리 완료!")
                print("=" * 60)
                break

    except KeyboardInterrupt:
        print("\n\n👋 챗봇을 종료합니다.")
        return

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}\n")
        import traceback

        traceback.print_exc()

    print("\n감사합니다! 👋\n")


if __name__ == "__main__":
    main()
