"""
ChatBot 핵심 로직 및 LangGraph 워크플로우 정의
"""

from typing import Any, Dict, List, Optional

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from openai import OpenAI

from utils.llm.chatbot.guidelines import GUIDELINES
from utils.llm.chatbot.matcher import LLMGuidelineMatcher
from utils.llm.chatbot.types import ChatBotState, Guideline


class ChatBot:
    """
    LangGraph를 사용한 대화형 챗봇 클래스 (Guideline 기반)
    """

    def __init__(
        self,
        openai_api_key: str,
        model_name: str = "gpt-4o-mini",
        gms_server: str = "http://localhost:8080",
        guidelines: Optional[List[Guideline]] = None,
    ):
        """
        ChatBot 인스턴스 초기화

        Args:
            openai_api_key: OpenAI API 키
            model_name: 사용할 모델명 (기본값: gpt-4o-mini)
            gms_server: DataHub GMS 서버 URL (기본값: http://localhost:8080)
            guidelines: 사용할 가이드라인 목록 (없으면 기본값 사용)
        """
        self.openai_api_key = openai_api_key
        self.model_name = model_name
        self.gms_server = gms_server
        self.guidelines = guidelines or GUIDELINES
        self.guideline_map = {g.id: g for g in self.guidelines}

        self._client = OpenAI(api_key=openai_api_key)
        self.matcher = LLMGuidelineMatcher(
            self.guidelines,
            model=self.model_name,
            client_obj=self._client,
        )
        self.llm = ChatOpenAI(
            temperature=0.0,
            model_name=self.model_name,
            openai_api_key=openai_api_key,
        )
        self.app = self._setup_workflow()

    def _setup_workflow(self):
        """
        LangGraph 워크플로우 설정
        """
        workflow = StateGraph(state_schema=ChatBotState)

        def select_guidelines(state: ChatBotState):
            user_text = ""
            # 마지막 사용자 메시지 찾기
            for msg in reversed(state["messages"]):
                if isinstance(msg, HumanMessage) or (
                    hasattr(msg, "type") and msg.type == "human"
                ):
                    user_text = msg.content
                    break

            # 만약 메시지 객체 구조가 달라서 못 찾았을 경우를 대비해 마지막 메시지 내용 사용
            if not user_text and state["messages"]:
                user_text = state["messages"][-1].content

            matched = self.matcher.match(str(user_text))

            # 컨텍스트 업데이트 (현재 사용자 메시지 추가)
            ctx = state.get("context") or {}
            ctx["last_user_message"] = user_text
            ctx["gms_server"] = self.gms_server
            # search_database_tables_tool을 위해 query 키도 설정
            ctx["query"] = user_text

            # 결과 저장을 위한 임시 딕셔너리
            updates = {
                "table_schema_outputs": [],
                "glossary_outputs": [],
                "query_example_outputs": [],
            }

            for g in matched:
                target_list = None
                if g.id == "table_schema":
                    target_list = updates["table_schema_outputs"]
                elif g.id == "glossary":
                    target_list = updates["glossary_outputs"]
                elif g.id == "query_examples":
                    target_list = updates["query_example_outputs"]

                # 매칭되는 카테고리가 없으면 스킵하거나 로깅 (현재는 스킵)
                if target_list is None:
                    continue

                for tool in g.tools or []:
                    try:
                        result = tool(ctx)
                        # 구조화된 데이터를 그대로 저장 (UI 렌더링용)
                        target_list.append(result)
                    except Exception as exc:
                        target_list.append({"error": str(exc), "tool": tool.__name__})

            # 빈 리스트인 필드는 제거하여 State 업데이트 시 기존 값을 덮어쓰지 않도록 함
            # (LangGraph State 업데이트 동작: 딕셔너리에 포함된 키만 업데이트됨)
            final_updates = {k: v for k, v in updates.items() if v}

            return {
                "selected_ids": [g.id for g in matched],
                "context": ctx,
                **final_updates,
            }

        def call_model(state: ChatBotState):
            selected_ids = state.get("selected_ids", [])

            # 각 출력 필드 가져오기
            table_outs = state.get("table_schema_outputs", [])
            glossary_outs = state.get("glossary_outputs", [])
            query_outs = state.get("query_example_outputs", [])

            guideline_lines = [
                f"- {gid}: {self.guideline_map[gid].description}"
                for gid in selected_ids
                if gid in self.guideline_map
            ] or ["- 적용 가능한 가이드라인 없음 (일반 대화 진행)"]

            # 툴 실행 결과 통합 (LLM 프롬프트용 문자열 변환)
            all_tool_lines = []
            if table_outs:
                all_tool_lines.append("## 테이블 스키마 정보")
                for item in table_outs:
                    all_tool_lines.append(str(item))
            if glossary_outs:
                all_tool_lines.append("## 용어집 정보")
                for item in glossary_outs:
                    all_tool_lines.append(str(item))
            if query_outs:
                all_tool_lines.append("## 쿼리 예제 정보")
                for item in query_outs:
                    all_tool_lines.append(str(item))

            if not all_tool_lines:
                all_tool_lines = ["(툴 실행 결과 없음)"]

            sys_msg = SystemMessage(
                content=(
                    "# 역할\n"
                    "당신은 사용자의 모호한 질문을 명확하고 구체적인 질문으로 만드는 전문 AI 어시스턴트입니다.\n"
                    "제공된 툴 실행 결과와 가이드라인을 바탕으로 사용자에게 답변하세요.\n\n"
                    "# 적용된 가이드라인\n"
                    + "\n".join(guideline_lines)
                    + "\n\n# 툴 실행 결과 (참고 정보)\n"
                    + "\n".join(all_tool_lines)
                    + "\n\n# 지침\n"
                    "- 툴 실행 결과에 유용한 정보가 있다면 적극적으로 인용하여 답변하세요.\n"
                    "- 정보가 부족하다면 추가 질문을 통해 구체화하세요.\n"
                    "- 항상 친절하고 명확하게 대화하세요."
                )
            )

            # 시스템 메시지를 대화의 맨 앞에 추가 (또는 매번 컨텍스트로 주입)
            # LangGraph에서는 메시지 리스트가 계속 쌓이므로,
            # 이번 턴의 시스템 메시지를 앞에 붙여서 invoke 하는 방식 사용
            messages = [sys_msg] + list(state["messages"])
            response = self.llm.invoke(messages)
            return {"messages": response}

        workflow.add_node("select", select_guidelines)
        workflow.add_node("respond", call_model)

        workflow.add_edge(START, "select")
        workflow.add_edge("select", "respond")
        workflow.add_edge("respond", END)

        return workflow.compile(checkpointer=MemorySaver())

    def chat(self, message: str, thread_id: str):
        """
        사용자 메시지에 대한 응답 생성

        Args:
            message: 사용자 입력 메시지
            thread_id: 대화 세션을 구분하는 고유 ID

        Returns:
            dict: LLM 응답을 포함한 결과 딕셔너리
        """
        config = {"configurable": {"thread_id": thread_id}}

        # 초기 상태 설정
        # add_messages 리듀서가 있으므로 messages에는 새 메시지만 넣으면 됨
        input_state = {
            "messages": [HumanMessage(content=message)],
            "context": {"gms_server": self.gms_server},
            "selected_ids": [],
            "table_schema_outputs": [],
            "glossary_outputs": [],
            "query_example_outputs": [],
        }

        return self.app.invoke(input_state, config)

    def update_model(self, model_name: str):
        """
        사용 중인 LLM 모델 변경
        """
        self.model_name = model_name
        self._client = OpenAI(api_key=self.openai_api_key)
        self.matcher = LLMGuidelineMatcher(
            self.guidelines,
            model=self.model_name,
            client_obj=self._client,
        )
        self.llm = ChatOpenAI(
            temperature=0.0,
            model_name=self.model_name,
            openai_api_key=self.openai_api_key,
        )
