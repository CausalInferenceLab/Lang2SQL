"""
ChatBot 핵심 로직 및 LangGraph 워크플로우 정의
"""

from typing import Any, Dict, List, Optional

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from openai import OpenAI

from utils.llm.tools import filter_relevant_outputs
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

            # 결과 저장을 위한 임시 딕셔너리 (기존 상태 유지 + 추가)
            updates = {
                "table_schema_outputs": list(state.get("table_schema_outputs") or []),
                "glossary_outputs": list(state.get("glossary_outputs") or []),
                "query_example_outputs": list(state.get("query_example_outputs") or []),
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

        def filter_context(state: ChatBotState):
            """
            수집된 컨텍스트를 LLM을 통해 필터링하는 노드
            """
            # HumanMessage만 필터링
            human_messages = [
                msg
                for msg in state["messages"]
                if isinstance(msg, HumanMessage)
                or (hasattr(msg, "type") and msg.type == "human")
            ]

            table_outs = state.get("table_schema_outputs", [])
            glossary_outs = state.get("glossary_outputs", [])
            query_outs = state.get("query_example_outputs", [])

            # 필터링 수행
            filtered = filter_relevant_outputs(
                messages=human_messages,
                table_outputs=table_outs,
                glossary_outputs=glossary_outs,
                query_outputs=query_outs,
                llm=self.llm,
            )

            return {
                "table_schema_outputs": filtered.get("table_schema_outputs", []),
                "glossary_outputs": filtered.get("glossary_outputs", []),
                "query_example_outputs": filtered.get("query_example_outputs", []),
            }

        def generate_analysis_guide(state: ChatBotState):
            """
            필터링된 컨텍스트를 바탕으로 분석 가이드를 생성하는 노드
            """
            user_text = ""
            for msg in reversed(state["messages"]):
                if isinstance(msg, HumanMessage) or (
                    hasattr(msg, "type") and msg.type == "human"
                ):
                    user_text = msg.content
                    break

            if not user_text and state["messages"]:
                user_text = state["messages"][-1].content

            table_outs = state.get("table_schema_outputs", [])
            glossary_outs = state.get("glossary_outputs", [])
            query_outs = state.get("query_example_outputs", [])

            # 컨텍스트가 없으면 가이드 생성 생략
            if not (table_outs or glossary_outs or query_outs):
                return {"analysis_guide": None}

            prompt = (
                "당신은 데이터 분석 전문가입니다. 사용자의 질문과 제공된 컨텍스트(테이블 스키마, 용어집, 쿼리 예제)를 바탕으로 "
                "데이터 분석 시나리오(Analysis Guide)를 작성해주세요.\n\n"
                "다음 우선순위에 따라 분석 전략을 수립하세요:\n"
                "1. Query Example 활용: 유사한 쿼리 예제가 있다면 이를 변형하여 분석하는 방법을 제안하세요.\n"
                "2. Glossary 활용: 질문에 포함된 모호한 용어가 용어집에 있다면 그 정의를 바탕으로 분석 방법을 제안하세요.\n"
                "3. Table Schema 활용: 위 정보가 부족하다면 테이블 스키마를 보고 어떤 컬럼을 조합하여 분석할지 제안하세요.\n\n"
                f"# 사용자 질문: {user_text}\n\n"
                f"# 테이블 스키마 정보: {table_outs}\n"
                f"# 용어집 정보: {glossary_outs}\n"
                f"# 쿼리 예제 정보: {query_outs}\n\n"
                "분석 가이드는 명확하고 논리적인 단계로 작성해주세요."
            )

            response = self.llm.invoke([HumanMessage(content=prompt)])
            return {"analysis_guide": response.content}

        def call_model(state: ChatBotState):
            selected_ids = state.get("selected_ids", [])

            # 각 출력 필드 가져오기
            table_outs = state.get("table_schema_outputs", [])
            glossary_outs = state.get("glossary_outputs", [])
            query_outs = state.get("query_example_outputs", [])
            analysis_guide = state.get("analysis_guide")

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

            # 분석 가이드 추가
            analysis_guide_text = ""
            if analysis_guide:
                analysis_guide_text = (
                    f"\n\n# 분석 가이드 (Analysis Guide)\n{analysis_guide}"
                )

            sys_msg = SystemMessage(
                content=(
                    "# 역할\n"
                    "당신은 사용자의 비즈니스 질문을 구체적인 데이터 분석 시나리오로 연결해주는 '데이터 분석 컨설턴트'입니다.\n"
                    "단순히 질문을 구체화하는 것을 넘어, 제공된 데이터 자산(테이블, 용어, 쿼리)을 활용하여 '어떤 데이터를 어떻게 분석하면 답을 얻을 수 있는지'를 전문적으로 가이드해야 합니다.\n"
                    "# 적용된 가이드라인\n"
                    + "\n".join(guideline_lines)
                    + "\n\n# 툴 실행 결과 (참고 정보)\n"
                    + "\n".join(all_tool_lines)
                    + analysis_guide_text
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
        workflow.add_node("filter", filter_context)
        workflow.add_node("generate_analysis_guide", generate_analysis_guide)
        workflow.add_node("respond", call_model)

        workflow.add_edge(START, "select")
        workflow.add_edge("select", "filter")
        workflow.add_edge("filter", "generate_analysis_guide")
        workflow.add_edge("generate_analysis_guide", "respond")
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
        input_state = {
            "messages": [HumanMessage(content=message)],
            "context": {"gms_server": self.gms_server},
            "selected_ids": [],
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
