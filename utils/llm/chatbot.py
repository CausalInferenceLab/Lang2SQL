"""
LangGraph 기반 ChatBot 모델
OpenAI의 ChatGPT 모델을 사용하여 대화 기록을 유지하는 챗봇 구현
"""

from typing import Annotated, Sequence, TypedDict

from langchain_core.messages import BaseMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from utils.llm.tools import (
    search_database_tables,
    get_glossary_terms,
    get_query_examples,
)


class ChatBotState(TypedDict):
    """
    챗봇 상태 - 사용자 질문을 SQL로 변환 가능한 구체적인 질문으로 만들어가는 과정 추적
    """

    # 기본 메시지 (MessagesState와 동일)
    messages: Annotated[Sequence[BaseMessage], add_messages]

    # datahub 서버 정보
    gms_server: str


class ChatBot:
    """
    LangGraph를 사용한 대화형 챗봇 클래스
    OpenAI API를 통해 다양한 GPT 모델을 사용할 수 있으며,
    MemorySaver를 통해 대화 기록을 관리합니다.
    """

    def __init__(
        self,
        openai_api_key: str,
        model_name: str = "gpt-4o-mini",
        gms_server: str = "http://localhost:8080",
    ):
        """
        ChatBot 인스턴스 초기화

        Args:
            openai_api_key: OpenAI API 키
            model_name: 사용할 모델명 (기본값: gpt-4o-mini)
            gms_server: DataHub GMS 서버 URL (기본값: http://localhost:8080)
        """
        self.openai_api_key = openai_api_key
        self.model_name = model_name
        self.gms_server = gms_server
        # SQL 생성을 위한 데이터베이스 메타데이터 조회 도구
        self.tools = [
            search_database_tables,  # 데이터베이스 테이블 정보 검색
            get_glossary_terms,  # 용어집 조회 도구
            get_query_examples,  # 쿼리 예제 조회 도구
        ]
        self.llm = self._setup_llm()  # LLM 인스턴스 설정
        self.app = self._setup_workflow()  # LangGraph 워크플로우 설정

    def _setup_llm(self):
        """
        OpenAI ChatGPT LLM 인스턴스 생성
        Tool을 바인딩하여 LLM이 필요시 tool을 호출할 수 있도록 설정합니다.

        Returns:
            ChatOpenAI: Tool이 바인딩된 LLM 인스턴스
        """
        llm = ChatOpenAI(
            temperature=0.0,  # SQL 생성은 정확성이 중요하므로 0으로 설정
            openai_api_key=self.openai_api_key,
            model_name=self.model_name,
        )
        # Tool을 LLM에 바인딩하여 함수 호출 기능 활성화
        return llm.bind_tools(self.tools)

    def _setup_workflow(self):
        """
        LangGraph 워크플로우 설정
        대화 기록을 관리하고 LLM과 통신하는 그래프 구조를 생성합니다.
        Tool 호출 기능을 포함하여 LLM이 필요시 도구를 사용할 수 있도록 합니다.

        Returns:
            CompiledGraph: 컴파일된 LangGraph 워크플로우
        """
        # ChatBotState를 사용하는 StateGraph 생성
        workflow = StateGraph(state_schema=ChatBotState)

        def call_model(state: ChatBotState):
            """
            LLM 모델을 호출하는 노드 함수
            LLM이 응답을 생성하거나 tool 호출을 결정합니다.

            Args:
                state: 현재 메시지 상태

            Returns:
                dict: LLM 응답이 포함된 상태 업데이트
            """
            # 질문 구체화 전문 어시스턴트 시스템 메시지
            sys_msg = SystemMessage(content="""# 역할
당신은 사용자의 모호한 질문을 명확하고 구체적인 질문으로 만드는 전문 AI 어시스턴트입니다.

# 주요 임무
- 사용자의 자연어 질문을 이해하고 의도를 정확히 파악합니다
- 대화를 통해 날짜, 지표, 필터 조건 등 구체적인 정보를 수집합니다
- 단계별로 사용자와 대화하며 명확하고 구체적인 질문으로 다듬어갑니다

# 작업 프로세스
1. 사용자의 최초 질문에서 의도 파악
2. 질문을 명확히 하기 위해 필요한 정보 식별 (날짜, 지표, 대상, 조건 등)
3. **도구를 적극 활용하여 데이터베이스 스키마, 테이블 정보, 용어집 등을 확인**
4. 부족한 정보를 자연스럽게 질문하여 수집
5. 수집된 정보를 바탕으로 질문을 점진적으로 구체화
6. 충분히 구체화되면 최종 질문 확정

# 도구 사용 가이드
- **search_database_tables**: 사용자와의 대화를 데이터와 연관짓기 위해 관련 테이블을 적극적으로 확인할 수 있는 도구
- **get_glossary_terms**: 사용자가 사용한 용어의 정확한 의미를 확인할 때 사용가능한 도구
- **get_query_examples**: 조직내 저장된 쿼리 예제를 조회하여 참고할 수 있는 도구
- 답변하기 전에 최대한 많은 도구를 적극 활용하여 정보를 수집하세요
- 불확실한 정보가 있다면 추측하지 말고 도구를 사용하여 확인하세요

# 예시
- 모호한 질문: "KPI가 궁금해"
- 대화 후 구체화: "2025-01-02 날짜의 신규 유저가 발생시킨 매출이 궁금해"

# 주의사항
- 항상 친절하고 명확하게 대화합니다
- 이전 대화 맥락을 고려하여 일관성 있게 응답합니다
- 한 번에 너무 많은 것을 물어보지 않고 단계적으로 진행합니다
- **중요: 사용자가 말한 내용이 충분히 구체화되지 않거나 의도가 명확히 파악되지 않을 경우, 추측하지 말고 모든 도구(get_glossary_terms, get_query_examples, search_database_tables)를 적극적으로 사용하여 맥락을 파악하세요**
- 도구를 통해 수집한 정보를 바탕으로 사용자에게 구체적인 방향성과 옵션을 제안하세요
- 불확실한 정보가 있다면 추측하지 말고 도구를 사용하여 확인한 후 답변하세요

---
다음은 사용자와의 대화입니다:""")
            # 시스템 메시지를 대화의 맨 앞에 추가
            messages = [sys_msg] + state["messages"]
            response = self.llm.invoke(messages)
            return {"messages": response}

        def route_model_output(state: ChatBotState):
            """
            LLM 출력에 따라 다음 노드를 결정하는 라우팅 함수
            Tool 호출이 필요한 경우 'tools' 노드로, 아니면 대화를 종료합니다.

            Args:
                state: 현재 메시지 상태

            Returns:
                str: 다음에 실행할 노드 이름 ('tools' 또는 '__end__')
            """
            messages = state["messages"]
            last_message = messages[-1]
            # LLM이 tool을 호출하려고 하는 경우 (tool_calls가 있는 경우)
            if hasattr(last_message, "tool_calls") and last_message.tool_calls:
                return "tools"
            # Tool 호출이 없으면 대화 종료
            return "__end__"

        # 워크플로우 구조 정의
        workflow.add_edge(START, "model")  # 시작 -> model 노드
        workflow.add_node("model", call_model)  # LLM 호출 노드
        workflow.add_node("tools", ToolNode(self.tools))  # Tool 실행 노드

        # model 노드 이후 조건부 라우팅
        workflow.add_conditional_edges("model", route_model_output)
        # Tool 실행 후 다시 model로 돌아가서 최종 응답 생성
        workflow.add_edge("tools", "model")

        # MemorySaver를 사용하여 대화 기록 저장 기능 추가
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

        # 상태 준비
        input_state = {
            "messages": [{"role": "user", "content": message}],
            "gms_server": self.gms_server,  # DataHub 서버 URL을 상태에 포함
        }

        return self.app.invoke(input_state, config)

    def update_model(self, model_name: str):
        """
        사용 중인 LLM 모델 변경
        모델 변경 시 LLM 인스턴스와 워크플로우를 재설정합니다.

        Args:
            model_name: 변경할 모델명
        """
        self.model_name = model_name
        self.llm = self._setup_llm()  # 새 모델로 LLM 재설정
        self.app = self._setup_workflow()  # 워크플로우 재생성
