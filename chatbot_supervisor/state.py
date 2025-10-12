"""
Supervisor State Definition

Supervisor와 Agent들이 공유하는 상태 정의
"""

from typing import Optional
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages, MessagesState


class SupervisorState(MessagesState):
    """
    Supervisor와 Agent들이 공유하는 상태

    MessagesState를 상속받아 messages 필드를 자동으로 관리합니다.
    messages는 add_messages reducer를 사용하여 대화 이력을 누적합니다.
    """

    # 질문 관련
    original_question: str  # 사용자의 원본 질문
    clarified_question: str  # 명확화된 질문
    is_question_complete: bool  # 질문이 완전한지 여부
    missing_info: list[str]  # 누락된 정보 목록

    # 검색 및 프로파일링
    searched_tables: dict[str, dict[str, str]]  # 검색된 테이블 메타데이터
    question_profile: dict  # 질문 프로파일 (시계열, 집계 등)

    # SQL 관련
    enriched_question: str  # 테이블 정보로 보강된 질문
    generated_sql: str  # 생성된 SQL 쿼리
    sql_validation_result: dict  # SQL 검증 결과

    # 실행 결과
    sql_result: Optional[object]  # SQL 실행 결과 (DataFrame)
    execution_error: Optional[str]  # 실행 에러 메시지

    # 시각화
    chart_code: str  # 시각화 코드
    formatted_output: str  # 포맷팅된 출력

    # 환경 설정
    database_env: str  # 데이터베이스 환경 (clickhouse, postgres 등)
    retriever_config: dict  # 검색 설정 (retriever_name, top_n, device)

    # Dialect 정보
    dialect_name: str  # SQL Dialect 이름
    supports_ilike: bool  # ILIKE 지원 여부
    dialect_hints: list[str]  # Dialect별 힌트

    # Agent 간 통신 (선택적)
    current_agent: str  # 현재 활성 Agent
    conversation_stage: (
        str  # 대화 단계 ("clarification" | "building" | "execution" | "reporting")
    )

    # 에러 추적 (human-in-the-loop 판단용)
    error_count: int  # 연속 에러 발생 횟수
    last_error_message: str  # 마지막 에러 메시지
    incomplete_question_count: int  # 질문 불완전 판단 횟수
