"""
Clarification Tools

질문 명확화를 위한 도구들
"""

from typing import Annotated
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState

from utils.llm.chains import question_gate_chain


@tool
def validate_question_tool(state: Annotated[dict, InjectedState]) -> str:
    """
    질문이 SQL 쿼리 생성에 충분한지 검증합니다.

    state의 messages에서 최신 사용자 메시지를 자동으로 추출하여 검증합니다.

    Args:
        state: 현재 상태 (InjectedState로 자동 주입)

    Returns:
        str: 검증 결과 메시지
    """
    try:
        # state에서 최신 사용자 메시지 추출
        question = None
        messages = state.get("messages", [])

        # 역순으로 탐색하여 가장 최근 사용자 메시지 찾기
        for msg in reversed(messages):
            if isinstance(msg, dict) and msg.get("role") == "user":
                question = msg.get("content", "")
                break
            elif hasattr(msg, "type") and msg.type == "human":
                question = msg.content
                break

        if not question:
            question = state.get("original_question", "")

        print(f"[DEBUG] validate_question_tool - 검증할 질문: {question}")

        # Question Gate 체인 실행
        result = question_gate_chain.invoke({"question": question})

        # 결과 분석
        is_passed = (
            len(result.missing_entities) == 0 and not result.requires_data_science
        )

        # 상태 업데이트
        state["is_question_complete"] = is_passed
        state["missing_info"] = result.missing_entities
        state["clarified_question"] = question if is_passed else ""

        if is_passed:
            return f"✅ 질문이 완벽합니다! SQL로 처리 가능합니다.\n질문: {question}"

        if result.requires_data_science:
            return f"⚠️ 이 질문은 SQL로 처리하기 어렵습니다. (통계/ML 분석 필요)\n이유: {result.reason}"

        return f"🔍 질문이 불완전합니다.\n이유: {result.reason}\n누락된 정보: {', '.join(result.missing_entities)}"

    except Exception as e:
        return f"❌ 질문 검증 중 오류 발생: {str(e)}"


@tool
def suggest_missing_entities_tool(state: Annotated[dict, InjectedState]) -> str:
    """
    질문에서 누락된 엔티티를 제안합니다.

    state의 missing_info를 활용하여 제안합니다.

    Args:
        state: 현재 상태

    Returns:
        str: 누락된 엔티티 제안 메시지
    """
    try:
        # state에서 누락 정보 가져오기
        missing_entities = state.get("missing_info", [])

        print(f"[DEBUG] suggest_missing_entities_tool - 누락: {missing_entities}")

        if not missing_entities:
            return "✅ 모든 필수 정보가 포함되어 있습니다."

        suggestions = []
        suggestions.append(
            f"💡 다음 정보를 추가해주세요: {', '.join(missing_entities)}"
        )
        suggestions.append("\n📝 예시:")

        # 누락 엔티티별 예시 제공
        if "시간" in missing_entities or "기간" in missing_entities:
            suggestions.append("  - '2024년 1월'")
            suggestions.append("  - '최근 3개월'")
            suggestions.append("  - '2023년 Q4'")

        if "지역" in missing_entities or "장소" in missing_entities:
            suggestions.append("  - '서울 지역'")
            suggestions.append("  - '전국'")

        if "측정값" in missing_entities or "지표" in missing_entities:
            suggestions.append("  - '매출'")
            suggestions.append("  - '주문 수'")
            suggestions.append("  - '평균 금액'")

        return "\n".join(suggestions)

    except Exception as e:
        return f"❌ 엔티티 제안 중 오류 발생: {str(e)}"
