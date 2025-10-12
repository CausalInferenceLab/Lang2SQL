"""
Visualization Tools

데이터 시각화 및 포맷팅을 위한 도구들
"""

import pandas as pd
from typing import Annotated
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState


@tool
def format_table_tool(state: Annotated[dict, InjectedState]) -> str:
    """
    SQL 실행 결과를 사용자 친화적인 표 형식으로 포맷팅합니다.

    Args:
        state: 현재 상태 (sql_result 포함)

    Returns:
        str: 포맷팅된 결과 메시지
    """
    try:
        # SQL 결과 가져오기 (dict 형태로 저장되어 있음)
        sql_result_dict = state.get("sql_result")

        if sql_result_dict is None:
            return "❌ 포맷팅할 결과가 없습니다. 먼저 SQL을 실행해주세요."

        # dict를 DataFrame으로 변환
        sql_result = pd.DataFrame(sql_result_dict["data"])

        if len(sql_result) == 0:
            return "⚠️ 결과가 비어있습니다."

        # DataFrame을 문자열로 변환 (최대 20행)
        row_count = len(sql_result)
        preview_rows = min(20, row_count)

        formatted = sql_result.head(preview_rows).to_string(index=False)

        # 상태에 저장
        state["formatted_output"] = formatted

        result_message = [
            f"📊 결과 요약:",
            f"- 총 {row_count}개의 행",
            f"- {len(sql_result.columns)}개의 컬럼: {', '.join(sql_result.columns)}",
            "",
            "결과 미리보기:",
            "```",
            formatted,
            "```",
        ]

        if row_count > preview_rows:
            result_message.append(f"\n(전체 {row_count}개 중 {preview_rows}개만 표시)")

        return "\n".join(result_message)

    except Exception as e:
        return f"❌ 포맷팅 중 오류 발생: {str(e)}"


@tool
def create_chart_tool(question: str, state: Annotated[dict, InjectedState]) -> str:
    """
    SQL 실행 결과를 바탕으로 시각화 코드를 생성합니다.

    Args:
        question: 원본 질문 (시각화 타입 결정에 활용)
        state: 현재 상태 (sql_result, generated_sql 포함)

    Returns:
        str: 시각화 코드 또는 메시지
    """
    try:
        # SQL 결과 가져오기 (dict 형태로 저장되어 있음)
        sql_result_dict = state.get("sql_result")

        if sql_result_dict is None:
            return "⚠️ 시각화할 데이터가 없습니다."

        # dict를 DataFrame으로 변환
        sql_result = pd.DataFrame(sql_result_dict["data"])

        if len(sql_result) == 0:
            return "⚠️ 시각화할 데이터가 없습니다."

        # 시각화 모듈 임포트
        try:
            from utils.visualization.display_chart import DisplayChart
        except ImportError:
            return (
                "⚠️ 시각화 모듈을 찾을 수 없습니다. (utils.visualization.display_chart)"
            )

        # 생성된 SQL
        generated_sql = state.get("generated_sql", "")

        # 시각화 생성
        chart = DisplayChart(
            question=question,
            sql=generated_sql,
            df_metadata=str(sql_result.dtypes.to_dict()),
        )

        # Plotly 코드 생성
        code = chart.generate_plotly_code()

        # 상태에 저장
        state["chart_code"] = code

        return f"📈 시각화 코드 생성 완료!\n\n```python\n{code}\n```"

    except Exception as e:
        return f"⚠️ 시각화 생성 실패: {str(e)}"
