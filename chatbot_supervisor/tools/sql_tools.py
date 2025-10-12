"""
SQL Tools

SQL 생성 및 실행을 위한 도구들
"""

import json
import re
from typing import Annotated
from langchain_core.tools import tool
from langgraph.prebuilt import InjectedState

from utils.llm.chains import query_maker_chain
from utils.llm.llm_response_parser import LLMResponseParser
from utils.databases.factory import DatabaseFactory


@tool
def generate_sql_tool(question: str, state: Annotated[dict, InjectedState]) -> str:
    """
    질문과 테이블 정보를 바탕으로 SQL 쿼리를 생성합니다.

    Args:
        question: SQL을 생성할 질문 (보강된 질문 권장)
        state: 현재 상태 (테이블 정보, 데이터베이스 환경 포함)

    Returns:
        str: 생성된 SQL 쿼리 또는 오류 메시지
    """
    try:
        # 검색된 테이블 정보
        searched_tables = state.get("searched_tables", {})
        if not searched_tables:
            return "❌ 테이블 정보가 없습니다. 먼저 search_tables_tool을 실행해주세요."

        # 데이터베이스 환경
        database_env = state.get("database_env", "clickhouse")

        # Dialect 정보
        dialect_name = state.get("dialect_name", "")
        supports_ilike = state.get("supports_ilike", False)
        dialect_hints = state.get("dialect_hints", [])

        # 테이블 정보를 JSON으로 변환
        searched_tables_json = json.dumps(searched_tables, ensure_ascii=False, indent=2)
        dialect_hints_str = "\n".join(dialect_hints) if dialect_hints else ""

        # SQL 생성
        result = query_maker_chain.invoke(
            {
                "user_input": question,
                "user_database_env": database_env,
                "searched_tables": searched_tables_json,
                "dialect_name": dialect_name or "standard",
                "supports_ilike": supports_ilike,
                "dialect_hints": dialect_hints_str,
            }
        )

        # SQL 추출
        result_content = result.content if hasattr(result, "content") else str(result)

        try:
            sql = LLMResponseParser.extract_sql(result_content)
        except ValueError:
            # <SQL> 태그가 없으면 코드 블록 추출 시도
            sql_match = re.search(r"```sql\n(.*?)```", result_content, re.DOTALL)
            if sql_match:
                sql = sql_match.group(1).strip()
            else:
                # 코드 블록도 없으면 전체를 SQL로 간주
                sql = result_content.strip()

        # 상태에 SQL 저장
        state["generated_sql"] = sql

        return f"✅ SQL 생성 완료:\n\n```sql\n{sql}\n```"

    except Exception as e:
        return f"❌ SQL 생성 중 오류 발생: {str(e)}"


@tool
def execute_sql_tool(state: Annotated[dict, InjectedState]) -> str:
    """
    생성된 SQL 쿼리를 데이터베이스에서 실행합니다.

    Args:
        state: 현재 상태 (generated_sql, database_env 포함)

    Returns:
        str: 실행 결과 메시지
    """
    try:
        # 생성된 SQL 가져오기
        sql = state.get("generated_sql", "")
        if not sql:
            return "❌ 실행할 SQL이 없습니다. 먼저 generate_sql_tool을 실행해주세요."

        # 데이터베이스 환경
        database_env = state.get("database_env", "clickhouse")

        # 데이터베이스 커넥터 가져오기
        db = DatabaseFactory.get_connector(db_type=database_env)

        # SQL 실행
        result = db.run_sql(sql)

        # 상태에 결과 저장 (DataFrame을 dict로 변환하여 직렬화 가능하게)
        if result is not None:
            state["sql_result"] = {
                "data": result.to_dict("records"),
                "columns": result.columns.tolist(),
                "index": result.index.tolist(),
            }
        else:
            state["sql_result"] = None
        state["execution_error"] = None

        # 결과 메시지 생성
        row_count = len(result) if result is not None else 0

        if row_count == 0:
            return "⚠️ SQL 실행은 성공했지만 결과가 비어있습니다."

        # 결과 미리보기 (최대 5행)
        preview = result.head(5).to_string(index=False) if row_count > 0 else ""

        return f"✅ SQL 실행 완료!\n\n총 {row_count}개의 행이 반환되었습니다.\n\n미리보기:\n{preview}"

    except Exception as e:
        error_msg = str(e)
        state["execution_error"] = error_msg
        return f"❌ SQL 실행 중 오류 발생: {error_msg}"
