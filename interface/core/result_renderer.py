"""
Lang2SQL 결과 표시 모듈.

이 모듈은 LLM이 생성한 SQL 쿼리 및 결과 데이터를
Streamlit UI를 통해 다양한 형태(쿼리, 표, 차트, 설명 등)로 표시합니다.
토큰 사용량, 문서 적합성 평가, 재해석된 질문 등도 함께 확인할 수 있습니다.
"""

import pandas as pd
import streamlit as st
from langchain_core.messages import AIMessage

from infra.observability.token_usage import TokenUtils
from utils.databases import DatabaseFactory
from utils.llm.llm_response_parser import LLMResponseParser
from utils.visualization.display_chart import DisplayChart


def display_result(res: dict) -> None:
    """Lang2SQL 실행 결과를 Streamlit UI로 출력합니다.

    Args:
        res (dict): Lang2SQL 실행 결과를 담은 딕셔너리.
            - generated_query (AIMessage | str): LLM이 생성한 SQL 쿼리
            - messages (list): LLM 입력/출력 메시지 목록
            - question_gate_result (dict, optional): 질문 게이트 결과
            - document_suitability (dict, optional): 문서 적합성 평가 결과
            - searched_tables (list, optional): 검색된 테이블 목록

    표시 항목:
        - SQL 쿼리 및 실행 결과
        - 결과 설명 및 재해석된 질문
        - 문서 적합성 평가 및 질문 게이트 결과
        - 토큰 사용량 요약
        - 쿼리 결과 표 및 차트
    """

    def should_show(_key: str) -> bool:
        return st.session_state.get(_key, True)

    has_query = bool(res.get("generated_query"))
    show_sql_section = has_query and should_show("show_sql")
    show_result_desc = has_query and should_show("show_result_description")
    show_reinterpreted = has_query and should_show("show_question_reinterpreted_by_ai")
    show_gate_result = should_show("show_question_gate_result")
    show_doc_suitability = should_show("show_document_suitability")
    show_table_section = has_query and should_show("show_table")
    show_chart_section = has_query and should_show("show_chart")

    if show_gate_result and ("question_gate_result" in res):
        st.markdown("---")
        st.markdown("**Question Gate 결과:**")
        st.json(res.get("question_gate_result", {}))

    if show_doc_suitability and ("document_suitability" in res):
        st.markdown("---")
        st.markdown("**문서 적합성 평가:**")
        ds = res.get("document_suitability")
        if isinstance(ds, dict) and ds:
            rows = [
                {
                    "table": t,
                    "score": float(info.get("score", -1)),
                    "matched_columns": ", ".join(info.get("matched_columns", [])),
                    "missing_entities": ", ".join(info.get("missing_entities", [])),
                    "reason": info.get("reason", ""),
                }
                for t, info in ds.items()
                if isinstance(info, dict)
            ]
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("문서 적합성 평가 결과가 비어 있습니다.")

    if should_show("show_token_usage"):
        st.markdown("---")
        token_summary = TokenUtils.get_token_usage_summary(data=res["messages"])
        st.write("**토큰 사용량:**")
        st.markdown(f"""
            - Input tokens: `{token_summary['input_tokens']}`
            - Output tokens: `{token_summary['output_tokens']}`
            - Total tokens: `{token_summary['total_tokens']}`
            """)

    if show_sql_section:
        st.markdown("---")
        generated_query = res.get("generated_query")
        if generated_query:
            query_text = (
                generated_query.content
                if isinstance(generated_query, AIMessage)
                else str(generated_query)
            )
            try:
                sql = LLMResponseParser.extract_sql(query_text)
                st.markdown("**생성된 SQL 쿼리:**")
                st.code(sql, language="sql")
            except ValueError:
                st.warning("SQL 블록을 추출할 수 없습니다.")
                st.text(query_text)
            interpretation = LLMResponseParser.extract_interpretation(query_text)
            if interpretation:
                st.markdown("**결과 해석:**")
                st.code(interpretation)
        else:
            st.warning("쿼리 텍스트가 문자열이 아닙니다.")
            st.text(str(query_text))

    if show_result_desc and res.get("messages"):
        st.markdown("---")
        st.markdown("**결과 설명:**")
        result_message = res["messages"][-1].content

        if isinstance(result_message, str):
            try:
                sql = LLMResponseParser.extract_sql(result_message)
                st.code(sql, language="sql")
            except ValueError:
                st.warning("SQL 블록을 추출할 수 없습니다.")
                st.text(result_message)

            interpretation = LLMResponseParser.extract_interpretation(result_message)
            if interpretation:
                st.code(interpretation, language="plaintext")
        else:
            st.warning("결과 메시지가 문자열이 아닙니다.")
            st.text(str(result_message))

    if show_reinterpreted and res.get("messages"):
        st.markdown("---")
        st.markdown("**AI가 재해석한 사용자 질문:**")
        try:
            if len(res["messages"]) > 1:
                candidate = res["messages"][-2]
                question_text = (
                    candidate.content
                    if hasattr(candidate, "content")
                    else str(candidate)
                )
            else:
                question_text = res["messages"][0].content
        except Exception:
            question_text = str(res["messages"][0].content)
        st.code(question_text)

    if should_show("show_referenced_tables"):
        st.markdown("---")
        st.markdown("**참고한 테이블 목록:**")
        st.write(res.get("searched_tables", []))

    if not has_query:
        st.info("QUERY_MAKER 없이 실행되었습니다. 검색된 테이블 정보만 표시합니다.")

    if show_table_section or show_chart_section:
        database = DatabaseFactory.get_connector()
        df = pd.DataFrame()
        try:
            sql_raw = (
                res["generated_query"].content
                if isinstance(res["generated_query"], AIMessage)
                else str(res["generated_query"])
            )
            if isinstance(sql_raw, str):
                sql = LLMResponseParser.extract_sql(sql_raw)
                df = database.run_sql(sql)
            else:
                st.error("SQL 원본이 문자열이 아닙니다.")
        except Exception as e:
            st.markdown("---")
            st.error(f"쿼리 실행 중 오류 발생: {e}")
            df = pd.DataFrame()

        if not df.empty and show_table_section:
            st.markdown("---")
            st.markdown("**쿼리 실행 결과:**")
            try:
                st.dataframe(df.head(10) if len(df) > 10 else df)
            except Exception as e:
                st.error(f"결과 테이블 생성 중 오류 발생: {e}")

        if df is not None and show_chart_section:
            st.markdown("---")
            try:
                st.markdown("**쿼리 결과 시각화:**")
                try:
                    if len(res["messages"]) > 1:
                        candidate = res["messages"][-2]
                        chart_question = (
                            candidate.content
                            if hasattr(candidate, "content")
                            else str(candidate)
                        )
                    else:
                        chart_question = res["messages"][0].content
                except Exception:
                    chart_question = str(res["messages"][0].content)

                display_code = DisplayChart(
                    question=chart_question,
                    sql=sql,
                    df_metadata=f"Running df.dtypes gives:\n{df.dtypes}",
                )
                # plotly_code 변수도 따로 보관할 필요 없이 바로 그려도 됩니다
                fig = display_code.get_plotly_figure(
                    plotly_code=display_code.generate_plotly_code(), df=df
                )
                st.plotly_chart(fig)
            except Exception as e:
                st.error(f"차트 생성 중 오류 발생: {e}")
