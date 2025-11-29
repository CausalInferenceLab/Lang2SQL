import streamlit as st
import json


def render_context_cards(context):
    """
    검색된 컨텍스트(테이블 스키마, 용어집, 쿼리 예제)를 가로 스크롤 가능한 카드 형태로 렌더링합니다.
    """
    if not context:
        return

    cards_html = ""

    # 카드 HTML 생성 헬퍼 함수
    def create_card(title, icon, content_html, color_border):
        return f"""<div style="min-width: 300px; max-width: 350px; border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 8px; padding: 15px; margin-bottom: 5px; background-color: rgba(255, 255, 255, 0.05); border-left: 5px solid {color_border}; box-shadow: 0 2px 4px rgba(0,0,0,0.05); display: flex; flex-direction: column; flex-shrink: 0;">
            <div style="font-weight: bold; margin-bottom: 8px; display: flex; align-items: center; font-size: 1rem;">
                <span style="margin-right: 8px;">{icon}</span> {title}
            </div>
            <div style="font-size: 0.85rem; opacity: 0.9; overflow-y: auto; max-height: 200px; font-family: monospace; line-height: 1.4;">
                {content_html}
            </div>
        </div>"""

    # 1. 테이블 스키마 처리
    if "table_schema_outputs" in context:
        for output in context["table_schema_outputs"]:
            if isinstance(output, dict):
                # 에러 처리
                if output.get("error"):
                    cards_html += create_card(
                        "Error", "⚠️", f"오류: {output.get('message')}", "#F44336"
                    )
                    continue

                # 테이블별로 카드 생성
                for table_name, table_info in output.items():
                    if not isinstance(table_info, dict):
                        continue

                    desc = table_info.get("table_description", "설명 없음")
                    columns_html = "<div style='margin-top: 5px; font-size: 0.8em; color: #aaa;'>컬럼:</div>"
                    columns_html += "<ul style='margin: 0; padding-left: 15px;'>"

                    for col_name, col_desc in table_info.items():
                        if col_name == "table_description":
                            continue
                        columns_html += f"<li><b>{col_name}</b>: {col_desc}</li>"
                    columns_html += "</ul>"

                    content = f"<div><b>{table_name}</b></div><div style='margin-bottom: 5px;'>{desc}</div>{columns_html}"
                    cards_html += create_card("Table Schema", "🗃️", content, "#4CAF50")
            else:
                # 문자열이나 기타 타입인 경우
                cards_html += create_card("Table Schema", "🗃️", str(output), "#4CAF50")

    # 2. 용어집 처리
    if "glossary_outputs" in context:
        for output in context["glossary_outputs"]:
            if isinstance(output, list):
                for term in output:
                    if isinstance(term, dict):
                        name = term.get("name", "이름 없음")
                        desc = term.get("description", "설명 없음")
                        content = f"<div><b>{name}</b></div><div>{desc}</div>"
                        cards_html += create_card("Glossary", "📚", content, "#2196F3")
                    else:
                        cards_html += create_card(
                            "Glossary", "📚", str(term), "#2196F3"
                        )
            elif isinstance(output, dict) and output.get("error"):
                cards_html += create_card(
                    "Error", "⚠️", f"오류: {output.get('message')}", "#F44336"
                )
            else:
                cards_html += create_card("Glossary", "📚", str(output), "#2196F3")

    # 3. 쿼리 예제 처리
    if "query_example_outputs" in context:
        for output in context["query_example_outputs"]:
            if isinstance(output, list):
                for example in output:
                    if isinstance(example, dict):
                        name = example.get("name", "예제")
                        desc = example.get("description", "")
                        sql = example.get("statement", "")

                        content = f"<div><b>{name}</b></div>"
                        if desc:
                            content += f"<div style='margin-bottom: 5px; font-size: 0.8em;'>{desc}</div>"
                        content += f"<div style='background: rgba(0,0,0,0.2); padding: 5px; border-radius: 4px; overflow-x: auto;'><code>{sql}</code></div>"

                        cards_html += create_card(
                            "Query Example", "💡", content, "#FF9800"
                        )
                    else:
                        cards_html += create_card(
                            "Query Example", "💡", str(example), "#FF9800"
                        )
            elif isinstance(output, dict) and output.get("error"):
                cards_html += create_card(
                    "Error", "⚠️", f"오류: {output.get('message')}", "#F44336"
                )
            else:
                cards_html += create_card("Query Example", "💡", str(output), "#FF9800")

    if not cards_html:
        return

    # 가로 스크롤 컨테이너 렌더링
    st.markdown(
        f"""<div style="display: flex; overflow-x: auto; gap: 15px; padding: 10px 5px; margin-top: 10px; margin-bottom: 10px; scrollbar-width: thin;">{cards_html}</div>""",
        unsafe_allow_html=True,
    )
