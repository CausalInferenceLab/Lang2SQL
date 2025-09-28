import re
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
import pandas as pd
import os

import plotly
import plotly.express as px
import plotly.graph_objects as go


class DisplayChart:
    """
    SQL쿼리가 실행된 결과를 그래프로 시각화하는 Class입니다.

    쿼리 결과를 비롯한 유저 질문, sql를 prompt에 입력하여
    plotly코드를 출력하여 excute한 결과를 fig 객체로 반환합니다.
    """

    def __init__(self, question, sql, df_metadata):
        self.question = question
        self.sql = sql
        self.df_metadata = df_metadata

    def llm_model_for_chart(self, message_log):
        provider = os.getenv("LLM_PROVIDER")
        if provider == "openai":
            llm = ChatOpenAI(
                model=os.getenv("OPEN_AI_LLM_MODEL", "gpt-4o"),
                api_key=os.getenv("OPEN_AI_KEY"),
            )
            result = llm.invoke(message_log)
            return result

    def _extract_python_code(self, markdown_string: str) -> str:
        # Strip whitespace to avoid indentation errors in LLM-generated code
        markdown_string = markdown_string.content.split("```")[1][6:].strip()

        # Regex pattern to match Python code blocks
        pattern = r"```[\w\s]*python\n([\s\S]*?)```|```([\s\S]*?)```"  # 여러 문자와 공백 뒤에 python이 나오고, 줄바꿈 이후의 모든 내용

        # Find all matches in the markdown string
        matches = re.findall(
            pattern, markdown_string, re.IGNORECASE
        )  # 대소문자 구분 안함

        # Extract the Python code from the matches
        python_code = []
        for match in matches:
            python = match[0] if match[0] else match[1]
            python_code.append(python.strip())

        if len(python_code) == 0:
            return markdown_string

        return python_code[0]

    def _sanitize_plotly_code(self, raw_plotly_code):
        # Remove the fig.show() statement from the plotly code
        plotly_code = raw_plotly_code.replace("fig.show()", "")

        return plotly_code

    def generate_plotly_code(self) -> str:
        if self.question is not None:
            system_msg = f"The following is a pandas DataFrame that contains the results of the query that answers the question the user asked: '{self.question}'"
        else:
            system_msg = "The following is a pandas DataFrame "

        if self.sql is not None:
            system_msg += (
                f"\n\nThe DataFrame was produced using this query: {self.sql}\n\n"
            )

        system_msg += f"The following is information about the resulting pandas DataFrame 'df': \n{self.df_metadata}"

        message_log = [
            SystemMessage(content=system_msg),
            HumanMessage(
                content="Can you generate the Python plotly code to chart the results of the dataframe? Assume the data is in a pandas dataframe called 'df'. If there is only one value in the dataframe, use an Indicator. Respond with only Python code. Do not answer with any explanations -- just the code."
            ),
        ]

        plotly_code = self.llm_model_for_chart(message_log)

        return self._sanitize_plotly_code(self._extract_python_code(plotly_code))

    def get_plotly_figure(
        self, plotly_code: str, df: pd.DataFrame, dark_mode: bool = True
    ) -> plotly.graph_objs.Figure:

        ldict = {"df": df, "px": px, "go": go}
        try:
            exec(plotly_code, globals(), ldict)
            fig = ldict.get("fig", None)

        except Exception as e:

            # Inspect data types
            numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
            categorical_cols = df.select_dtypes(
                include=["object", "category"]
            ).columns.tolist()

            # Decision-making for plot type
            if len(numeric_cols) >= 2:
                # Use the first two numeric columns for a scatter plot
                fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1])
            elif len(numeric_cols) == 1 and len(categorical_cols) >= 1:
                # Use a bar plot if there's one numeric and one categorical column
                fig = px.bar(df, x=categorical_cols[0], y=numeric_cols[0])
            elif len(categorical_cols) >= 1 and df[categorical_cols[0]].nunique() < 10:
                # Use a pie chart for categorical data with fewer unique values
                fig = px.pie(df, names=categorical_cols[0])
            else:
                # Default to a simple line plot if above conditions are not met
                fig = px.line(df)

        if fig is None:
            return None

        if dark_mode:
            fig.update_layout(template="plotly_dark")

        return fig
