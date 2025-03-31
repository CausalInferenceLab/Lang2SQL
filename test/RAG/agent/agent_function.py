import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnablePassthrough

from agent.tables_json_import import load_json_table
from prompt.prompt_template import QUERY_PROMPT_TEMPLATE


prompt = QUERY_PROMPT_TEMPLATE

llm = ChatOpenAI(model_name="gpt-4o", temperature=0)


def query_agent(text, table_name):
    context = load_json_table(table_name)
    chain = (
        {"context": RunnablePassthrough(), "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )

    return chain.invoke({"context": context, "question": text})


# print(query_agent("구독자의 평균 유지 기간은 얼마인가요?", "client_subscription.json"))