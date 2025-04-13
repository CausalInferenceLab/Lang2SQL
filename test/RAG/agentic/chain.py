import os
import sys
from enum import Enum
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from langchain_core.output_parsers import StrOutputParser
from langchain.output_parsers.enum import EnumOutputParser
from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnablePassthrough
from langchain_core.prompts import PromptTemplate

from agent.tables_json_import import load_json_table
from prompt.prompt_template import QUERY_PROMPT_TEMPLATE



class Tables(Enum):
    client = "빨간색"
    contact = "초록색"
    deal = "파란색"


parser = EnumOutputParser(enum=Tables)



prompt = PromptTemplate.from_template(
    """다음의 물체는 어떤 색깔인가요?

question: {question}

Instructions: {instructions}"""


).partial(instructions=parser.get_format_instructions())


llm = ChatOpenAI(model_name="gpt-4o", temperature=0)



def chain_invoke(text, table_name):


    llm = ChatOpenAI(model_name="gpt-4o", temperature=0)
    chain = prompt | ChatOpenAI() | parser

    return chain.invoke({"question": text})






def query_agent(text, table_name):
    context = load_json_table(table_name)
    chain = (
        {"context": RunnablePassthrough(), "question": RunnablePassthrough()}
        | prompt
        | llm
        | EnumOutputParser(enum=Tables)

    )

    return chain.invoke({"context": context, "question": text})


# print(query_agent("구독자의 평균 유지 기간은 얼마인가요?", "client_subscription.json"))