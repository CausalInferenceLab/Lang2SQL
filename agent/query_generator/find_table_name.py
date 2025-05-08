from dotenv import load_dotenv
load_dotenv()

from typing_extensions import TypedDict
from pprint import pprint

from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableConfig
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import CommaSeparatedListOutputParser
from langgraph.graph import StateGraph, MessagesState, START, END

from llm_utils.tools import get_info_from_db, _get_column_info
from table_name_splitter import table_name_splitter
from prompt import *


llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0)
table_name_splitter = table_name_splitter()

def table_name_finder(question):
    parser = CommaSeparatedListOutputParser()
    prompt = FIND_TABLE_NAMES.partial(instructions=parser.get_format_instructions())
    context = table_name_splitter   

    chain = (
            {"context": RunnablePassthrough(), "question": RunnablePassthrough()}
            | prompt
            | llm
            | parser
        )
    answer = chain.invoke({"context": context, "question": question})
    return answer

def table_info_finder(question):
    table_info = []
    table_name = table_name_finder(question)
    # pprint(table_name)   
    for table in table_name:
        table_info.append(_get_column_info(table)) 

    return table_info

pprint(table_info_finder("클라이언트 테이블 2개만"))
