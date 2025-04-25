import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv()

from typing import Annotated

from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import CommaSeparatedListOutputParser

from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import ToolNode
from langgraph.graph.message import add_messages


from prompt import *
from agent.pinecone.retriever import pinecone_retriever_invoke

llm = ChatOpenAI(model_name="gpt-4.1", temperature=0)


def format_docs(docs):
    # 검색한 문서 결과를 하나의 문단으로 합쳐줍니다.
    return "\n\n".join(doc.page_content for doc in docs)



def table_names_chain(state: MessagesState):
        question = state["messages"]
        parser = CommaSeparatedListOutputParser()
        prompt = FIND_TABLE_NAMES.partial(instructions=parser.get_format_instructions())
        chain = prompt | llm | parser
        return chain.invoke({"question": question})



def table_names_retriever(state: MessagesState):
    question = state["messages"]
    table_names = table_names_chain(question)
    context = ''
    retriever = pinecone_retriever_invoke("sql-ddl-tables", 3)
    
    for table_name in table_names:
        context +=  format_docs(retriever.invoke("name: " + table_name))
    print(context)
    return context


def search_table_chain_invoke(state: MessagesState):
    question = state["messages"]
    context = table_names_retriever(question)
    prompt = SEARCH_TABLE_PROMPT

    chain = (
        {"context": RunnablePassthrough(), "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    answer = chain.invoke({"context": context, "question": question})
    
    return answer






workflow = StateGraph(MessagesState)
workflow.add_node("TableNamesChain", table_names_chain)
workflow.add_node("TableNamesRetriever", table_names_retriever)
workflow.add_node("SearchTableChainInvoke", search_table_chain_invoke)

workflow.add_edge(START, "TableNamesChain")
workflow.add_edge("TableNamesChain", "TableNamesRetriever")
workflow.add_edge("TableNamesRetriever", "SearchTableChainInvoke")
workflow.add_edge("SearchTableChainInvoke", END)

workflow.compile()



