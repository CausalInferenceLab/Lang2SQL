import os
import sys

from dotenv import load_dotenv

load_dotenv()

from typing_extensions import TypedDict
from pprint import pprint

from langchain_core.output_parsers import (
    StrOutputParser,
    CommaSeparatedListOutputParser,
)
from langchain_core.runnables import RunnablePassthrough, RunnableConfig
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, MessagesState, START, END

from langchain.globals import set_debug
from langchain.schema import Document

# set_debug(True)

# 절대 경로를 사용한 임포트로 변경
# from llm_utils.tools import get_info_from_db, get_column_info
from .table_splitter import (
    get_table_info_from_results,
    get_table_names_list,
    get_unique_named_lists,
    get_sample_tables,
)
from .prompt import *
from .retriever_chroma_db import chroma_db_generator


class GraphState(TypedDict):
    question: str

    split_table_name: list[str]
    retriever_table_name: list[str]
    tables_column_info: list[str]
    
    context: str
    answer: str


class QueryGenerator:
    def __init__(self, model_name="gpt-4.1-mini", temperature=0):
        self.llm = ChatOpenAI(model_name=model_name, temperature=temperature)

        self.list_parser = CommaSeparatedListOutputParser()
        self.str_parser = StrOutputParser()

        self.table_names_list = get_table_names_list()
        self.unique_named_lists = get_unique_named_lists()
        self.sample_table_name = get_sample_tables()
        

        self.builder = StateGraph(GraphState)
        self._setup_graph()

    def format_docs(self, docs):
        return "\n\n".join(doc.page_content for doc in docs)

    def table_name_finder(self, state: GraphState):
        question = state["question"]
        parser = self.list_parser
        prompt = TABLE_NAME_GENERATOR.partial(
            instructions=parser.get_format_instructions()
        )

        chain = (
            {
                "table_info": RunnablePassthrough(),
                "sample_table_name": RunnablePassthrough(),
                "question": RunnablePassthrough(),
            }
            | prompt
            | self.llm
            | self.list_parser
        )
        answer = chain.invoke(
            {
                "table_info": self.unique_named_lists,
                "sample_table_name": self.sample_table_name,
                "question": question,
            }
        )

        return GraphState(split_table_name=answer)

    def table_name_retriever(self, state: GraphState):
        question = state["question"]

        split_table_name = state["split_table_name"]

        documents = [
            Document(page_content=table_name, metadata={})
            for table_name in self.table_names_list
        ]

        vector_store = chroma_db_generator("table_name_retriever", documents)
        retriever = vector_store.as_retriever(
            search_type="similarity", search_kwargs={"k": 3}
        )

        context_list = []
        for table_name in split_table_name:
            context = retriever.invoke(table_name)
            context = self.format_docs(context)
            context_list.append(context)

        parser = CommaSeparatedListOutputParser()
        prompt = RETRIEVE_TABLE_NAME.partial(
            instructions=parser.get_format_instructions()
        )

        top_k = context_list

        chain = (
            {
                "top_k": RunnablePassthrough(),
                "question": RunnablePassthrough(),
            }
            | prompt
            | self.llm
            | self.list_parser
        )

        answer = chain.invoke({"top_k": top_k, "question": question})

        return GraphState(retriever_table_name=answer)

    def get_table_info(self, state):
        column_info = []
        tables_info = []
        # state가 리스트로 들어올 수도 있으니 타입 체크
        if isinstance(state, list):
            retriever_table_name = state
        else:
            retriever_table_name = state["retriever_table_name"]

        for table_name in retriever_table_name:
            column_info.append(get_table_info_from_results(table_name))

            for table_info in column_info:
                tables_info.append(table_info)

        return GraphState(tables_column_info=tables_info)

    def query_generator(self, state: GraphState):
        question = state["question"]
        table_name_context = state["retriever_table_name"]

        table_info = self.get_table_info(table_name_context)


        vector_store = chroma_db_generator("few_shot")
        retriever = vector_store.as_retriever(
            search_type="similarity", search_kwargs={"k": 1}
        )

        few_shot = retriever.invoke(question)

        prompt = QUERY_GENERATOR

        chain = (
            {
                "table_info": RunnablePassthrough(),
                "few_shot": RunnablePassthrough(),
                "question": RunnablePassthrough(),
            }
            | prompt
            | self.llm
            | self.str_parser
        )

        answer = chain.invoke(
            {
                "table_info": table_info,
                "few_shot": few_shot,
                "question": question,
            }
        )

        return GraphState(answer=answer)
    
    def _setup_graph(self):
        self.builder.add_node("TableNamesFinder", self.table_name_finder)
        self.builder.add_node("TableNamesRetriever", self.table_name_retriever)
        self.builder.add_node("GetTableInfo", self.get_table_info)
        self.builder.add_node("QueryGenerator", self.query_generator)

        self.builder.add_edge(START, "TableNamesFinder")
        self.builder.add_edge("TableNamesFinder", "TableNamesRetriever")
        self.builder.add_edge("TableNamesRetriever", "GetTableInfo")
        self.builder.add_edge("GetTableInfo", "QueryGenerator")
        self.builder.add_edge("QueryGenerator", END)


    def graph_response(self, question):
        graph = self.builder.compile()
        # config = RunnableConfig(recursion_limit=10)
        inputs = GraphState(question=question)
        output = graph.invoke(inputs)
        return output["answer"]



if __name__ == "__main__":
    query_generator = QueryGenerator()
    result = query_generator.graph_response("고객 데이터를 기반으로 유니크한 유저 수를 카운트하는 쿼리만들어줘")
    print(result)