import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv()

from typing_extensions import TypedDict

from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableConfig
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import CommaSeparatedListOutputParser

from langgraph.graph import StateGraph, START, END


from prompt import *
from agent.pinecone.retriever import pinecone_retriever_invoke

class GraphState(TypedDict):
    question: str
    table_names: list[str]
    context: str
    answer: str


class SearchQuery:
    def __init__(self, model_name="gpt-4.1", temperature=0):
        self.llm = ChatOpenAI(model_name=model_name, temperature=temperature)
        self.builder = StateGraph(GraphState)
        self._setup_graph()

    def _setup_graph(self):
        self.builder.add_node("TableNamesChain", self.table_names_chain)
        self.builder.add_node("TableNamesRetriever", self.table_names_retriever)
        self.builder.add_node("SearchTableChainInvoke", self.search_table_chain_invoke)

        self.builder.add_edge(START, "TableNamesChain")
        self.builder.add_edge("TableNamesChain", "TableNamesRetriever")
        self.builder.add_edge("TableNamesRetriever", "SearchTableChainInvoke")
        self.builder.add_edge("SearchTableChainInvoke", END)

    def graph_response(self, question):
        graph = self.builder.compile()
        config = RunnableConfig(
            recursion_limit=10
        )
        inputs = GraphState(question=question)
        output = graph.invoke(inputs, config=config)
        return output["answer"]
    
    def format_docs(self, docs):
    # 검색한 문서 결과를 하나의 문단으로 합쳐줍니다.
        return "\n\n".join(doc.page_content for doc in docs)

    def table_names_chain(self, state: GraphState):
        question = state["question"]
        parser = CommaSeparatedListOutputParser()
        prompt = FIND_TABLE_NAMES.partial(instructions=parser.get_format_instructions())
        chain = prompt | self.llm | parser
        table_names = chain.invoke({"question": question})
        return GraphState(
            table_names=table_names
        )


    def table_names_retriever(self, state: GraphState):
        table_names = state["table_names"]
        context = ''
        retriever = pinecone_retriever_invoke("sql-ddl-tables", 3)
        
        for table_name in table_names:
            context +=  self.format_docs(retriever.invoke("name: " + table_name))
        print(context)
        return context


    def search_table_chain_invoke(self, state: GraphState):
        question = state["question"]
        context = self.table_names_retriever(question)
        prompt = SEARCH_TABLE_PROMPT

        chain = (
            {"context": RunnablePassthrough(), "question": RunnablePassthrough()}
            | prompt
            | self.llm
            | StrOutputParser()
        )
        answer = chain.invoke({"context": context, "question": question})
        
        return GraphState(
            answer=answer
        )
        


if __name__ == "__main__":
    search_query = SearchQuery()
    result = search_query.graph_response("client_stream_churned_on_product 뭐야?") 
    print(result)





