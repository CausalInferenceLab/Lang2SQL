import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from dotenv import load_dotenv
load_dotenv()

from typing import TypedDict, Dict, Any
from langgraph.graph import StateGraph, START, END
# from langchain_upstage import UpstageGroundednessCheck

from RAG.agentic.chain import RAGChain


class GraphState(TypedDict):
    question: str  # 질문
    context: str  # 문서의 검색 결과
    answer: str  # 답변
    relevance: str  # 답변의 문서에 대한 관련성


class RAGGraph:
    def __init__(self, model_name="gpt-4o", temperature=0):
        self.rag_chain = RAGChain(model_name=model_name, temperature=temperature)
        self._upstage_ground_checker = None
        self.builder = StateGraph(GraphState)
        self._setup_graph()
    
    def _setup_graph(self):
        """그래프 노드와 엣지를 설정합니다."""
        # 노드 추가
        self.builder.add_node("CATEGORY_NAMES", self._category_names_node)
        self.builder.add_node("TABLE_NAMES", self._table_names_node)
        self.builder.add_node("FUNCTION_RETRIEVER", self._function_retriever_node)
        self.builder.add_node("RETRIEVER_CHAIN", self._retriever_chain_node)
        
        # 엣지 추가
        self.builder.add_edge(START, "CATEGORY_NAMES")
        self.builder.add_edge("CATEGORY_NAMES", "TABLE_NAMES")
        self.builder.add_edge("TABLE_NAMES", "FUNCTION_RETRIEVER")
        self.builder.add_edge("FUNCTION_RETRIEVER", "RETRIEVER_CHAIN")
        self.builder.add_edge("RETRIEVER_CHAIN", END)
    
    def _category_names_node(self, state: GraphState) -> Dict[str, Any]:
        """카테고리 이름을 찾는 노드"""
        question = state["question"]
        category_names = self.rag_chain.category_names_chain(question)
        return {"category_names": category_names, "question": question}
    
    def _table_names_node(self, state: GraphState) -> Dict[str, Any]:
        """테이블 이름을 찾는 노드"""
        question = state["question"]
        table_names = self.rag_chain.tables_names_chain(question)
        return {"table_names": table_names, "question": question}
    
    def _function_retriever_node(self, state: GraphState) -> Dict[str, Any]:
        """문서를 검색하는 노드"""
        question = state["question"]
        context = self.rag_chain.function_retriever(question)
        return {"context": context, "question": question}
    
    def _retriever_chain_node(self, state: GraphState) -> Dict[str, Any]:
        """검색 결과를 바탕으로 답변을 생성하는 노드"""
        question = state["question"]
        context = state["context"]
        answer = self.rag_chain.retriever_chain_invoke(question)
        return {"answer": answer, "context": context, "question": question}
    
    def get_graph(self):
        """구성된 그래프를 반환합니다."""
        return self.builder.compile()
    
    def invoke(self, question: str) -> Dict[str, Any]:
        """그래프를 실행하여 질문에 답변합니다."""
        graph = self.get_graph()
        result = graph.invoke({"question": question})
        return result
    
    # 관련성 검사 기능 (주석 처리된 코드 재구성)
    def get_upstage_ground_checker(self):
        """UpstageGroundednessCheck 인스턴스를 반환합니다."""
        # from langchain_upstage import UpstageGroundednessCheck
        # if self._upstage_ground_checker is None:
        #     api_key = os.getenv("UPSTAGE_API_KEY")
        #     self._upstage_ground_checker = UpstageGroundednessCheck(api_key=api_key)
        # return self._upstage_ground_checker
        pass
    
    def relevance_check(self, state: GraphState) -> GraphState:
        """답변과 문서 간의 관련성을 검사합니다."""
        # upstage_ground_checker = self.get_upstage_ground_checker()
        # response = upstage_ground_checker.run(
        #     {"context": state["context"], "answer": state["answer"]}
        # )
        # return GraphState(
        #     relevance=response,
        #     context=state["context"],
        #     answer=state["answer"],
        #     question=state["question"],
        # )
        pass


# 예제 사용
# if __name__ == "__main__":
#     rag_graph = RAGGraph()
#     result = rag_graph.invoke("각 고객별로 구독 시작 후 컨택한 SDR과 관련된 거래 기회 수익은 얼마인가요?")
#     print(result["answer"])
