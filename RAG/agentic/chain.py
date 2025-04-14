import os
import sys
from enum import Enum
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dotenv import load_dotenv
load_dotenv()

from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import CommaSeparatedListOutputParser

from RAG.agentic.retriever import pinecone_retriever_invoke
from RAG.prompt.prompt_template import *


class RAGChain:
    def __init__(self, model_name="gpt-4o", temperature=0):
        self.llm = ChatOpenAI(model_name=model_name, temperature=temperature)
        self.parser = CommaSeparatedListOutputParser()
    
    def category_names_chain(self, text):
        prompt = CATEGORY_NAMES_PROMPT.partial(instructions=self.parser.get_format_instructions())
        chain = prompt | self.llm | self.parser
        return chain.invoke({"question": text})
    
    def tables_names_chain(self, text):
        output_tables = []
        table_names = self.category_names_chain(text)
        
        for table_name in table_names:
            if table_name == "client":
                prompt_template = CLIENT_TABLE_NAMES_PROMPT.partial(instructions=self.parser.get_format_instructions())
                chain = prompt_template | self.llm | self.parser
                tables = chain.invoke({"question": text})
                output_tables.extend(tables)  # 리스트를 확장
                
            elif table_name == "contact":
                prompt_template = CONTACT_TABLE_NAMES_PROMPT.partial(instructions=self.parser.get_format_instructions())
                chain = prompt_template | self.llm | self.parser
                tables = chain.invoke({"question": text})
                output_tables.extend(tables)  # 리스트를 확장
    
            elif table_name == "deal":
                prompt_template = DEAL_TABLE_NAMES_PROMPT.partial(instructions=self.parser.get_format_instructions())
                chain = prompt_template | self.llm | self.parser
                tables = chain.invoke({"question": text})
                output_tables.extend(tables)  # 리스트를 확장
            
        return output_tables
    
    def function_retriever(self, text):
        category_names = self.category_names_chain(text)
        table_names = self.tables_names_chain(text)
        
        all_results = []
        
        for category_name in category_names:
            try:
                retriever = pinecone_retriever_invoke(category_name, 1)
                
                for table_name in table_names:
                    try:
                        results = retriever.invoke(table_name)
                        all_results.extend(results)
                    except Exception as e:
                        print(f"오류 발생: {e} - 카테고리: {category_name}, 테이블: {table_name}")
            except Exception as e:
                print(f"검색기 생성 오류: {e} - 카테고리: {category_name}")
        
        context_str = ""
        for doc in all_results:
            if hasattr(doc, 'page_content'):
                context_str += doc.page_content + "\n\n"
            elif isinstance(doc, dict) and "page_content" in doc:
                context_str += doc["page_content"] + "\n\n"
            elif isinstance(doc, str):
                context_str += doc + "\n\n"
        
        return context_str
    
    def retriever_chain_invoke(self, text):
        context = self.function_retriever(text)
        prompt = RETRIEVER_CHAIN_PROMPT
    
        chain = (
                {"context": RunnablePassthrough(), "question": RunnablePassthrough()}
                | prompt
                | self.llm
                | StrOutputParser()
        )
        answer = chain.invoke({"context": context, "question": text})
        
        return answer


# 기존 코드와 호환성을 위한 전역 인스턴스 및 함수
# _rag_chain = RAGChain()
# llm = _rag_chain.llm
# parser = _rag_chain.parser

# def category_names_chain(text):
#     return _rag_chain.category_names_chain(text)

# def tables_names_chain(text):
#     return _rag_chain.tables_names_chain(text)

# def function_retriever(text):
#     return _rag_chain.function_retriever(text)

# def retriever_chain_invoke(text):
#     return _rag_chain.retriever_chain_invoke(text)


# # 테스트 코드
# if __name__ == "__main__":
#     rag = RAGChain()
#     result = rag.retriever_chain_invoke("각 고객별로 구독 시작 후 컨택한 SDR과 관련된 거래 기회 수익은 얼마인가요?")
#     print(result)
        
        
        
        
        
        
        
        
        
        
        
        
        
    


