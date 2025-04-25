# sql_save_pinecone.py
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain.schema import Document
import os
import re
import sqlparse

load_dotenv()

embeddings = OpenAIEmbeddings(model="text-embedding-3-large")

# 현재 스크립트 위치 기준 절대 경로 계산
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sql_path = os.path.join(base_path, "pinecone", "all_tables_ddl.sql")

def extract_table_info_from_sql(sql_file_path):
    """SQL 파일에서 테이블 정보를 추출하는 함수"""
    if not os.path.exists(sql_file_path):
        print(f"경고: SQL 파일이 존재하지 않습니다: {sql_file_path}")
        return []
    
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # SQL 문을 파싱하여 각 테이블 정의 추출
    statements = sqlparse.split(sql_content)
    tables = []
    current_table = None
    table_comments = {}
    column_comments = {}
    
    for statement in statements:
        statement = statement.strip()
        
        # CREATE TABLE 구문 추출
        if statement.upper().startswith('CREATE TABLE'):
            parsed = sqlparse.parse(statement)[0]
            # 테이블 이름 추출
            create_table_tokens = [token for token in parsed.tokens if token.ttype is None and isinstance(token, sqlparse.sql.Identifier)]
            if create_table_tokens:
                table_name = create_table_tokens[0].value
            else:
                # 정규식으로 추출 시도
                match = re.search(r'CREATE TABLE\s+(\w+)', statement, re.IGNORECASE)
                if match:
                    table_name = match.group(1)
                else:
                    continue  # 테이블 이름을 찾을 수 없음
            
            current_table = {
                "name": table_name,
                "definition": statement,
                "columns": {}
            }
            tables.append(current_table)
        
        # 테이블 코멘트 추출
        elif statement.upper().startswith('COMMENT ON TABLE') and current_table:
            match = re.search(r"COMMENT ON TABLE\s+(\w+)\s+IS\s+'([^']+)';", statement, re.IGNORECASE)
            if match:
                table_name, comment = match.groups()
                table_comments[table_name] = comment
                # 현재 테이블에 코멘트 추가
                if current_table["name"] == table_name:
                    current_table["description"] = comment
        
        # 컬럼 코멘트 추출
        elif statement.upper().startswith('COMMENT ON COLUMN') and current_table:
            match = re.search(r"COMMENT ON COLUMN\s+(\w+)\.(\w+)\s+IS\s+'([^']+)';", statement, re.IGNORECASE)
            if match:
                table_name, column_name, comment = match.groups()
                column_key = f"{table_name}.{column_name}"
                column_comments[column_key] = comment
                
                # 현재 테이블의 컬럼에 코멘트 추가
                if current_table["name"] == table_name:
                    current_table["columns"][column_name] = comment
    
    return tables

def create_documents_from_tables(tables):
    """테이블 정보로부터 Document 객체를 생성"""
    documents = []
    
    for table in tables:
        table_name = table.get("name", "")
        table_description = table.get("description", "")
        table_definition = table.get("definition", "")
        
        # 테이블 정보를 텍스트로 변환
        content = f"Table: {table_name}\n"
        content += f"Description: {table_description}\n"
        content += "Columns:\n"
        
        for column_name, column_description in table.get("columns", {}).items():
            content += f"  - {column_name}: {column_description}\n"
        
        # 메타데이터 구성
        metadata = {
            "type": "table",
            "name": table_name,
            "description": table_description,
            "definition": table_definition,
            "source": "sql"
        }
        
        # Document 객체 생성
        doc = Document(page_content=content, metadata=metadata)
        documents.append(doc)
        
    return documents

# 메인 실행 함수
def main():
    # SQL 파일에서 테이블 정보 추출
    print(f"SQL 파일에서 테이블 정보를 추출합니다: {sql_path}")
    tables = extract_table_info_from_sql(sql_path)
    print(f"추출된 테이블 수: {len(tables)}")
    
    if not tables:
        print("처리할 테이블이 없습니다.")
        return
    
    # Document 객체 생성
    docs = create_documents_from_tables(tables)
    print(f"생성된 문서 수: {len(docs)}")
    
    # Pinecone에 저장
    index_name = "sql-ddl-tables"
    print(f"Pinecone 인덱스 {index_name}에 데이터를 저장합니다...")
    
    try:
        # 기존 인덱스가 없다면 새로 생성
        docsearch = PineconeVectorStore.from_documents(docs, embeddings, index_name=index_name)
        print("모든 테이블 정보가 Pinecone에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"Pinecone 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    main()