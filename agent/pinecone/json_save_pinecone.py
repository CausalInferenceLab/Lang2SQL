from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import JSONLoader
from langchain_pinecone import PineconeVectorStore
import os

load_dotenv()

embeddings = OpenAIEmbeddings(model="text-embedding-3-large")

# 현재 스크립트 위치 기준 절대 경로 계산
# base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# categorized_tables_path = os.path.join(base_path, "categorized_tables")

# 파일 경로들을 리스트로 정의
# json_files = [
#     os.path.join(categorized_tables_path, "deal_contact.json"),
#     os.path.join(categorized_tables_path, "deal_demo.json"),
#     os.path.join(categorized_tables_path, "deal_opportunity.json"),
    
# ]


json_files = ["/Users/sbk/pseudo_lab/Lang2SQL/agent/pinecone/dbt_tables_info.json"]


# 경로가 실제로 존재하는지 확인
for file_path in json_files:
    if not os.path.exists(file_path):
        print(f"경고: 파일이 존재하지 않습니다: {file_path}")

index_name = "metadata"

# 처음에는 벡터 스토어 초기화
docsearch = None

# 각 파일별로 문서를 로드하고 Pinecone에 추가
for json_file in json_files:
    if not os.path.exists(json_file):
        print(f"건너뜀: 파일이 존재하지 않습니다 - {json_file}")
        continue
        
    # 각 JSON 파일마다 로더 생성
    loader = JSONLoader(file_path=json_file,
                       jq_schema='.',
                       text_content=False,
                       )
    
    # 문서 로드
    docs = loader.load()
    
    print(f"처리 중: {os.path.basename(json_file)} - {len(docs)}개 문서")
    
    if docsearch is None:
        # 첫 번째 파일은 from_documents로 초기화
        docsearch = PineconeVectorStore.from_documents(docs, embeddings, index_name=index_name)
    else:
        # 이후 파일들은 add_documents로 추가
        docsearch.add_documents(docs)

print("모든 문서가 Pinecone에 성공적으로 추가되었습니다.")