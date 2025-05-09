import os

from langchain.schema import Document
table_info = Document(metadata={}, page_content="client_stream_activated_on_product: 고객이 제품을 활성화할 때 트리거되는 활동 데이터\nColumns:\n id: 이 테이블의 기본 키\nentity_id: 고객의 엔티티 ID\nactivity_ts: 활동이 발생한 타임스탬프\nactivity: 활동의 이름\nrevenue_impact: 활동의 수익 영향\nfeature_json: 활동과 관련된 기능 데이터를 포함하는 JSON 문자열로, 'active_users', 'churn_risk_users', 'churned_users', \n'free_users', 'paid_users', 'grace_period_users', 'canceled_users', 'new_users', 'returning_users', \n'trial_users'와 같은 고객 세그먼트와 'basic_plan', 'standard_plan', 'premium_plan', 'monthly_plan', \n'annual_plan', 'lifetime_plan'과 같은 요금제 유형, 그리고 'chris', 'john', 'jane', 'jim', 'jill', 'james'와 \n같은 CSM 이름, 'tier1', 'tier2', 'tier3', 'tier4', 'tier5'와 같은 MRR 등급 유형이 포함됩니다.\n"), Document(metadata={}, page_content="client_stream_active_on_subscription: 고객이 구독에서 활동 중일 때 트리거되는 활동 데이터\nColumns:\n id: 이 테이블의 기본 키\nentity_id: 고객의 엔티티 ID\nactivity_ts: 활동이 발생한 타임스탬프\nactivity: 활동 이름\nrevenue_impact: 활동의 수익 영향(해당되는 경우)\nfeature_json: 활동과 관련된 기능 데이터를 포함하는 JSON 문자열로, 'active_users', 'churn_risk_users', 'churned_users', 'free_users', 'paid_users', 'grace_period_users', 'canceled_users', 'new_users', 'returning_users', 'trial_users'와 같은 고객 세그먼트 및 'basic_plan', 'standard_plan', 'premium_plan', 'monthly_plan', 'annual_plan', 'lifetime_plan'과 같은 플랜 유형을 포함합니다."), Document(metadata={}, page_content="client_stream_called_support: 고객이 지원팀으로부터 전화를 받았을 때 트리거되는 활동 데이터\nColumns:\n id: 이 테이블의 기본 키\nentity_id: 고객의 엔티티 ID\nactivity_ts: 활동의 타임스탬프\nactivity: 활동의 이름\nrevenue_impact: 활동의 수익 영향\nfeature_json: 활동과 관련된 기능 데이터를 포함하는 JSON 문자열로, 'active_users', 'churn_risk_users', 'churned_users', \n'free_users', 'paid_users', 'grace_period_users', 'canceled_users', 'new_users', 'returning_users', \n'trial_users'와 같은 고객 세그먼트와 'basic_plan', 'standard_plan', 'premium_plan', 'monthly_plan', \n'annual_plan', 'lifetime_plan'과 같은 플랜 유형, 그리고 'chris', 'john', 'jane', 'jim', 'jill', 'james'와 \n같은 CSM 이름, 'tier1', 'tier2', 'tier3', 'tier4', 'tier5'와 같은 MRR 등급 유형을 포함합니다.\n"), Document(metadata={}, page_content="client_stream_churned_on_product: 고객이 제품에서 이탈할 때 트리거되는 활동 데이터\nColumns:\n id: 이 테이블의 기본 키\nentity_id: 고객의 엔티티 ID\nactivity_ts: 활동이 발생한 타임스탬프\nactivity: 활동의 이름\nrevenue_impact: 활동의 수익 영향\nfeature_json: 활동과 관련된 기능 데이터를 포함하는 JSON 문자열로, 'active_users', 'churn_risk_users', 'churned_users', \n'free_users', 'paid_users', 'grace_period_users', 'canceled_users', 'new_users', 'returning_users', \n'trial_users'와 같은 고객 세그먼트와 'basic_plan', 'standard_plan', 'premium_plan', 'monthly_plan', \n'annual_plan', 'lifetime_plan'과 같은 요금제 유형, 그리고 'chris', 'john', 'jane', 'jim', 'jill', 'james'와 \n같은 CSM 이름, 'tier1', 'tier2', 'tier3', 'tier4', 'tier5'와 같은 MRR 등급 유형이 포함됩니다.\n")


from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings


embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
persist_directory = os.path.join(os.getcwd(), "table_info_chroma_db")


def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)

def chroma_db_generator(index_name, table_info, k):
    try:
    # Chroma DB 로드 시도
        db = Chroma(
            collection_name=index_name,
            persist_directory=persist_directory,
            embedding_function=embeddings
        )
        # 컬렉션이 비어있는지 확인 (선택적)
        if db._collection.count() == 0:
            raise ValueError("빈 Chroma 컬렉션")
        
    except Exception as e:
        print(f"Chroma DB를 로드하지 못했습니다: {e}")
        print("새 Chroma DB를 생성합니다...")
        
        # 기존 정보 가져오기
        documents = table_info
        
        # 새 Chroma DB 생성 
        db = Chroma.from_documents(
            collection_name=index_name,
            documents=documents,
            embedding=embeddings,
            persist_directory=persist_directory
        )
        # 변경사항 저장
    retriever = db.as_retriever(
        search_type="similarity", search_kwargs={"k": k}
    )   
        

    return retriever




# retriever = chroma_db_generator(index_name="tablename", table_info=table_info, k=5)
# result = retriever.invoke("client_stream_activated_on_product")
# print(format_docs(result))