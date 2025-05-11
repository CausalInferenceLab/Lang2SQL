import os

from langchain.schema import Document

from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings


embeddings = OpenAIEmbeddings(model="text-embedding-3-large")
persist_directory = os.path.join(os.getcwd(), "table_info_chroma_db")


# table_info = (
#     Document(
#         metadata={},
#         page_content="client_stream_activated_on_product: 고객이 제품을 활성화할 때 트리거되는 활동 데이터\nColumns:\n id: 이 테이블의 기본 키\nentity_id: 고객의 엔티티 ID\nactivity_ts: 활동이 발생한 타임스탬프\nactivity: 활동의 이름\nrevenue_impact: 활동의 수익 영향\nfeature_json: 활동과 관련된 기능 데이터를 포함하는 JSON 문자열로, 'active_users', 'churn_risk_users', 'churned_users', \n'free_users', 'paid_users', 'grace_period_users', 'canceled_users', 'new_users', 'returning_users', \n'trial_users'와 같은 고객 세그먼트와 'basic_plan', 'standard_plan', 'premium_plan', 'monthly_plan', \n'annual_plan', 'lifetime_plan'과 같은 요금제 유형, 그리고 'chris', 'john', 'jane', 'jim', 'jill', 'james'와 \n같은 CSM 이름, 'tier1', 'tier2', 'tier3', 'tier4', 'tier5'와 같은 MRR 등급 유형이 포함됩니다.\n",
#     ),
#     Document(
#         metadata={},
#         page_content="client_stream_active_on_subscription: 고객이 구독에서 활동 중일 때 트리거되는 활동 데이터\nColumns:\n id: 이 테이블의 기본 키\nentity_id: 고객의 엔티티 ID\nactivity_ts: 활동이 발생한 타임스탬프\nactivity: 활동 이름\nrevenue_impact: 활동의 수익 영향(해당되는 경우)\nfeature_json: 활동과 관련된 기능 데이터를 포함하는 JSON 문자열로, 'active_users', 'churn_risk_users', 'churned_users', 'free_users', 'paid_users', 'grace_period_users', 'canceled_users', 'new_users', 'returning_users', 'trial_users'와 같은 고객 세그먼트 및 'basic_plan', 'standard_plan', 'premium_plan', 'monthly_plan', 'annual_plan', 'lifetime_plan'과 같은 플랜 유형을 포함합니다.",
#     ),
#     Document(
#         metadata={},
#         page_content="client_stream_called_support: 고객이 지원팀으로부터 전화를 받았을 때 트리거되는 활동 데이터\nColumns:\n id: 이 테이블의 기본 키\nentity_id: 고객의 엔티티 ID\nactivity_ts: 활동의 타임스탬프\nactivity: 활동의 이름\nrevenue_impact: 활동의 수익 영향\nfeature_json: 활동과 관련된 기능 데이터를 포함하는 JSON 문자열로, 'active_users', 'churn_risk_users', 'churned_users', \n'free_users', 'paid_users', 'grace_period_users', 'canceled_users', 'new_users', 'returning_users', \n'trial_users'와 같은 고객 세그먼트와 'basic_plan', 'standard_plan', 'premium_plan', 'monthly_plan', \n'annual_plan', 'lifetime_plan'과 같은 플랜 유형, 그리고 'chris', 'john', 'jane', 'jim', 'jill', 'james'와 \n같은 CSM 이름, 'tier1', 'tier2', 'tier3', 'tier4', 'tier5'와 같은 MRR 등급 유형을 포함됩니다.\n",
#     ),
#     Document(
#         metadata={},
#         page_content="client_stream_churned_on_product: 고객이 제품에서 이탈할 때 트리거되는 활동 데이터\nColumns:\n id: 이 테이블의 기본 키\nentity_id: 고객의 엔티티 ID\nactivity_ts: 활동이 발생한 타임스탬프\nactivity: 활동의 이름\nrevenue_impact: 활동의 수익 영향\nfeature_json: 활동과 관련된 기능 데이터를 포함하는 JSON 문자열로, 'active_users', 'churn_risk_users', 'churned_users', \n'free_users', 'paid_users', 'grace_period_users', 'canceled_users', 'new_users', 'returning_users', \n'trial_users'와 같은 고객 세그먼트와 'basic_plan', 'standard_plan', 'premium_plan', 'monthly_plan', \n'annual_plan', 'lifetime_plan'과 같은 요금제 유형, 그리고 'chris', 'john', 'jane', 'jim', 'jill', 'james'와 \n같은 CSM 이름, 'tier1', 'tier2', 'tier3', 'tier4', 'tier5'와 같은 MRR 등급 유형이 포함됩니다.\n",
#     ),
# )


def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)


def chroma_db_generator(index_name, documents=None):
    """
    Chroma 벡터스토어를 생성하거나 로드합니다.

    Args:
        index_name (str): 컬렉션 이름
        documents (list): Document 객체 리스트
    """
    # documents가 None이 아니고 리스트 형태가 아니면 리스트로 변환
    if documents is not None and not isinstance(documents, list):
        documents = list(documents)

    try:
        # Chroma DB 로드 시도
        db = Chroma(
            collection_name=index_name,
            persist_directory=persist_directory,
            embedding_function=embeddings,
        )

        # 문서가 제공된 경우 추가
        if documents:
            db.add_documents(documents)
            # db.persist()

        return db

    except Exception as e:
        print(f"Chroma DB를 로드하지 못했습니다: {e}")
        print("새 Chroma DB를 생성합니다...")

        # 문서가 제공되지 않은 경우 에러 발생
        if not documents:
            raise ValueError("문서가 제공되지 않았습니다. 새 컬렉션을 생성하려면 문서가 필요합니다.")

        # 새 Chroma DB 생성
        db = Chroma.from_documents(
            collection_name=index_name,
            documents=documents,
            embedding=embeddings,
            persist_directory=persist_directory,
        )
        # db.persist()

        return db


# chroma_db_generator("table_name_retriever", table_info)
