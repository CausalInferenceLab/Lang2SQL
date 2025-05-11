from langchain_core.prompts import PromptTemplate

TABLE_NAME_GENERATOR = PromptTemplate.from_template(
    """

    ### 역할 ###
    너의 역할은 테이블 이름을 만들어주는 역할이야
    
    ### 역할 설명 ###
    너에게 테이블 정보와 사용자의 질문이 주어질거야
    사용자는 DB에서 사용할 쿼리에 대해서 질문해
    너는 사용자의 질문에 맞는 쿼리를 만들기 위해 필요한 테이블 이름을 추론해서 만들어야해
    
    ### 정보 설명 ###
    해당 정보들은 테이블의 카테고리들이야 해당 카테고리들이 어떻게 구성됐냐면
    예를 들어 "client_stream_responded_to_nps_survey" 테이블이 있다고 가정할게
    이때 "_" 기준으로 카테고리를 나누면

    "client" -> "first"
    "stream" -> "second"
    "responded" -> "third"
    "to" -> "fourth"
    "nps" -> "fifth"
    "survey" -> "sixth"

    이렇게 카테고리들이 나뉘는데 이때 너는 사용자의 질문에 맞는 테이블 이름을 추론해야해
    내가 [테이블 정보]에 카테고리화된 테이블 정보들을 너에게 줄테니 너는 사용자의 질문에 맞는 테이블 이름을 추론해야해

    사용자의 질문에 맞는 테이블은 여러개가 될수있어 너가 만약 해당 질문을 받고 쿼리를 생성해야한다면 그 쿼리에 맞는 테이블 이름을 추론해야해

    ### 카테고리 이름 ###
    카테고리 이름은 다음과 같아
    "first", "second", "third", "fourth", "fifth",
    "sixth", "seventh", "eighth", "ninth", "tenth",
    "eleventh", "twelfth", "thirteenth", "fourteenth", "fifteenth"


    ### 테이블 정보 ###
    table_info: {table_info}


    ### 테이블 이름 예시 ###
    sample_table_name: {sample_table_name}
    
    ### 사용자의 질문 ###
    question: {question}


    ### 출력 ###
    Instructions: {instructions}
    """
)


RETRIEVE_TABLE_NAME = PromptTemplate.from_template(
    """

    ### 역할 ###
    검색된 테이블 정보들 중에서 사용자의 질문에 맞는 쿼리를 만들기 위해 필요한 테이블 이름을 골라줘

    ### 역할 설명 ###
    너는 벡터DB에서 검색된 TOP K개의 테이블 정보를 보고 사용자의 질문에 맞는 쿼리를 만들기 위해 필요한 테이블 이름을 골라줘
    다만 너가 인지해야하는 점은 쿼리를 만들기위해서는 여러개의 테이블이 필요할수도있는점이야
    너는 그점을 고려해서 사용자 질문에 맞는 쿼리를 만들기 위해서는 필요한 테이블 이름을 골라서 출력해줘
    출력할때는 테이블 이름만 출력해줘

    ### TOP K 테이블 정보 ###
    {top_k}

    ### 사용자의 질문 ###
    {question}

    ### 출력 ###
    Instructions: {instructions}

    """
)


QUERY_GENERATOR = PromptTemplate.from_template(
    """

    ### 역할 ###
    사용자의 질문에 맞는 SQL쿼리를 생성해줘

    ### 역할 설명 ###
    너에게 테이블정보, 쿼리예제가 주어질거야
    쿼리예제는 없거나 맞지 않는 쿼리예제일수있어
    너는 해당 정보들을 전체적으로 파악하고 사용자의 질문에 맞는 SQL쿼리를 생성해줘


    ### 테이블 정보 ###
    {table_info}


    ### 쿼리 예제 ###
    {few_shot}

    ### 질문 ###
    {question}

    

    """
)


# QUERY_GENERATOR = PromptTemplate.from_template(
#     """
#     ### 역할 ###
#     너는 사용자의 질문에 맞는 SQL쿼리를 생성해줘


#     ### 테이블 정보 ###
#     {context}

#     ### 질문 ###
#     {question}

#     """
# )
