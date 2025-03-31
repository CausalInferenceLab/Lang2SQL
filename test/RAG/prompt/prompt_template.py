from langchain_core.prompts import PromptTemplate


QUERY_PROMPT_TEMPLATE = PromptTemplate.from_template(
    """

    ### 지시사항 ###
        답변은 SQL 쿼리로 해줘
        테이블 정보를 보고 사용자 입력에 맞는 SQL 쿼리를 생성해줘

    ### 절대 하지 말아야 할 답변 ###
        SQL 쿼리 없는 설명

    ### 답변 예시 ###

        질문: 지난 달 생성된 지원 티켓의 평균 해결 시간은 얼마인가요?
    
        답변:
        WITH created_tickets AS (
        SELECT 
            entity_id, 
            activity_ts AS created_ts
        FROM 
            client_stream_created_support_ticket
        WHERE 
            activity_ts >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
            AND activity_ts < DATE_TRUNC('month', CURRENT_DATE)
        ),
        closed_tickets AS (
            SELECT 
                entity_id, 
                activity_ts AS closed_ts
            FROM 
                client_stream_closed_support_ticket
            WHERE 
                activity_ts >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
                AND activity_ts < DATE_TRUNC('month', CURRENT_DATE)
        )
        SELECT 
            AVG(closed_ts - created_ts) AS avg_resolution_time
        FROM 
            created_tickets
        JOIN 
            closed_tickets
        ON 
            created_tickets.entity_id = closed_tickets.entity_id;


    ### 테이블 정보 ###
    {context}


    ### 사용자 입력 ###
    {question}
    
    
    """
)
