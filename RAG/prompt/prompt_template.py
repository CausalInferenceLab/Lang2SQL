from langchain_core.prompts import PromptTemplate


QUERY_PROMPT_TEMPLATE = PromptTemplate.from_template(
    """

    ### 역할 ###
    너는 SQL쿼리를 작성하는 전문가야
    
    ### 테이블 정보 ###
    {context}
    
    
    ### 지시사항 ###
        답변은 SQL 쿼리로 해줘
        테이블 정보를 보고 사용자 입력에 맞는 SQL 쿼리를 생성해줘

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

            
    ### 절대 하지 말아야 할 답변 ###
        SQL 쿼리 없는 설명

    
    ### 사용자 입력 ###
    {question}
    
    
    """
)



CATEGORY_NAMES_PROMPT = PromptTemplate.from_template(
    """
    역할: 너는 사용자의 질문에 맞게 SQL쿼리를 작성하는 전문가야 해당 질문에서 필요한 테이블을 선택해줘
    나는 테이블이 너무 많아서 테이블들을 카테고리화 했어
    카테고리화 테이블은 다음과 같아
    1. client
    2. contact
    3. deal
    
    너에게 전체 테이블을 알려줄테니 너는 사용자의 질문에 맞는 테이블의 카테고리를 선택해줘

    # 전체 테이블 정보    
    ## Client 카테고리 테이블
    ### 구독 관련 테이블
    - client_stream_activated_on_product: 고객이 제품을 활성화할 때 트리거되는 활동 데이터
    - client_stream_active_on_subscription: 고객이 구독에서 활동 중일 때 트리거되는 활동 데이터
    - client_stream_churned_on_product: 고객이 제품에서 이탈할 때 트리거되는 활동 데이터
    - client_stream_committed_to_churn: 고객이 이탈을 확정했을 때 트리거되는 활동 데이터
    - client_stream_created_cancellation_request: 고객이 취소 요청을 생성할 때 트리거되는 활동 데이터
    - client_stream_ended_subscription: 고객이 구독을 종료할 때 트리거되는 활동 데이터
    - client_stream_started_subscription: 고객이 구독을 시작할 때 트리거되는 활동 데이터
    - client_stream_withdrew_cancellation_request: 고객이 취소 요청을 철회할 때 트리거되는 활동 데이터

    ### 지원 관련 테이블
    - client_stream_called_support: 고객이 지원팀에 전화했을 때 트리거되는 활동 데이터
    - client_stream_closed_support_ticket: 고객이 지원 티켓을 닫을 때 트리거되는 활동 데이터
    - client_stream_created_support_ticket: 고객이 지원 티켓을 생성할 때 트리거되는 활동 데이터
    - client_stream_reopened_support_ticket: 고객이 지원 티켓을 다시 열 때 트리거되는 활동 데이터
    - client_stream_updated_support_ticket: 고객이 지원 티켓을 업데이트할 때 트리거되는 활동 데이터

    ### 계약 관련 테이블
    - client_stream_decreased_contract: 고객이 계약을 축소할 때 트리거되는 활동 데이터
    - client_stream_expanded_contract: 고객이 계약을 확장할 때 트리거되는 활동 데이터
    - client_stream_renewed_contract: 고객이 계약을 갱신할 때 트리거되는 활동 데이터
    - client_stream_resurrected_contract: 고객이 계약을 부활시킬 때 트리거되는 활동 데이터

    ### 온보딩 관련 테이블
    - client_stream_onboarded: 고객이 온보딩되었을 때 트리거되는 활동 데이터
    - client_stream_onboarding_call: 고객과 온보딩 통화가 있을 때 트리거되는 활동 데이터

    ### 설문조사 관련 테이블
    - client_stream_responded_to_ces_survey: 고객이 CES 설문조사에 응답할 때 트리거되는 활동 데이터
    - client_stream_responded_to_csat_survey: 고객이 CSAT 설문조사에 응답할 때 트리거되는 활동 데이터
    - client_stream_responded_to_nps_survey: 고객이 NPS 설문조사에 응답할 때 트리거되는 활동 데이터

    ### 서비스 관련 테이블
    - client_stream_incurred_overage: 고객이 초과 사용량이 발생했을 때 트리거되는 활동 데이터
    - client_stream_ordered_service: 고객이 서비스를 주문했을 때 트리거되는 활동 데이터

    ## Contact 카테고리 테이블
    ### 마케팅 관련 테이블
    - contact_stream_attended_event: 연락처가 이벤트에 참석했을 때 트리거되는 활동 데이터
    - contact_stream_attended_webinar: 연락처가 웨비나에 참석했을 때 트리거되는 활동 데이터
    - contact_stream_clicked_ad: 연락처가 광고를 클릭했을 때 트리거되는 활동 데이터
    - contact_stream_doesnt_receive_email: 연락처가 이메일을 받지 않을 때 트리거되는 활동 데이터
    - contact_stream_engaged_with_email: 연락처가 이메일과 상호작용했을 때 트리거되는 활동 데이터
    - contact_stream_generated_suspect: 연락처가 의심 대상으로 생성되었을 때 트리거되는 활동 데이터
    - contact_stream_opened_email: 연락처가 이메일을 열었을 때 트리거되는 활동 데이터
    - contact_stream_sdr_emailed_prospect: SDR이 잠재 고객에게 이메일을 보냈을 때 트리거되는 활동 데이터
    - contact_stream_sent_email: 연락처에게 이메일이 발송되었을 때 트리거되는 활동 데이터
    - contact_stream_unsubscribed_email: 연락처가 이메일 구독을 취소했을 때 트리거되는 활동 데이터
    - contact_stream_viewed_ad: 연락처가 광고를 보았을 때 트리거되는 활동 데이터

    ### 세션 관련 테이블
    - contact_stream_completed_session: 연락처가 세션을 완료했을 때 트리거되는 활동 데이터
    - contact_stream_started_session: 연락처가 세션을 시작했을 때 트리거되는 활동 데이터
    - contact_stream_visited_page: 연락처가 페이지를 방문했을 때 트리거되는 활동 데이터

    ### 평가판 관련 테이블
    - contact_stream_converted_suspect_to_prospect: 의심 대상이 잠재 고객으로 전환되었을 때 트리거되는 활동 데이터
    - contact_stream_converted_to_pql: 연락처가 PQL로 전환되었을 때 트리거되는 활동 데이터
    - contact_stream_ended_trial: 연락처의 평가판이 종료되었을 때 트리거되는 활동 데이터
    - contact_stream_started_trial: 연락처가 평가판을 시작했을 때 트리거되는 활동 데이터
    - contact_stream_started_true_trial: 연락처가 진정한 평가판을 시작했을 때 트리거되는 활동 데이터
    - contact_stream_trial_converted_to_customer: 연락처의 평가판이 고객으로 전환되었을 때 트리거되는 활동 데이터

    ### SDR 관련 테이블
    - contact_stream_sdr_accepted_prospect: SDR이 잠재 고객을 수락했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_called_prospect: SDR이 잠재 고객에게 전화했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_cold_outbounded_prospect: SDR이 잠재 고객에게 콜드 아웃바운드를 했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_created_business_opportunity: SDR이 비즈니스 기회를 생성했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_disqualified_prospect: SDR이 잠재 고객을 자격 박탈했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_qualified_prospect: SDR이 잠재 고객을 자격을 부여했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_scheduled_intake_for_business_opportunity: SDR이 비즈니스 기회를 위한 인테이크를 예약했을 때 트리거되는 활동 데이터

    ### 양식 관련 테이블
    - contact_stream_interacted_with_form: 연락처가 양식과 상호작용했을 때 트리거되는 활동 데이터
    - contact_stream_submitted_form: 연락처가 양식을 제출했을 때 트리거되는 활동 데이터
    - contact_stream_viewed_form: 연락처가 양식을 보았을 때 트리거되는 활동 데이터

    ### 계약 관련 테이블
    - contact_stream_signed_contract: 연락처가 계약에 서명했을 때 트리거되는 활동 데이터

    ## Deal 카테고리 테이블
    ### 기회 관련 테이블
    - deal_stream_created_expansion_opportunity: 새로운 확장 기회가 생성되었을 때 트리거되는 활동 데이터
    - deal_stream_created_new_opportunity: 새로운 기회가 생성되었을 때 트리거되는 활동 데이터
    - deal_stream_created_renewal_opportunity: 갱신 기회가 생성되었을 때 트리거되는 활동 데이터
    - deal_stream_disqualified_new_opportunity: 새로운 기회가 자격 박탈되었을 때 트리거되는 활동 데이터
    - deal_stream_lost_opportunity: 기회를 잃었을 때 트리거되는 활동 데이터
    - deal_stream_qualified_new_opportunity: 새로운 기회가 자격을 얻었을 때 트리거되는 활동 데이터
    - deal_stream_updated_close_date: 마감일이 업데이트되었을 때 트리거되는 활동 데이터
    - deal_stream_updated_deal_amount: 거래 금액이 업데이트되었을 때 트리거되는 활동 데이터
    - deal_stream_updated_win_probability: 승리 확률이 업데이트되었을 때 트리거되는 활동 데이터
    - deal_stream_won_opportunity: 기회가 성사되었을 때 트리거되는 활동 데이터

    ### 연락처 관련 테이블
    - deal_stream_called_contact: 거래에서 연락처에 전화했을 때 트리거되는 활동 데이터
    - deal_stream_emailed_contact: 거래에서 연락처에 이메일을 보냈을 때 트리거되는 활동 데이터
    - deal_stream_met_contact: 거래에서 연락처를 만났을 때 트리거되는 활동 데이터

    ### 데모 관련 테이블
    - deal_stream_completed_demo: 데모가 완료되었을 때 트리거되는 활동 데이터
    - deal_stream_scheduled_demo: 데모가 예약되었을 때 트리거되는 활동 데이터

    ## 기타 카테고리
    ### 분석 수익 관련 테이블
    - ga_cube_churned_revenue: 이탈된 수익에 대한 분석 데이터
    - ga_cube_committed_revenue: 확약된 수익에 대한 분석 데이터


    # 출력 예시
    ## 1. client
    ## 2. client, contact
    ## 3. deal
    ### 이런식으로 너는 카테고리를 선택해줘

        
    question: {question}

    Instructions: {instructions}
    
    """


)


CLIENT_TABLE_NAMES_PROMPT = PromptTemplate.from_template(
    """
    
    # 역할: SQL쿼리 전문가야
    # 역할설명: 사용자의 질문에 가장 적합한 테이블을 선택해줘

    # 테이블 정보
    ## Client 카테고리 테이블
    ### 구독 관련 테이블
    - client_stream_activated_on_product: 고객이 제품을 활성화할 때 트리거되는 활동 데이터
    - client_stream_active_on_subscription: 고객이 구독에서 활동 중일 때 트리거되는 활동 데이터
    - client_stream_churned_on_product: 고객이 제품에서 이탈할 때 트리거되는 활동 데이터
    - client_stream_committed_to_churn: 고객이 이탈을 확정했을 때 트리거되는 활동 데이터
    - client_stream_created_cancellation_request: 고객이 취소 요청을 생성할 때 트리거되는 활동 데이터
    - client_stream_ended_subscription: 고객이 구독을 종료할 때 트리거되는 활동 데이터
    - client_stream_started_subscription: 고객이 구독을 시작할 때 트리거되는 활동 데이터
    - client_stream_withdrew_cancellation_request: 고객이 취소 요청을 철회할 때 트리거되는 활동 데이터

    ### 지원 관련 테이블
    - client_stream_called_support: 고객이 지원팀에 전화했을 때 트리거되는 활동 데이터
    - client_stream_closed_support_ticket: 고객이 지원 티켓을 닫을 때 트리거되는 활동 데이터
    - client_stream_created_support_ticket: 고객이 지원 티켓을 생성할 때 트리거되는 활동 데이터
    - client_stream_reopened_support_ticket: 고객이 지원 티켓을 다시 열 때 트리거되는 활동 데이터
    - client_stream_updated_support_ticket: 고객이 지원 티켓을 업데이트할 때 트리거되는 활동 데이터

    ### 계약 관련 테이블
    - client_stream_decreased_contract: 고객이 계약을 축소할 때 트리거되는 활동 데이터
    - client_stream_expanded_contract: 고객이 계약을 확장할 때 트리거되는 활동 데이터
    - client_stream_renewed_contract: 고객이 계약을 갱신할 때 트리거되는 활동 데이터
    - client_stream_resurrected_contract: 고객이 계약을 부활시킬 때 트리거되는 활동 데이터

    ### 온보딩 관련 테이블
    - client_stream_onboarded: 고객이 온보딩되었을 때 트리거되는 활동 데이터
    - client_stream_onboarding_call: 고객과 온보딩 통화가 있을 때 트리거되는 활동 데이터

    ### 설문조사 관련 테이블
    - client_stream_responded_to_ces_survey: 고객이 CES 설문조사에 응답할 때 트리거되는 활동 데이터
    - client_stream_responded_to_csat_survey: 고객이 CSAT 설문조사에 응답할 때 트리거되는 활동 데이터
    - client_stream_responded_to_nps_survey: 고객이 NPS 설문조사에 응답할 때 트리거되는 활동 데이터

    ### 서비스 관련 테이블
    - client_stream_incurred_overage: 고객이 초과 사용량이 발생했을 때 트리거되는 활동 데이터
    - client_stream_ordered_service: 고객이 서비스를 주문했을 때 트리거되는 활동 데이터
    
    
    """
)


CONTACT_TABLE_NAMES_PROMPT = PromptTemplate.from_template(
    """
    
    # 역할: SQL쿼리 전문가야
    # 역할설명: 사용자의 질문에 가장 적합한 테이블을 선택해줘

    # 테이블 정보
    ## Contact 카테고리 테이블
    ### 마케팅 관련 테이블
    - contact_stream_attended_event: 연락처가 이벤트에 참석했을 때 트리거되는 활동 데이터
    - contact_stream_attended_webinar: 연락처가 웨비나에 참석했을 때 트리거되는 활동 데이터
    - contact_stream_clicked_ad: 연락처가 광고를 클릭했을 때 트리거되는 활동 데이터
    - contact_stream_doesnt_receive_email: 연락처가 이메일을 받지 않을 때 트리거되는 활동 데이터
    - contact_stream_engaged_with_email: 연락처가 이메일과 상호작용했을 때 트리거되는 활동 데이터
    - contact_stream_generated_suspect: 연락처가 의심 대상으로 생성되었을 때 트리거되는 활동 데이터
    - contact_stream_opened_email: 연락처가 이메일을 열었을 때 트리거되는 활동 데이터
    - contact_stream_sdr_emailed_prospect: SDR이 잠재 고객에게 이메일을 보냈을 때 트리거되는 활동 데이터
    - contact_stream_sent_email: 연락처에게 이메일이 발송되었을 때 트리거되는 활동 데이터
    - contact_stream_unsubscribed_email: 연락처가 이메일 구독을 취소했을 때 트리거되는 활동 데이터
    - contact_stream_viewed_ad: 연락처가 광고를 보았을 때 트리거되는 활동 데이터

    ### 세션 관련 테이블
    - contact_stream_completed_session: 연락처가 세션을 완료했을 때 트리거되는 활동 데이터
    - contact_stream_started_session: 연락처가 세션을 시작했을 때 트리거되는 활동 데이터
    - contact_stream_visited_page: 연락처가 페이지를 방문했을 때 트리거되는 활동 데이터

    ### 평가판 관련 테이블
    - contact_stream_converted_suspect_to_prospect: 의심 대상이 잠재 고객으로 전환되었을 때 트리거되는 활동 데이터
    - contact_stream_converted_to_pql: 연락처가 PQL로 전환되었을 때 트리거되는 활동 데이터
    - contact_stream_ended_trial: 연락처의 평가판이 종료되었을 때 트리거되는 활동 데이터
    - contact_stream_started_trial: 연락처가 평가판을 시작했을 때 트리거되는 활동 데이터
    - contact_stream_started_true_trial: 연락처가 진정한 평가판을 시작했을 때 트리거되는 활동 데이터
    - contact_stream_trial_converted_to_customer: 연락처의 평가판이 고객으로 전환되었을 때 트리거되는 활동 데이터

    ### SDR 관련 테이블
    - contact_stream_sdr_accepted_prospect: SDR이 잠재 고객을 수락했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_called_prospect: SDR이 잠재 고객에게 전화했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_cold_outbounded_prospect: SDR이 잠재 고객에게 콜드 아웃바운드를 했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_created_business_opportunity: SDR이 비즈니스 기회를 생성했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_disqualified_prospect: SDR이 잠재 고객을 자격 박탈했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_qualified_prospect: SDR이 잠재 고객을 자격을 부여했을 때 트리거되는 활동 데이터
    - contact_stream_sdr_scheduled_intake_for_business_opportunity: SDR이 비즈니스 기회를 위한 인테이크를 예약했을 때 트리거되는 활동 데이터

    ### 양식 관련 테이블
    - contact_stream_interacted_with_form: 연락처가 양식과 상호작용했을 때 트리거되는 활동 데이터
    - contact_stream_submitted_form: 연락처가 양식을 제출했을 때 트리거되는 활동 데이터
    - contact_stream_viewed_form: 연락처가 양식을 보았을 때 트리거되는 활동 데이터

    ### 계약 관련 테이블
    - contact_stream_signed_contract: 연락처가 계약에 서명했을 때 트리거되는 활동 데이터
    
    
    """
)

DEAL_TABLE_NAMES_PROMPT = PromptTemplate.from_template(
    """
    
    # 역할: SQL쿼리 전문가야
    # 역할설명: 사용자의 질문에 가장 적합한 테이블을 선택해줘

    # 테이블 정보
    ## Deal 카테고리 테이블
    ### 기회 관련 테이블
    - deal_stream_created_expansion_opportunity: 새로운 확장 기회가 생성되었을 때 트리거되는 활동 데이터
    - deal_stream_created_new_opportunity: 새로운 기회가 생성되었을 때 트리거되는 활동 데이터
    - deal_stream_created_renewal_opportunity: 갱신 기회가 생성되었을 때 트리거되는 활동 데이터
    - deal_stream_disqualified_new_opportunity: 새로운 기회가 자격 박탈되었을 때 트리거되는 활동 데이터
    - deal_stream_lost_opportunity: 기회를 잃었을 때 트리거되는 활동 데이터
    - deal_stream_qualified_new_opportunity: 새로운 기회가 자격을 얻었을 때 트리거되는 활동 데이터
    - deal_stream_updated_close_date: 마감일이 업데이트되었을 때 트리거되는 활동 데이터
    - deal_stream_updated_deal_amount: 거래 금액이 업데이트되었을 때 트리거되는 활동 데이터
    - deal_stream_updated_win_probability: 승리 확률이 업데이트되었을 때 트리거되는 활동 데이터
    - deal_stream_won_opportunity: 기회가 성사되었을 때 트리거되는 활동 데이터

    ### 연락처 관련 테이블
    - deal_stream_called_contact: 거래에서 연락처에 전화했을 때 트리거되는 활동 데이터
    - deal_stream_emailed_contact: 거래에서 연락처에 이메일을 보냈을 때 트리거되는 활동 데이터
    - deal_stream_met_contact: 거래에서 연락처를 만났을 때 트리거되는 활동 데이터

    ### 데모 관련 테이블
    - deal_stream_completed_demo: 데모가 완료되었을 때 트리거되는 활동 데이터
    - deal_stream_scheduled_demo: 데모가 예약되었을 때 트리거되는 활동 데이터
    
    
    """
)


RETRIEVER_CHAIN_PROMPT = PromptTemplate.from_template(
    """
    
    # 역할: SQL쿼리 전문가
    ## 역할설명: 해당 테이블의 정보 바탕으로 사용자의 질문에 가장 적합한 SQL쿼리를 작성해줘

    # 테이블 정보: 
      {context}

    
    # 사용자 질문: 
      {question}
    
    """
)