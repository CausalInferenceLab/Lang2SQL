#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import re
from collections import defaultdict

def load_tables():
    """
    dbt_tables_info.json 파일에서 테이블 정보를 로드합니다.
    """
    with open('dbt_tables_info.json', 'r', encoding='utf-8') as f:
        tables = json.load(f)
    return tables

def categorize_tables(tables):
    """
    테이블을 카테고리별로 분류합니다.
    """
    # 카테고리 정의
    categories = {
        # 클라이언트 활동 관련 카테고리
        'client_subscription': [],        # 구독 관련
        'client_support': [],            # 지원 티켓 관련
        'client_contract': [],           # 계약 관련
        'client_onboarding': [],         # 온보딩 관련
        'client_survey': [],             # 설문 조사 관련
        'client_service': [],            # 서비스 관련
        
        # 회사 관련 카테고리
        'company_sales_team': [],        # 영업팀 관련 (AE, SDR)
        'company_quota': [],             # 할당량 관련
        
        # 연락처 활동 관련 카테고리
        'contact_marketing': [],         # 마케팅 관련 (이메일, 광고, 이벤트)
        'contact_session': [],           # 세션 관련
        'contact_trial': [],             # 평가판 관련
        'contact_sdr': [],               # SDR 관련
        'contact_form': [],              # 양식 관련
        'contact_contract': [],          # 계약 관련
        
        # 거래 관련 카테고리
        'deal_opportunity': [],          # 기회 관련
        'deal_contact': [],              # 연락처 관련
        'deal_demo': [],                 # 데모 관련
        
        # 분석 관련 카테고리
        'analytics_revenue': [],         # 수익 관련 분석
        'analytics_subscription': [],    # 구독 관련 분석
        'analytics_churn': [],           # 이탈 관련 분석
        'analytics_growth': [],          # 성장 관련 분석
        'analytics_retention': [],       # 유지 관련 분석
        
        # 기타
        'entity_model': [],              # 엔티티 모델 (contact, customer 등)
        'empty_description': []          # 설명이 없는 테이블
    }
    
    # 패턴 매칭을 통한 분류
    for table_id, table_info in tables.items():
        table_name = table_info['name']
        description = table_info['description']
        columns = table_info['columns']
        
        # 설명이 없는 테이블은 별도 분류
        if not description and all(not desc for desc in columns.values()):
            categories['empty_description'].append(table_id)
            continue
        
        # 테이블 이름 기반 분류
        if table_name.startswith('client_stream_'):
            if any(keyword in table_name for keyword in ['subscription', 'active_on_subscription']):
                categories['client_subscription'].append(table_id)
            elif any(keyword in table_name for keyword in ['support_ticket', 'called_support']):
                categories['client_support'].append(table_id)
            elif any(keyword in table_name for keyword in ['contract', 'expanded', 'decreased', 'renewed', 'resurrected']):
                categories['client_contract'].append(table_id)
            elif any(keyword in table_name for keyword in ['onboarded', 'onboarding']):
                categories['client_onboarding'].append(table_id)
            elif any(keyword in table_name for keyword in ['survey', 'ces', 'csat', 'nps']):
                categories['client_survey'].append(table_id)
            elif any(keyword in table_name for keyword in ['service', 'ordered_service', 'incurred_overage']):
                categories['client_service'].append(table_id)
            else:
                # 다른 client_stream 테이블은 가장 가까운 카테고리에 할당
                categories['client_subscription'].append(table_id)
                
        elif table_name.startswith('company_stream_'):
            if any(keyword in table_name for keyword in ['ae', 'sdr', 'hired', 'offboarded', 'onboarded', 'ramped']):
                categories['company_sales_team'].append(table_id)
            elif any(keyword in table_name for keyword in ['quota', 'accrued']):
                categories['company_quota'].append(table_id)
            else:
                categories['company_sales_team'].append(table_id)
                
        elif table_name.startswith('contact_stream_'):
            if any(keyword in table_name for keyword in ['email', 'ad', 'event', 'webinar']):
                categories['contact_marketing'].append(table_id)
            elif any(keyword in table_name for keyword in ['session', 'visited', 'page']):
                categories['contact_session'].append(table_id)
            elif any(keyword in table_name for keyword in ['trial', 'converted']):
                categories['contact_trial'].append(table_id)
            elif any(keyword in table_name for keyword in ['sdr', 'prospect', 'opportunity', 'qualified']):
                categories['contact_sdr'].append(table_id)
            elif any(keyword in table_name for keyword in ['form', 'submitted']):
                categories['contact_form'].append(table_id)
            elif any(keyword in table_name for keyword in ['contract', 'signed']):
                categories['contact_contract'].append(table_id)
            else:
                categories['contact_marketing'].append(table_id)
                
        elif table_name.startswith('deal_stream_'):
            if any(keyword in table_name for keyword in ['opportunity', 'expansion', 'renewal', 'won', 'lost', 'qualified']):
                categories['deal_opportunity'].append(table_id)
            elif any(keyword in table_name for keyword in ['contact', 'called', 'emailed', 'met']):
                categories['deal_contact'].append(table_id)
            elif any(keyword in table_name for keyword in ['demo', 'scheduled']):
                categories['deal_demo'].append(table_id)
            else:
                categories['deal_opportunity'].append(table_id)
                
        elif table_name.startswith('ga_cube_'):
            if any(keyword in table_name for keyword in ['revenue', 'dollar']):
                categories['analytics_revenue'].append(table_id)
            elif any(keyword in table_name for keyword in ['subscription', 'subscriber']):
                categories['analytics_subscription'].append(table_id)
            elif any(keyword in table_name for keyword in ['churn']):
                categories['analytics_churn'].append(table_id)
            elif any(keyword in table_name for keyword in ['growth', 'new', 'cmgr']):
                categories['analytics_growth'].append(table_id)
            elif any(keyword in table_name for keyword in ['retention', 'retained', 'resurrected']):
                categories['analytics_retention'].append(table_id)
            else:
                categories['analytics_revenue'].append(table_id)
                
        elif table_name in ['contact', 'customer', 'growth_accounting_cube', 'quick_ratio_cube', 'client_activity_stream']:
            categories['entity_model'].append(table_id)
        else:
            # 매칭되지 않는 테이블은 연관성 있는 카테고리에 할당
            if 'revenue' in table_name or 'revenue' in description.lower():
                categories['analytics_revenue'].append(table_id)
            elif 'subscription' in table_name or 'subscription' in description.lower():
                categories['analytics_subscription'].append(table_id)
            else:
                categories['empty_description'].append(table_id)
                
    # 빈 카테고리 제거
    return {k: v for k, v in categories.items() if v}

def save_categorized_tables(tables, categories):
    """
    카테고리별로 테이블을 저장합니다.
    """
    # categories_dir이 없으면 생성
    categories_dir = 'categorized_tables'
    if not os.path.exists(categories_dir):
        os.makedirs(categories_dir)
    
    # 각 카테고리별로 JSON 파일 저장
    for category, table_ids in categories.items():
        category_tables = {}
        for table_id in table_ids:
            table_key = f"{table_id.split('_', 1)[1]}"  # table_001_ 부분 제거
            category_tables[table_key] = tables[table_id]
        
        # 카테고리별 JSON 파일 저장
        file_path = os.path.join(categories_dir, f"{category}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(category_tables, f, ensure_ascii=False, indent=2)
        
        print(f"카테고리 '{category}'에 {len(table_ids)}개 테이블이 저장되었습니다. (파일: {file_path})")
    
    # 모든 카테고리 정보를 담은 메타 파일 저장
    meta = {}
    for category, table_ids in categories.items():
        meta[category] = {
            "count": len(table_ids),
            "tables": [tables[table_id]["name"] for table_id in table_ids]
        }
        
    meta_file_path = os.path.join(categories_dir, "categories_meta.json")
    with open(meta_file_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    
    print(f"\n카테고리 메타 정보가 저장되었습니다. (파일: {meta_file_path})")

def main():
    """
    메인 함수
    """
    print("테이블 정보 로딩 중...")
    tables = load_tables()
    print(f"총 {len(tables)}개 테이블을 로드했습니다.")
    
    print("\n테이블 카테고리화 중...")
    categories = categorize_tables(tables)
    total_categorized = sum(len(tables) for tables in categories.values())
    print(f"총 {len(categories)}개 카테고리로 {total_categorized}개 테이블을 분류했습니다.")
    
    print("\n카테고리별 JSON 파일 저장 중...")
    save_categorized_tables(tables, categories)
    print("\n카테고리화 완료!")

if __name__ == "__main__":
    main() 