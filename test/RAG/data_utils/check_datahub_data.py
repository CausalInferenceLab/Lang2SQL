#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import re
from pprint import pprint
from datahub_source import DatahubMetadataFetcher

def main():
    """데이터허브에서 데이터를 불러와 확인하는 메인 함수"""
    # 데이터허브 연결 정보
    gms_server = "http://34.125.222.80:8080"
    
    # 인증 정보 헤더 설정 (Basic Auth)
    import base64
    auth_header = base64.b64encode(b"datahub:datahub").decode("utf-8")
    extra_headers = {"Authorization": f"Basic {auth_header}"}
    
    print(f"데이터허브({gms_server})에 연결 시도 중...")
    
    try:
        # 데이터허브에 연결
        fetcher = DatahubMetadataFetcher(gms_server=gms_server, extra_headers=extra_headers)
        print("데이터허브 연결 성공!")
        
        # 모든 데이터셋(테이블) URN 가져오기 - generator를 리스트로 변환
        urns = list(fetcher.get_urns())
        print(f"\n총 {len(urns)}개의 엔티티를 발견했습니다.\n")
        
        # dataset이 포함된 URN만 필터링
        dataset_urns = [urn for urn in urns if 'dataset' in urn.lower()]
        print(f"데이터셋 URN: {len(dataset_urns)}개")
        
        # DBT 테이블 URN 필터링
        dbt_matches = []
        for urn in dataset_urns:
            urn_lower = urn.lower()
            # DBT 패턴 확인 (데이터허브의 URN 형식에 맞게)
            if 'dbt' in urn_lower:
                dbt_matches.append(urn)
        
        # 전체 115개 중 현재 찾은 개수 표시
        print(f"\nDBT 관련 데이터셋: {len(dbt_matches)}개 / 115개 예상")
        
        # DBT 테이블 정보 저장
        all_tables_info = {}
        processed_tables = 0
        
        print("\nDBT 테이블 처리 중...")
        
        for i, urn in enumerate(dbt_matches):
            table_name = fetcher.get_table_name(urn)
            table_desc = fetcher.get_table_description(urn)
            
            # 테이블 정보가 없으면 건너뜀
            if not table_name:
                continue
                
            # 컬럼 정보 가져오기
            columns = fetcher.get_column_names_and_descriptions(urn)
            columns_dict = {}
            for col in columns:
                columns_dict[col['column_name']] = col['column_description'] or ''
            
            # 테이블 정보 저장 (테이블 번호 매김)
            processed_tables += 1
            table_key = f"table_{processed_tables:03d}_{table_name}"
            
            all_tables_info[table_key] = {
                'name': table_name,
                'description': table_desc or '',
                'columns': columns_dict
            }
            
            # 진행 상황 표시 (20개마다)
            if processed_tables % 20 == 0:
                print(f"  {processed_tables}개 테이블 처리 완료...")
        
        # JSON 파일로 저장
        filename = "dbt_tables_info.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_tables_info, f, ensure_ascii=False, indent=2)
        
        print(f"\nDBT 테이블 정보 {processed_tables}개가 '{filename}' 파일에 저장되었습니다.")
        print(f"(URN 정보 제외, 테이블 번호 매김 적용)")
            
    except Exception as e:
        print(f"오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 