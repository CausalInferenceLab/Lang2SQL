#!/usr/bin/env python3
import json
import os
import glob
from typing import Dict, Any, List

def determine_sql_type(column_name: str, description: str) -> str:
    """컬럼 이름과 설명을 기반으로 SQL 데이터 타입을 추정합니다."""
    column_name = column_name.lower()
    description = description.lower()
    
    # ID 필드는 기본적으로 INT로 처리
    if column_name == 'id' or column_name.endswith('_id'):
        return 'INT'
    
    # 날짜/시간 관련 필드
    if any(term in column_name for term in ['date', 'time', 'ts', 'timestamp', 'updated_at', 'created_at']):
        return 'TIMESTAMP'
    
    # 숫자 관련 필드
    if any(term in column_name for term in ['amount', 'price', 'cost', 'revenue', 'impact', 'count', 'number']):
        if 'decimal' in description or 'money' in description or 'revenue' in description:
            return 'DECIMAL(10, 2)'
        return 'INT'
    
    # 불리언 필드
    if any(term in column_name for term in ['is_', 'has_', 'flag']):
        return 'BOOLEAN'
    
    # JSON 필드
    if 'json' in column_name or 'json' in description:
        return 'JSON'
    
    # 기본값은 VARCHAR
    return 'VARCHAR(255)'

def generate_ddl_from_json(json_file_path: str) -> str:
    """JSON 파일에서 테이블 정보를 읽어 DDL 스크립트를 생성합니다."""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"파일 읽기 에러 ({json_file_path}): {e}")
        return ""

    ddl_lines = []
    file_name = os.path.basename(json_file_path)
    ddl_lines.append(f"-- DDL for {file_name}")
    ddl_lines.append("")
    
    for table_id, table_info in data.items():
        table_name = table_info.get('name', f"table_{table_id}")
        table_desc = table_info.get('description', '')
        
        create_table_lines = [f"CREATE TABLE {table_name} ("]
        columns = table_info.get('columns', {})
        
        column_lines = []
        primary_key = None
        
        for column_name, description in columns.items():
            sql_type = determine_sql_type(column_name, description)
            
            # ID 컬럼을 기본 키로 설정
            if column_name.lower() == 'id':
                primary_key = column_name
                column_lines.append(f"    {column_name} {sql_type} NOT NULL, -- {description}")
            else:
                column_lines.append(f"    {column_name} {sql_type}, -- {description}")
        
        # PRIMARY KEY 제약 조건 추가
        if primary_key:
            column_lines.append(f"    PRIMARY KEY ({primary_key})")
        
        create_table_lines.append(",\n".join(column_lines))
        create_table_lines.append(");")
        
        if table_desc:
            create_table_lines.append(f"\nCOMMENT ON TABLE {table_name} IS '{table_desc}';")
        
        # 컬럼 설명 코멘트 추가
        for column_name, description in columns.items():
            create_table_lines.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{description}';")
        
        ddl_lines.append("\n".join(create_table_lines))
        ddl_lines.append("\n")
    
    return "\n".join(ddl_lines)

def main():
    input_dir = "/Users/sbk/psuedo_lab/Lang2SQL/RAG/data_utils/categorized_tables"
    output_dir = "/Users/sbk/psuedo_lab/Lang2SQL/DDL_scripts"
    
    # 출력 디렉토리가 없으면 생성
    os.makedirs(output_dir, exist_ok=True)
    
    # 모든 JSON 파일 처리
    all_json_files = glob.glob(os.path.join(input_dir, "*.json"))
    
    # 전체 DDL을 합칠 파일
    combined_ddl_path = os.path.join(output_dir, "all_tables_ddl.sql")
    with open(combined_ddl_path, 'w', encoding='utf-8') as combined_file:
        
        for json_file in all_json_files:
            file_name = os.path.basename(json_file)
            print(f"처리 중: {file_name}")
            
            # 개별 DDL 생성
            ddl_content = generate_ddl_from_json(json_file)
            
            if ddl_content:
                # 개별 파일 저장
                output_file = os.path.join(output_dir, f"{os.path.splitext(file_name)[0]}_ddl.sql")
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(ddl_content)
                print(f"생성됨: {output_file}")
                
                # 전체 DDL 파일에 추가
                combined_file.write(ddl_content)
                combined_file.write("\n\n")
    
    print(f"DDL 스크립트 생성 완료. 결과는 {output_dir} 디렉토리에 저장되었습니다.")
    print(f"모든 테이블의 DDL은 {combined_ddl_path}에 통합되었습니다.")

if __name__ == "__main__":
    main() 