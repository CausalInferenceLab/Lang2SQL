import os
import json
import pandas as pd
import csv

# 카테고리별 테이블이 저장된 디렉토리 경로
TABLES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'categorized_tables')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'metadata')

def load_json_file(file_path):
    """JSON 파일을 로드합니다."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def create_table_metadata():
    """
    모든 테이블 파일을 읽고 메타데이터를 생성합니다.
    테이블 이름으로 검색할 수 있도록 최적화됩니다.
    """
    metadata_list = []
    categories_metadata = {}
    
    # 카테고리 메타데이터 먼저 로드
    categories_meta_path = os.path.join(TABLES_DIR, 'categories_meta.json')
    if os.path.exists(categories_meta_path):
        categories_metadata = load_json_file(categories_meta_path)
    
    # 디렉토리에서 모든 JSON 파일을 로드합니다
    for file_name in os.listdir(TABLES_DIR):
        if file_name.endswith('.json') and file_name != 'categories_meta.json':
            file_path = os.path.join(TABLES_DIR, file_name)
            category_name = file_name.replace('.json', '')
            
            try:
                # 파일 데이터 로드
                table_data = load_json_file(file_path)
                
                # 각 테이블에 대한 메타데이터 생성
                for table_key, table_info in table_data.items():
                    table_name = table_info.get('name', '')
                    if not table_name:
                        continue
                    
                    # 메타데이터 항목 생성
                    metadata_item = {
                        'table_name': table_name,
                        'category': category_name,
                        'description': table_info.get('description', ''),
                        'columns': []
                    }
                    
                    # 테이블 설명에 카테고리 정보 추가
                    if category_name in categories_metadata:
                        related_tables = categories_metadata[category_name].get('tables', [])
                        category_count = categories_metadata[category_name].get('count', 0)
                        metadata_item['category_count'] = category_count
                        metadata_item['related_tables'] = related_tables
                    
                    # 컬럼 정보 추가
                    columns_info = table_info.get('columns', {})
                    for col_name, col_desc in columns_info.items():
                        metadata_item['columns'].append({
                            'name': col_name,
                            'description': col_desc
                        })
                    
                    metadata_list.append(metadata_item)
            
            except Exception as e:
                print(f"Error processing {file_name}: {str(e)}")
    
    return metadata_list

def save_metadata_to_csv(metadata):
    """메타데이터를 CSV 파일로 저장합니다."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 기본 메타데이터를 저장할 CSV 파일
    basic_csv_path = os.path.join(OUTPUT_DIR, 'table_metadata_basic.csv')
    
    # 컬럼 정보를 저장할 CSV 파일
    columns_csv_path = os.path.join(OUTPUT_DIR, 'table_metadata_columns.csv')
    
    # 관련 테이블 정보를 저장할 CSV 파일
    related_tables_csv_path = os.path.join(OUTPUT_DIR, 'table_metadata_related.csv')
    
    # 기본 메타데이터 CSV 생성
    with open(basic_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['table_name', 'category', 'description', 'category_count'])
        
        for item in metadata:
            writer.writerow([
                item['table_name'],
                item['category'],
                item['description'],
                item.get('category_count', '')
            ])
    
    # 컬럼 정보 CSV 생성
    with open(columns_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['table_name', 'column_name', 'column_description'])
        
        for item in metadata:
            table_name = item['table_name']
            for column in item['columns']:
                writer.writerow([
                    table_name,
                    column['name'],
                    column['description']
                ])
    
    # 관련 테이블 CSV 생성
    with open(related_tables_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['table_name', 'category', 'related_table'])
        
        for item in metadata:
            table_name = item['table_name']
            category = item['category']
            related_tables = item.get('related_tables', [])
            
            for related_table in related_tables:
                writer.writerow([
                    table_name,
                    category,
                    related_table
                ])
    
    # 통합 버전 (DataFrame 사용)
    flattened_data = []
    
    for item in metadata:
        # 컬럼 정보를 문자열로 변환
        columns_str = []
        for col in item['columns']:
            columns_str.append(f"{col['name']}: {col['description']}")
        
        # 관련 테이블 목록 (있는 경우)
        related_tables_str = ", ".join(item.get('related_tables', []))
        
        flat_item = {
            'table_name': item['table_name'],
            'category': item['category'],
            'description': item['description'],
            'category_count': item.get('category_count', ''),
            'columns': '|'.join(columns_str),
            'related_tables': related_tables_str
        }
        
        flattened_data.append(flat_item)
    
    # DataFrame 생성 및 저장
    df = pd.DataFrame(flattened_data)
    combined_csv_path = os.path.join(OUTPUT_DIR, 'table_metadata_combined.csv')
    df.to_csv(combined_csv_path, index=False, encoding='utf-8')
    
    print(f"메타데이터 CSV 파일이 생성되었습니다:")
    print(f"1. 기본 메타데이터: {basic_csv_path}")
    print(f"2. 컬럼 정보: {columns_csv_path}")
    print(f"3. 관련 테이블: {related_tables_csv_path}")
    print(f"4. 통합 정보: {combined_csv_path}")

def save_metadata_to_json(metadata):
    """메타데이터를 JSON 파일로 저장합니다."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    json_path = os.path.join(OUTPUT_DIR, 'table_metadata.json')
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    print(f"메타데이터 JSON 파일이 생성되었습니다: {json_path}")

if __name__ == "__main__":
    # 메타데이터 생성
    metadata = create_table_metadata()
    
    # CSV 파일로 저장
    save_metadata_to_csv(metadata)
    
    # JSON 파일로도 저장
    save_metadata_to_json(metadata) 