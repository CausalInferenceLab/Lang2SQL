import os
import json
import pandas as pd

# 카테고리별 테이블이 저장된 디렉토리 경로
TABLES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'categorized_tables')

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
    
    # 디렉토리에서 모든 JSON 파일을 로드합니다
    for file_name in os.listdir(TABLES_DIR):
        if file_name.endswith('.json'):
            file_path = os.path.join(TABLES_DIR, file_name)
            category_name = file_name.replace('.json', '')
            
            try:
                # 파일 데이터 로드
                table_data = load_json_file(file_path)
                
                # 카테고리 메타데이터가 있으면 저장
                if category_name == 'categories_meta':
                    categories_metadata = table_data
                    continue
                
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
                        'columns': [],
                        'search_terms': [table_name.replace('_', ' ')]  # 검색어 초기화
                    }
                    
                    # 컬럼 정보 추가
                    columns_info = table_info.get('columns', {})
                    for col_name, col_desc in columns_info.items():
                        metadata_item['columns'].append({
                            'name': col_name,
                            'description': col_desc
                        })
                        # 컬럼 이름을 검색어에 추가
                        metadata_item['search_terms'].append(col_name.replace('_', ' '))
                    
                    # 카테고리를 검색어에 추가
                    metadata_item['search_terms'].append(category_name.replace('_', ' '))
                    
                    # 콘텐츠 필드는 모든 텍스트를 결합
                    metadata_item['content'] = f"{table_name} - {metadata_item['description']} - {' '.join([col['description'] for col in metadata_item['columns']])}"
                    
                    metadata_list.append(metadata_item)
            
            except Exception as e:
                print(f"Error processing {file_name}: {str(e)}")
    
    # 카테고리별 추가 정보 병합
    if categories_metadata:
        for item in metadata_list:
            category = item['category']
            if category in categories_metadata:
                item['category_info'] = {
                    'count': categories_metadata[category]['count'],
                    'related_tables': categories_metadata[category]['tables']
                }
                # 관련 테이블을 검색어에 추가
                for related_table in categories_metadata[category]['tables']:
                    if related_table not in item['search_terms']:
                        item['search_terms'].append(related_table.replace('_', ' '))
    
    return metadata_list

def save_metadata_to_json(metadata, output_path):
    """메타데이터를 JSON 파일로 저장합니다."""
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"메타데이터가 {output_path}에 저장되었습니다.")

def save_metadata_to_csv(metadata, output_path):
    """메타데이터를 CSV 파일로 저장합니다."""
    # 중첩 구조를 평면화하여 DataFrame으로 변환
    flattened_data = []
    
    for item in metadata:
        flat_item = {
            'table_name': item['table_name'],
            'category': item['category'],
            'description': item['description'],
            'content': item['content'],
            'search_terms': ', '.join(item['search_terms'])
        }
        
        # 컬럼 정보를 문자열로 변환
        columns_str = []
        for col in item['columns']:
            columns_str.append(f"{col['name']}: {col['description']}")
        flat_item['columns'] = '|'.join(columns_str)
        
        # 카테고리 정보가 있으면 추가
        if 'category_info' in item:
            flat_item['category_count'] = item['category_info']['count']
            flat_item['related_tables'] = ', '.join(item['category_info']['related_tables'])
        
        flattened_data.append(flat_item)
    
    # DataFrame 생성 및 저장
    df = pd.DataFrame(flattened_data)
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"메타데이터가 {output_path}에 저장되었습니다.")

if __name__ == "__main__":
    # 메타데이터 생성
    metadata = create_table_metadata()
    
    # 결과 디렉토리 생성
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'metadata')
    os.makedirs(output_dir, exist_ok=True)
    
    # JSON 및 CSV 파일로 저장
    save_metadata_to_json(metadata, os.path.join(output_dir, 'table_metadata.json'))
    save_metadata_to_csv(metadata, os.path.join(output_dir, 'table_metadata.csv')) 