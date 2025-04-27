import os
import json
import csv
import pandas as pd

# 카테고리별 테이블이 저장된 디렉토리 경로
TABLES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'categorized_tables')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ddl_output')

def load_json_file(file_path):
    """JSON 파일을 로드합니다."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def convert_to_sql_type(column_name, description=""):
    """컬럼 이름과 설명을 바탕으로 적절한 SQL 타입을 추정합니다."""
    column_name = column_name.lower()
    description = description.lower()
    
    # ID 필드는 보통 정수형
    if column_name == 'id' or column_name.endswith('_id'):
        return 'INTEGER'
    
    # 타임스탬프 관련 필드
    if 'timestamp' in column_name or 'time' in column_name or 'date' in column_name or '_ts' in column_name:
        return 'TIMESTAMP'
    
    # 금액, 영향도 등 수치 관련 필드
    if 'amount' in column_name or 'impact' in column_name or 'revenue' in column_name or 'price' in column_name:
        return 'DECIMAL(18, 2)'
    
    # JSON 문자열 필드
    if 'json' in column_name or 'feature_json' in column_name:
        return 'JSON'
    
    # 확률 또는 비율 관련 필드
    if 'probability' in column_name or 'rate' in column_name or 'ratio' in column_name:
        return 'DECIMAL(5, 2)'
    
    # 그 외 문자열 필드는 기본적으로 VARCHAR로 설정
    return 'VARCHAR(255)'

def generate_ddl(table_name, columns):
    """테이블 이름과 컬럼 정보를 바탕으로 DDL 문을 생성합니다."""
    ddl = f"CREATE TABLE {table_name} (\n"
    
    # 컬럼 정의 추가
    for i, column in enumerate(columns):
        column_name = column['name']
        sql_type = convert_to_sql_type(column_name, column['description'])
        
        # 마지막 컬럼이 아니면 콤마 추가
        if i < len(columns) - 1:
            ddl += f"    {column_name} {sql_type},\n"
        else:
            ddl += f"    {column_name} {sql_type}\n"
    
    # 기본 키 설정 (id 컬럼이 있는 경우)
    if any(col['name'] == 'id' for col in columns):
        ddl += f"    , PRIMARY KEY (id)\n"
    
    ddl += ");"
    return ddl

def create_ddl_from_tables():
    """테이블 정보를 읽고 DDL을 생성합니다."""
    ddl_data = []
    
    # 디렉토리에서 모든 JSON 파일을 로드합니다
    for file_name in os.listdir(TABLES_DIR):
        if file_name.endswith('.json') and file_name != 'categories_meta.json':
            file_path = os.path.join(TABLES_DIR, file_name)
            category_name = file_name.replace('.json', '')
            
            try:
                # 파일 데이터 로드
                table_data = load_json_file(file_path)
                
                # 각 테이블에 대한 DDL 생성
                for table_key, table_info in table_data.items():
                    table_name = table_info.get('name', '')
                    if not table_name:
                        continue
                    
                    # 컬럼 정보 가져오기
                    columns = []
                    columns_info = table_info.get('columns', {})
                    for col_name, col_desc in columns_info.items():
                        columns.append({
                            'name': col_name,
                            'description': col_desc
                        })
                    
                    # DDL 생성
                    ddl = generate_ddl(table_name, columns)
                    
                    ddl_data.append({
                        'table_name': table_name,
                        'category': category_name,
                        'description': table_info.get('description', ''),
                        'ddl': ddl
                    })
            
            except Exception as e:
                print(f"Error processing {file_name}: {str(e)}")
    
    return ddl_data

def save_ddl_to_csv(ddl_data):
    """DDL 정보를 CSV 파일로 저장합니다."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, 'table_ddl.csv')
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['table_name', 'category', 'description', 'ddl'])
        
        for item in ddl_data:
            writer.writerow([
                item['table_name'],
                item['category'],
                item['description'],
                item['ddl']
            ])
    
    # DataFrame으로도 저장 (pandas 형식)
    df = pd.DataFrame(ddl_data)
    pd_csv_path = os.path.join(OUTPUT_DIR, 'table_ddl_pandas.csv')
    df.to_csv(pd_csv_path, index=False, encoding='utf-8')
    
    print(f"DDL CSV 파일이 생성되었습니다:")
    print(f"1. 일반 CSV: {csv_path}")
    print(f"2. Pandas CSV: {pd_csv_path}")

def save_ddl_to_sql_files(ddl_data):
    """각 테이블의 DDL을 개별 SQL 파일로 저장합니다."""
    sql_dir = os.path.join(OUTPUT_DIR, 'sql_files')
    os.makedirs(sql_dir, exist_ok=True)
    
    # 각 카테고리별 디렉토리 생성
    categories = set(item['category'] for item in ddl_data)
    for category in categories:
        category_dir = os.path.join(sql_dir, category)
        os.makedirs(category_dir, exist_ok=True)
    
    # 각 테이블별 SQL 파일 생성
    for item in ddl_data:
        category_dir = os.path.join(sql_dir, item['category'])
        sql_file_path = os.path.join(category_dir, f"{item['table_name']}.sql")
        
        with open(sql_file_path, 'w', encoding='utf-8') as f:
            f.write(f"-- {item['description']}\n")
            f.write(item['ddl'])
    
    print(f"각 테이블별 SQL DDL 파일이 {sql_dir} 디렉토리에 생성되었습니다.")

if __name__ == "__main__":
    # DDL 생성
    ddl_data = create_ddl_from_tables()
    
    # CSV 파일로 저장
    save_ddl_to_csv(ddl_data)
    
    # 개별 SQL 파일로 저장
    save_ddl_to_sql_files(ddl_data)
    
    # 샘플 DDL 출력 (3개)
    print("\n샘플 DDL 3개:")
    for i in range(min(3, len(ddl_data))):
        print(f"\n{ddl_data[i]['table_name']} DDL:")
        print(ddl_data[i]['ddl'])

"""
# DDL 샘플:

# 샘플 1: deal_stream_created_expansion_opportunity
CREATE TABLE deal_stream_created_expansion_opportunity (
    id INTEGER,
    entity_id INTEGER,
    activity_ts TIMESTAMP,
    activity VARCHAR(255),
    revenue_impact DECIMAL(18, 2),
    feature_json JSON
    , PRIMARY KEY (id)
);

# 샘플 2: contact_stream_attended_event
CREATE TABLE contact_stream_attended_event (
    id INTEGER,
    entity_id INTEGER,
    activity_ts TIMESTAMP,
    activity VARCHAR(255),
    revenue_impact DECIMAL(18, 2),
    feature_json JSON
    , PRIMARY KEY (id)
);

# 샘플 3: client_stream_active_on_subscription
CREATE TABLE client_stream_active_on_subscription (
    id INTEGER,
    entity_id INTEGER,
    activity_ts TIMESTAMP,
    activity VARCHAR(255),
    revenue_impact DECIMAL(18, 2),
    feature_json JSON
    , PRIMARY KEY (id)
);
""" 