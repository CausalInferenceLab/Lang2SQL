TABLE_CATEGORY = [
    {
        "type": "function",
        "function": {
            
            "name": "text-to-sql",
            
            "description": """ 

            # 역할: DB SQL쿼리 생성 전문가
            # 역할 설명: 사용자가 DB에 대한 SQL쿼리를 물어보면 작동합니다.
            

            """,
            "parameters": {
                "type": "object",
                # 함수의 리턴을 정한다.
                "properties": {
                    "text": {
                        "type": "string",
                        "description": """입력 받은 키워드""",
                    },
                },
                "required": ["text"],
            },
        },
    },
    
]
