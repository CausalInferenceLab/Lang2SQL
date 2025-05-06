"""
LLM 응답 텍스트에서 특정 마크업 태그(`<SQL>`, `<해석>`)에 포함된 콘텐츠 블록을 추출하는 유틸리티 모듈입니다.

이 모듈은 OpenAI, LangChain 등에서 생성된 LLM 응답 문자열에서 Markdown 코드 블록을 파싱하여,
SQL 쿼리 및 자연어 해석 설명을 분리하여 사용할 수 있도록 정적 메서드 형태의 API를 제공합니다.

지원되는 태그:
    - <SQL>: SQL 코드 블록 (```sql ... ```)
    - <해석>: 자연어 해석 블록 (```plaintext ... ```)
"""

import re


class LLMResponseParser:
    """
    LLM 응답 문자열에서 특정 태그(<SQL>, <해석>)에 포함된 블록을 추출하는 유틸리티 클래스입니다.

    주요 기능:
        - <SQL> 태그 내 ```sql ... ``` 블록에서 SQL 쿼리 추출
        - <해석> 태그 내 ```plaintext ... ``` 블록에서 자연어 해석 추출
    """

    @staticmethod
    def extract_sql(text: str) -> str:
        """
        <SQL> 태그 내부의 SQL 코드 블록만 추출합니다.

        Args:
            text (str): 전체 LLM 응답 문자열.

        Returns:
            str: SQL 쿼리 문자열 (```sql ... ``` 내부 텍스트).

        Raises:
            ValueError: <SQL> 태그 또는 SQL 코드 블록을 찾을 수 없는 경우.
        """
        match = re.search(r"<SQL>\s*```sql\n(.*?)```", text, re.DOTALL)
        if match:
            return match.group(1).strip()
        raise ValueError("SQL 블록을 추출할 수 없습니다.")

    @staticmethod
    def extract_interpretation(text: str) -> str:
        """
        <해석> 태그 내부의 해석 설명 텍스트만 추출합니다.

        Args:
            text (str): 전체 LLM 응답 문자열.

        Returns:
            str: 해석 설명 텍스트. 블록이 존재하지 않으면 빈 문자열을 반환합니다.
        """
        match = re.search(r"<해석>\s*```plaintext\n(.*?)```", text, re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""
