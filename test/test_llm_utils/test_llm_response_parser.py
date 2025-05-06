"""
LLMResponseParser 클래스의 기능을 테스트하는 단위 테스트 모듈입니다.

주요 테스트 항목:
- <SQL> 블록에서 SQL 쿼리 추출 성공/실패
- <해석> 블록에서 자연어 설명 추출 성공/실패
- 다양한 입력 형식(들여쓰기, 공백 등)에 대한 정규식 대응 여부 확인
"""

import unittest

from llm_utils.llm_response_parser import LLMResponseParser


class TestLLMResponseParser(unittest.TestCase):
    """
    LLMResponseParser 클래스의 정적 메서드 동작을 검증하는 테스트 케이스입니다.

    각 테스트는 SQL 및 해석 블록 추출 기능이 정상적으로 작동하는지,
    예외 상황에 올바르게 대응하는지를 검증합니다.
    """

    def test_extract_sql_success(self):
        """
        <SQL> 블록과 ```sql``` 코드 블록이 정상적으로 포함된 문자열에서
        SQL 쿼리가 정확히 추출되는지 확인합니다.
        """

        text = """
        <SQL>
        ```sql
        SELECT * FROM users;
        ````

        <해석>

        ```plaintext
        사용자 테이블의 모든 데이터를 조회합니다.
        ```

        """
        expected_sql = "SELECT * FROM users;"
        result = LLMResponseParser.extract_sql(text)
        self.assertEqual(result, expected_sql)

    def test_extract_sql_missing(self):
        """
        <SQL> 블록은 존재하지만 코드 블록이 없을 경우,
        ValueError 예외가 발생하는지 확인합니다.
        """

        text = "<SQL> no code block here"
        with self.assertRaises(ValueError):
            LLMResponseParser.extract_sql(text)

    def test_extract_interpretation_success(self):
        """
        <해석> 블록과 ```plaintext``` 코드 블록이 포함된 문자열에서
        해석 텍스트가 정상적으로 추출되는지 확인합니다.
        """

        text = """
        ```

        <SQL>
        ```sql
        SELECT * FROM users;
        ```
        <해석>
        ```plaintext
        사용자 테이블의 모든 데이터를 조회합니다.
        ```
        """
        expected = "사용자 테이블의 모든 데이터를 조회합니다."
        result = LLMResponseParser.extract_interpretation(text)
        self.assertEqual(result, expected)

    def test_extract_interpretation_empty(self):
        """
        <해석> 태그는 존재하지만 코드 블록이 없는 경우,
        빈 문자열을 반환하는지 확인합니다.
        """

        text = "<해석> 블록이 없습니다."
        result = LLMResponseParser.extract_interpretation(text)
        self.assertEqual(result, "")

    def test_extract_sql_with_leading_whitespace(self):
        """
        <SQL> 블록이 들여쓰기되어 있는 경우에도 SQL 쿼리를 정확히 추출하는지 확인합니다.
        """

        text = """
        ```

        <SQL>
            ```sql
            SELECT id FROM orders;
            ```
        <해석>
        ```plaintext
        주문 테이블에서 ID 조회
        ```
        """
        expected = "SELECT id FROM orders;"
        result = LLMResponseParser.extract_sql(text)
        self.assertEqual(result, expected.strip())
