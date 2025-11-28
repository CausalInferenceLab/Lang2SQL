# ChatBot Module

LangGraph 기반의 대화형 챗봇 모듈입니다. 사용자의 자연어 질문을 이해하고, 적절한 가이드라인과 도구를 선택하여 답변을 생성합니다.

## 구조

```
utils/llm/chatbot/
├── __init__.py          # 패키지 초기화 및 ChatBot 클래스 export
├── core.py              # ChatBot 클래스 및 LangGraph 워크플로우 정의
├── guidelines.py        # 가이드라인 및 툴 래퍼 함수 정의
├── matcher.py           # LLM 기반 가이드라인 매칭 로직
└── types.py             # 데이터 타입 및 구조 정의
```

## 주요 컴포넌트

### `ChatBot` (`core.py`)
챗봇의 메인 클래스입니다. LangGraph를 사용하여 대화 흐름을 제어합니다.
- **초기화**: OpenAI API 키, 모델명, GMS 서버 URL 등을 설정합니다.
- **워크플로우**: `select_guidelines` -> `call_model` 순서로 실행됩니다.
- **chat 메서드**: 사용자 메시지를 입력받아 응답을 생성합니다.

### `LLMGuidelineMatcher` (`matcher.py`)
사용자의 메시지를 분석하여 가장 적절한 가이드라인을 선택하는 클래스입니다.
- LLM을 사용하여 사용자 의도를 파악하고, 미리 정의된 가이드라인 중 하나 이상을 매칭합니다.
- JSON Schema를 사용하여 구조화된 출력을 보장합니다.

### `Guideline` (`types.py`)
챗봇이 따를 규칙과 도구를 정의하는 데이터 클래스입니다.
- `id`: 가이드라인 식별자
- `description`: 가이드라인 설명
- `example_phrases`: 매칭에 사용될 예시 문구
- `tools`: 해당 가이드라인에서 사용할 도구 함수 목록
- `priority`: 매칭 우선순위

### `GUIDELINES` (`guidelines.py`)
기본적으로 제공되는 가이드라인 목록입니다.
- `table_schema`: 데이터베이스 테이블 정보 검색
- `glossary`: 용어집 조회
- `query_examples`: 쿼리 예제 조회

## 사용 예시

```python
from utils.llm.chatbot import ChatBot

# 챗봇 인스턴스 생성
bot = ChatBot(
    openai_api_key="sk-...",
    gms_server="http://localhost:8080"
)

# 대화하기
response = bot.chat("매출 테이블 정보 알려줘", thread_id="session_1")
print(response["messages"][-1].content)
```
