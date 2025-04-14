import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()



def api_call_gpt(model_name="gpt-4o-mini"):
    
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
    
    
    return ChatOpenAI(
        model=model_name,
        # temperature=0,
        # streaming=True
    ) 


# print(gpt_response("한국날씨알려줘", model_name="gpt-4o-mini"))
