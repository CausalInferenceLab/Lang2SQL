# Python 3.10 slim 이미지 기반
FROM python:3.12-slim

# 시스템 라이브러리 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# 의존성 파일 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 전체 서비스 코드 복사
COPY . .

# Python 환경 설정
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Streamlit 포트 노출
EXPOSE 8501

# Streamlit 실행 명령
CMD ["python", "-c", "from llm_utils.tools import set_gms_server; import os; set_gms_server(os.getenv('DATAHUB_SERVER', 'http://localhost:8080'))"]
CMD ["streamlit", "run", "./interface/streamlit_app.py", "--server.port=8501"] 