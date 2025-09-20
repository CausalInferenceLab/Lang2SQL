# 1. Base image
FROM python:3.12-slim-bullseye

# 2. 시스템 라이브러리 설치
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 3. uv 설치
RUN pip install --no-cache-dir uv

# 4. 작업 디렉토리 설정
WORKDIR /app

# 5. 소스 코드 복사 및 의존성 설치
COPY pyproject.toml ./
COPY . .
RUN uv pip install --system --upgrade pip setuptools wheel \
    && uv pip install --system .

# 6. 환경 변수 설정
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# 7. 포트 설정
ENV STREAMLIT_SERVER_PORT=8501

# 8. 실행 명령
CMD ["lang2sql", "run-streamlit"]
