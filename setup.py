# setup.py
from setuptools import setup, find_packages
import os
import glob

with open("docs/README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# 프롬프트 파일들을 찾습니다
prompt_files = glob.glob("prompt/*.md")
prompt_files = [os.path.basename(f) for f in prompt_files]

setup(
    name="lang2SQL",  # 패키지 이름
    version="0.1.9",  # 버전
    author="ehddnr301",
    author_email="dy95032@gmail.com",
    url="https://github.com/CausalInferenceLab/Lang2SQL",
    description="Lang2SQL - Query Generator for Data Warehouse",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),  # my_package를 자동으로 찾음
    packages=find_packages() + ["prompt"],  # prompt 패키지 직접 추가
    package_data={
        "prompt": ["*.md", "*.py"],  # prompt 디렉토리의 모든 .md 파일 포함
    },
    data_files=[("prompt", [os.path.join("prompt", f) for f in prompt_files])],
    include_package_data=True,
    install_requires=[
        "langgraph==0.2.62",
        "datahub==0.999.1",
        "langchain==0.3.14",
        "langchain-community==0.3.14",
        "openai==1.59.8",
        "langchain-openai==0.3.0",
        "streamlit==1.41.1",
        "python-dotenv==1.0.1",
        "faiss-cpu==1.10.0",
        "langchain-aws>=0.2.21,<0.3.0",
        "langchain-google-genai>=2.1.3,<3.0.0",
        "langchain-ollama>=0.3.2,<0.4.0",
        "langchain-huggingface>=0.1.2,<0.2.0",
        "transformers==4.51.2",
        "clickhouse-driver==0.2.9",
    ],
    entry_points={
        "console_scripts": [
            # "my-project" 명령어로 my_package.main 모듈의 run 함수를 실행
            "lang2sql = cli.__init__:cli"
        ]
    },
)
