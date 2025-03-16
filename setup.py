# setup.py
from setuptools import setup, find_packages

with open("docs/README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="lang2sql",  # 패키지 이름
    version="0.1.9",  # 버전
    author="ehddnr301",
    author_email="dy95032@gmail.com",
    url="https://github.com/CausalInferenceLab/Lang2SQL",
    description="Lang2SQL - Query Generator for Data Warehouse",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),  # my_package를 자동으로 찾음
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
        "mlflow==2.21.0",
    ],
    entry_points={
        "console_scripts": [
            # "my-project" 명령어로 my_package.main 모듈의 run 함수를 실행
            "lang2sql = cli.__init__:cli"
        ]
    },
)
