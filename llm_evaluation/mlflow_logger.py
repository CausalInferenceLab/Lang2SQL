import os
import importlib.metadata
import mlflow
from dotenv import load_dotenv

load_dotenv()


def log_to_mlflow(
    question, generated_sql, ground_truth_sql, llm_metric, evaluation_type=None
):
    """활성화된 run 내에서 평가 결과 기록

    llm_metric은 dict 형태로 기록되며, mlflow.log_param은 내부적으로 문자열로 저장됩니다.
    """
    mlflow.log_param("question", question)
    mlflow.log_param("generated_sql", generated_sql)
    mlflow.log_param("ground_truth_sql", ground_truth_sql)
    mlflow.log_param("llm_evaluation_metric", llm_metric)
    if evaluation_type is not None:
        mlflow.log_param("evaluation_type", evaluation_type)
