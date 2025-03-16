import time
import mlflow
import os
import importlib.metadata
from langchain_core.messages import HumanMessage

import numpy as np
from llm_evaluation.dataset import QADataset
from llm_evaluation.llm_evaluator import compare_sql_with_llm
from llm_evaluation.mlflow_logger import log_to_mlflow


class Evaluator:
    def __init__(self, dataset_path):
        self.dataset = QADataset(dataset_path)

    def evaluate(self, generated_sql_fn):
        """Lang2SQL 평가 함수 (사용자가 SQL 생성 함수를 제공)

        각 평가 샘플은 nested run으로 기록됩니다.
        """
        results = []
        metrics_by_type = {}  # evaluation_type별 점수를 저장할 dict

        # MLflow 설정: tracking URI와 experiment 이름 설정
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
        try:
            lang2sql_version = importlib.metadata.version("lang2sql")
        except importlib.metadata.PackageNotFoundError:
            lang2sql_version = "unknown"
        experiment_name = f"lang2sql-evaluation-v{lang2sql_version}"
        mlflow.set_experiment(experiment_name)

        # 전체 평가를 하나의 부모 run으로 감싸기
        with mlflow.start_run(run_name="evaluation_run") as parent_run:
            for (
                question,
                ground_truth_sql,
                evaluation_type,
            ) in self.dataset.get_samples():
                start_time = time.time()
                generated_sql = generated_sql_fn(question)
                exec_time = time.time() - start_time

                # LLM 평가 결과 (현재 단일 점수)를 dict로 기록
                llm_score = compare_sql_with_llm(
                    generated_sql, ground_truth_sql, question
                )
                # evaluation_type별로 점수를 집계
                if evaluation_type not in metrics_by_type:
                    metrics_by_type[evaluation_type] = []
                metrics_by_type[evaluation_type].append(llm_score)

                feedback_data = {
                    "question": question,
                    "generated_sql": generated_sql,
                    "ground_truth_sql": ground_truth_sql,
                    "llm_evaluation_metric": llm_score,  # 각 쿼리별 metric은 dict 형태로 기록
                    "execution_time": exec_time,
                    "evaluation_type": evaluation_type,
                }

                # 각 샘플 평가를 nested run으로 기록 (run 이름으로 evaluation_type 사용)
                with mlflow.start_run(nested=True, run_name=str(evaluation_type)):
                    log_to_mlflow(
                        question,
                        generated_sql,
                        ground_truth_sql,
                        llm_score,
                        evaluation_type,
                    )
                    mlflow.log_metric("execution_time", exec_time)

                self.dataset.save_feedback(feedback_data)
                results.append(feedback_data)

            # 각 evaluation_type별로 집계한 metric 계산 (평균, 최고, 최저, 중앙값)
            aggregated_metrics = {}
            for eval_type, scores in metrics_by_type.items():

                for idx, score in enumerate(scores):
                    mlflow.log_metric(f"{eval_type}", score, step=idx)

            # aggregated_metrics를 태그로도 기록 (문자열로 변환)
            mlflow.set_tag("aggregated_metrics", str(aggregated_metrics))

        return results
