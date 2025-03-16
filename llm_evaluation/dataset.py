import json
import pandas as pd
import os


class QADataset:
    def __init__(self, dataset_path):
        self.dataset_path = dataset_path
        self.data = self._load_dataset()
        self.results_path = dataset_path.replace(".json", "_results.json")

    def _load_dataset(self):
        """JSON 파일에서 Question-Answer 데이터셋을 로드"""
        with open(self.dataset_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame.from_dict(data)
        else:
            raise ValueError(
                "지원되지 않는 JSON 형식입니다. 리스트 또는 딕셔너리 형식이어야 합니다."
            )

    def get_samples(self):
        """원본 데이터셋의 질문, 정답 SQL, 평가 타입(evaluation_type) 정보를 반환"""
        if "evaluation_type" in self.data.columns:
            eval_types = self.data["evaluation_type"].tolist()
        else:
            eval_types = [None] * len(self.data["inputs"])

        for i in range(len(self.data["inputs"])):
            yield self.data["inputs"][i], self.data["ground_truths"][i], eval_types[i]

    def save_feedback(self, feedback_data):
        """평가 결과를 별도 파일에 저장"""
        results = []
        if os.path.exists(self.results_path):
            with open(self.results_path, "r", encoding="utf-8") as f:
                results = json.load(f)

        results.append(feedback_data)

        with open(self.results_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
