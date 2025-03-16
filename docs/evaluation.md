# Evaluation

## MLFlow

### 실행

- `docker run --name mlflow_postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=mlflow_db -p 5430:5432 -d postgres`
- `mlflow server --backend-store-uri postgresql://postgres:postgres@localhost:5430/mlflow_db --default-artifact-root ./mlruns --host 0.0.0.0 --port 5000`
  - `backed-store-uri`: MLflow가 메타데이터를 저장하는 데이터베이스의 URI를 지정합니다. 예를 들어, PostgreSQL 데이터베이스를 사용할 경우 `postgresql://<username>:<password>@<hostname>:<port>/<database_name>` 형식으로 지정합니다.
  - `default-artifact-root`: MLflow가 모델 아티팩트를 저장할 기본 경로를 지정합니다. 예를 들어, 로컬 파일 시스템을 사용할 경우 `./mlruns`와 같이 지정할 수 있습니다.


## 평가 실행하기

### CLI 명령어

SQL 생성 모델을 평가하기 위해 다음과 같은 CLI 명령어를 사용할 수 있습니다:
```
lang2sql evaluate /path/to/dataset.json --user-database-env clickhouse
```

### 결과 예시

![MLFlow](./mlflow.png)
