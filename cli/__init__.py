import click
import subprocess
from llm_utils.tools import set_gms_server
from langchain_core.messages import HumanMessage
from llm_evaluation.evaluator import Evaluator
from llm_utils.graph import builder
import os


@click.group()
@click.version_option(version="0.1.4")
@click.pass_context
@click.option(
    "--datahub_server", default="http://localhost:8080", help="Datahub GMS 서버 URL"
)
@click.option("--run-streamlit", is_flag=True, help="Run the Streamlit app.")
@click.option("-p", "--port", type=int, default=8501, help="Streamlit port")
@click.option("--mlflow-tracking-uri", default=None, help="MLflow 트래킹 서버 URI")
def cli(ctx, datahub_server, run_streamlit, port, mlflow_tracking_uri):
    try:
        set_gms_server(datahub_server)
    except ValueError as e:
        click.echo(str(e))
        ctx.exit(1)

    if mlflow_tracking_uri:
        os.environ["MLFLOW_TRACKING_URI"] = mlflow_tracking_uri

    if run_streamlit:
        run_streamlit_command(port)


def run_streamlit_command(port):
    """Run the Streamlit app."""
    subprocess.run(
        ["streamlit", "run", "interface/streamlit_app.py", "--server.port", str(port)]
    )


@cli.command()
@click.option("-p", "--port", type=int, default=8501, help="Streamlit port")
def run_streamlit(port):
    """Run the Streamlit app."""
    run_streamlit_command(port)


@cli.command()
@click.argument("dataset_path", type=click.Path(exists=True))
@click.option(
    "--user-database-env", default="clickhouse", help="사용자 데이터베이스 환경"
)
def evaluate(dataset_path, user_database_env):
    """SQL 생성 모델을 평가합니다."""
    click.echo(f"데이터셋 {dataset_path}로 평가를 시작합니다...")

    evaluator = Evaluator(dataset_path)

    def generated_sql_fn(question: str):
        graph = builder.compile()

        res = graph.invoke(
            input={
                "messages": [HumanMessage(content=question)],
                "user_database_env": user_database_env,
                "best_practice_query": "",
            }
        )

        return res["generated_query"].content

    results = evaluator.evaluate(generated_sql_fn)
    click.echo(f"평가 완료! {len(results)}개 쿼리 평가됨")
