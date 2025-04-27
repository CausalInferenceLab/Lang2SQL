import subprocess

import click

from llm_utils.tools import set_gms_server


@click.group()
@click.version_option(version="0.1.4")
@click.pass_context
@click.option(
    "--datahub_server", default="http://localhost:8080", help="Datahub GMS 서버 URL"
)
@click.option("--run-streamlit", is_flag=True, help="Run the Streamlit app.")
@click.option("-p", "--port", type=int, default=8501, help="Streamlit port")
def cli(ctx, datahub_server, run_streamlit, port):
    try:
        set_gms_server(datahub_server)
    except ValueError as e:
        click.echo(str(e))
        ctx.exit(1)
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
