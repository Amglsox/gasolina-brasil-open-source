import json

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import datetime


default_args = {
    "owner": "lucas.mari",
    "depends_on_past": False,
    "email": ["lucashrm97@outlook.com"],
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_combustivel",
    default_args=default_args,
    description="Dag de carga de dados dos combustíveis",
    start_date=datetime(2004, 1, 1),
    schedule_interval="@once",
    tags=["combustivel"],
    max_active_runs=3,
) as dag:
    start_dag = DummyOperator(task_id="start_dag")
    fim_dag = DummyOperator(task_id="fim_dag")
    get_data = SimpleHttpOperator(
        task_id="get_data",
        method="POST",
        http_conn_id="api_data_challenge",
        endpoint="download_combustivel",
        data=json.dumps(
            {
                "remote_url": """https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/
                                ca-{{ dag_run.logical_date.strftime('%Y') }}
                                -{{ '01' if dag_run.logical_date.month <= 6 else '02'}}.csv""",
                "local_file": """./data/combustivel/
                                ca-{{ dag_run.logical_date.strftime('%Y') }}
                                -{{ '01' if dag_run.logical_date.month <= 6 else '02'}}.csv""",
            }
        ),
        headers={"Content-Type": "application/json"},
    )
    submit_job = DummyOperator(task_id="submit_job")
    start_dag >> get_data >> submit_job >> fim_dag
