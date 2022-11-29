from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import datetime


default_args = {
    "owner": "fulano.de.tal",
    "depends_on_past": False,
    "email": ["fulano.de.tal@avenuecode"],
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_combustivel",
    default_args=default_args,
    description="Dag de carga de dados dos combustíveis",
    start_date=datetime(1994, 1, 1),
    schedule_interval="@once",
    tags=["combustivel"],
    max_active_runs=3,
) as dag:
    """
    1 - Crie os operadores de chamada
        HTTP https://airflow.apache.org/docs/apache-airflow-providers-http/stable/operators.html
    2 - Crie os operadores de criação de cluster
        DataProc https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html
    3 - Crie os operadores de submit job pyspark no
        DataProc https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html
    4 - Crie os operadores de delete cluster
        dataproc https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html
    5 - Conecte todos operadores de maneira sequencial
    """
    start_dag = DummyOperator(task_id="start_dag")
    fim_dag = DummyOperator(task_id="fim_dag")
    create_cluster = DummyOperator(task_id="create_cluster")
    delete_cluster = DummyOperator(task_id="delete_cluster")
    submit_job = DummyOperator(task_id="submit_job")
    start_dag >> create_cluster >> submit_job >> delete_cluster >> fim_dag
