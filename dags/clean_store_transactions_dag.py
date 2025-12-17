from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime


with DAG(
dag_id="clean_store_transactions",
start_date=datetime(2024, 1, 1),
schedule=None,
catchup=False,
tags=["dataops", "spark"],
) as dag:


run_spark_job = SSHOperator(
task_id="run_spark_cleaning",
ssh_conn_id="spark_client_ssh",
command="""
spark-submit \
/opt/project/spark_app/clean_store_transactions.py
""",
)


run_spark_job