from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(
    "generate_customer_marketing_metrics",
    description="A DAG to extract data, load into db and generate customer marketing metrics",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="cd $AIRFLOW_HOME && python3 generate_data.py && python3 run_ddl.py",
    )

    run_pipeline = BashOperator(
        task_id="dbt_run",
        bash_command="cd $AIRFLOW_HOME && dbt run --profiles-dir /opt/airflow/tpch_analytics/ --project-dir /opt/airflow/tpch_analytics/",
    )

    extract_data >> run_pipeline
