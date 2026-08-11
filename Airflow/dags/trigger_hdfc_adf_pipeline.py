from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="trigger_hdfc_adf_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["adf", "airflow", "databricks", "hdfc"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_adf_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_pl_hdfc_master",
        azure_data_factory_conn_id="azure_data_factory_default",
        resource_group_name="rg-stock-forecast-dev",
        factory_name="adf-stock-forecast-dev",
        pipeline_name="pl_hdfc_master",
        wait_for_termination=True,
        check_interval=60,
        timeout=3600,
    )

    end = EmptyOperator(task_id="end")

    start >> run_adf_pipeline >> end
