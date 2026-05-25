from airflow.sdk import DAG
from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "Rodrigo Maturino",
    "depends_on_past": False,
}

def create_task (dag, image, container_name, command):
    return DockerOperator(
        task_id=container_name,
        image=image,
        container_name=container_name,
        api_version="auto",
        auto_remove="success", # Never, Success or Force
        command=command,
        docker_url="tcp://docker-proxy:2375",
        network_mode="bigdata",
        mount_tmp_dir=False,
        dag=dag
    )

with DAG(
    dag_id="ELT_minIO",
    default_args=default_args,
    start_date=datetime(2026,5,19),
    schedule="@weekly",
    catchup=False,
    tags=["ELT"],
) as dag:
    
    ingestion_landing = create_task(
        dag=dag,
        image="rodrigofbms/spark:3.5.0-delta-3.1.0",
        container_name="ingestion_landing",
        command="spark-submit \
                /app/05_incremental_extract_postgresql_to_landing_minio_parquet.py"
    )


    ingestion_bronze = create_task(
        dag=dag,
        image="rodrigofbms/spark:3.5.0-delta-3.1.0",
        container_name="ingestion_bronze",
        command="spark-submit \
                /app/06_incremental_load_landing_to_bronze_delta.py"
    )

    transform_silver = create_task(
        dag=dag,
        image="rodrigofbms/spark:3.5.0-delta-3.1.0",
        container_name="transform_silver",
        command="spark-submit \
                /app/07_incremental_transform_bronze_to_silver.py"
    )

    aggregation_gold = create_task(
        dag=dag,
        image="rodrigofbms/spark:3.5.0-delta-3.1.0",
        container_name="aggregation_gold",
        command="spark-submit \
                /app/08_incremental_agregation_silver_to_gold.py"
    )

    ingestion_landing >> ingestion_bronze >> transform_silver >> aggregation_gold