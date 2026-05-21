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
    
    """ingestion_landing = create_task(
        dag=dag,
        image="rodrigofbms/project-pyspark-minio-superset",
        container_name="ingestion_landing",
        command="/opt/spark/bin/spark-submit \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.postgresql:postgresql:42.7.2,io.delta:delta-spark_2.12:3.1.0 \
                /app/05_incremental_extract_postgresql_to_landing_minio_parquet.py"
    )


    ingestion_bronze = create_task(
        dag=dag,
        image="rodrigofbms/project-pyspark-minio-superset",
        container_name="ingestion_bronze",
        command="/opt/spark/bin/spark-submit \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.postgresql:postgresql:42.7.2,io.delta:delta-spark_2.12:3.1.0 \
                /app/06_incremental_load_landing_to_bronze_delta.py"
    )

    transform_silver = create_task(
        dag=dag,
        image="rodrigofbms/project-pyspark-minio-superset",
        container_name="transform_silver",
        command="/opt/spark/bin/spark-submit \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.postgresql:postgresql:42.7.2,io.delta:delta-spark_2.12:3.1.0 \
                /app/07_incremental_transform_bronze_to_silver.py"
    )"""

    aggregation_gold = create_task(
        dag=dag,
        image="rodrigofbms/spark-py:3.4.0-delta-spark2.12-3.1.0",
        container_name="aggregation_gold",
        command="/opt/spark/bin/spark-submit \
                /app/08_incremental_agregation_silver_to_gold.py"
    )

    aggregation_gold