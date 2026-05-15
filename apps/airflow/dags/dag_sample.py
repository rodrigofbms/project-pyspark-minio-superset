from datetime import datetime
from airflow import DAG # Directed Acyclic Graph (DAG) In other words, Directed → there is a direction between tasks
# Acyclic → cannot form an infinite loop and Graph → dependency structure
from airflow.providers.docker.operators.docker import DockerOperator # Import the operator responsible for running docker containers

# Define the default_args from Dag
default_args = {
    'owner': 'Rodrigo Maturino',
    'depends_on_past': False, # 'depends_on_past', that means it doesnt depende on the previous execution
}

# function run_container definition
def run_container(dag, image, container_name, command):
    # This return an airflow task that uses the DockerOperator to run a job in a docker container
    return DockerOperator(
        task_id=container_name, # Identifiy from task on DAG
        image=image, # Image from docker application
        container_name=container_name,
        api_version='auto', # Identify automatic the API version from docker
        auto_remove=True, # Remove automatic the container when the application finish
        command=command, # Define that the command run inside the container
        docker_url="tcp://docker-proxy:2375", # Airflow connect with daemon from docker
        network_mode="bigdata", # Define the docker network to be used
        mount_tmp_dir=False,  # Disable automatic mounting from temporary directory
        dag=dag  # passing the DAG reference to the operator
    )

# Definition from DAG
with DAG(
    'sample_airflow', # DAG's name
    default_args=default_args,
    start_date=datetime(2023, 1, 1),  # Define a start date
    schedule_interval='@weekly', # sets the time interval for execution
    catchup=False,  # Add this parameter to prevent past tasks from running
    tags=['example'] # Tag used to organize the DAGs
) as dag:

    # Creating a task that name is "sample_employee_task" and call the function to setting all configuration to create the dag
    sample_employee_task = run_container(
        dag=dag, # Reference the "dag" was created before
        image='wlcamargo/sparkanos-adventure-works',
        container_name='sample_airflow',
        command="spark-submit /app/111_sample_airflow.py"
    )

    sample_employee_task
