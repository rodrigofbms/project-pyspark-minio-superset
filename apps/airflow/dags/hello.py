from airflow.sdk import dag, task
from datetime import datetime

@dag(
    'hello_world',
    schedule='@once',  # Executar uma vez
    start_date=datetime(2026, 5, 15),
    catchup=False,
    tags=["exemplos"],
    description='Sample DAG Hello World'
)

def airflow_messages():
    @task(task_id='print_hello')
    def say_hello():
        print("Hello, World!")

    @task(task_id='print_goodbye')
    def say_goodbye():
        print("Goodbye Airflow!")


    say_hello() >> say_goodbye()

airflow_messages()