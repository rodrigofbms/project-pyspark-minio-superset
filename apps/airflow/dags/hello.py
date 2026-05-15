from airflow.decorators import dag, task
from datetime import datetime
import pendulum

@dag(
    'hello_world',
    schedule_interval='@once',  # Executar uma vez
    start_date=datetime(2026, 5, 15),
    catchup=False,
    tags=["example"],
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