from datetime import datetime,timedelta

from airflow import DAG 
from airflow.operators.bash import BashOperator


default_arg1 ={
    'owner' : 'saras',
    'retries' : 5 ,
    'retry_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='our_dag_first_v2',
    default_args=default_arg1,
    description='this is our first dag we write ',
    start_date=datetime(2024,12,18,11,55),
    schedule_interval='@daily'

) as dag:
    
    task1 = BashOperator(
    task_id = "first_task",
    bash_command="echo this is our first task made by me"

    
)
    task2=BashOperator(
        task_id = "second_task",
        bash_command="echo haiii this is saras prasad"
    )

    task1.set_downstream(task2)