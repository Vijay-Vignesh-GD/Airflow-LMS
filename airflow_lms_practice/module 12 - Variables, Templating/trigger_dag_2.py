"""

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    dag_id='trigger_table_update_dag',
    default_args=default_args,
    description='A DAG that waits for a file, triggers an external DAG, and removes the file',
    schedule_interval=None,  # The DAG will be triggered manually
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    # Task 1: Wait for the file 'run' to appear in a specific directory
    wait_for_file = FileSensor(
        task_id='wait_for_run_file',
        filepath='/opt/airflow/data/trigger_run.txt',  # Path to the trigger file
        poke_interval=30,  # How often to check for the file (in seconds)
        timeout=600,  # Timeout for the sensor to stop checking after this time (in seconds)
        fs_conn_id='fs_default'  # Default connection to the filesystem
    )

    # Task 2: Trigger the table update DAG once the file is detected
    trigger_table_update_dag = TriggerDagRunOperator(
        task_id='trigger_external_dag',
        trigger_dag_id='table_update_dag',  # Replace with the actual DAG ID you want to trigger
        wait_for_completion=True  # This ensures the current DAG waits for the external DAG to finish
    )

    # Task 3: Remove the 'run' file after the external DAG finishes
    remove_run_file = BashOperator(
        task_id='remove_run_file',
        bash_command='rm -f /opt/airflow/data/trigger_run.txt'  # Remove the file after it is processed
    )

    # Define task dependencies
    wait_for_file >> trigger_table_update_dag >> remove_run_file 

"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='trigger_table_update_dag_2',
    default_args=default_args,
    description='A DAG that waits for a file, triggers an external DAG, and processes results',
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    file_path = Variable.get('trigger_file_path', default_var='/opt/airflow/data/trigger_run.txt')
    wait_for_file = FileSensor(
        task_id='wait_for_run_file',
        filepath=file_path,
        poke_interval=30,
        timeout=600,
        fs_conn_id='fs_default'
    )

    trigger_table_update_dag = TriggerDagRunOperator(
        task_id='trigger_external_dag',
        trigger_dag_id='table_update_dag_2',
        wait_for_completion=False
    )

    with TaskGroup(group_id='process_results') as process_results:

        # Validate DAG completion (wait for 'end_task' in the external DAG)
        
        # validate_dag = ExternalTaskSensor(
        #     task_id='validate_dag_completion',
        #     external_dag_id='table_update_dag_2',
        #     external_task_id='end_task',
        #     timeout=600,
        #     poke_interval=30,
        #     mode='poke'
        # )

        # Print result using PythonOperator
        def print_result(**context):
            result = context['ti'].xcom_pull(
                dag_id='table_update_dag_2', 
                task_ids='end_task',
                key='result'
            )
            print(f"Result from table_update_dag_2: {result}")
            print("Execution Context:")
            print(f"Data Interval Start: {context.get('data_interval_start')}")
            print(f"Logical Date: {context.get('logical_date')}")

        print_result_task = PythonOperator(
            task_id='print_result',
            python_callable=print_result
        )

        # Remove 'run' file
        remove_run_file = BashOperator(
            task_id='remove_run_file',
            bash_command='rm -f /opt/airflow/data/trigger_run.txt'
        )

        # Create 'finished_#timestamp' file
        create_finished_file = BashOperator(
            task_id='create_finished_file',
            bash_command='touch /opt/airflow/data/finished_{{ ts_nodash }}.txt'
        )

        # Task dependencies within TaskGroup

        # validate_dag >> 
        print_result_task >> remove_run_file >> create_finished_file

    # Task dependencies for the main DAG
    wait_for_file >> trigger_table_update_dag >> process_results
