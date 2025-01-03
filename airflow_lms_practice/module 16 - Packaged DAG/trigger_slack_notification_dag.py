from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from slack import WebClient
from slack.errors import SlackApiError
from airflow.hooks.base_hook import BaseHook 
def send_slack_notification(**kwargs):
  
    slack_token = BaseHook.get_connection("slack_connection").extra_dejson.get("token")
    client = WebClient(token=slack_token)

    message = f"DAG ID: {kwargs['dag'].dag_id}, Execution Date: {kwargs['execution_date']}"

    try:
        response = client.chat_postMessage(
            channel="#airflow-channel-saras",
            text=message
        )
    except SlackApiError as e:
        assert e.response["error"]

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='trigger_dag_with_slack_notification',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:


    wait_for_run_file = FileSensor(
        task_id='wait_for_run_file',
        filepath='/opt/airflow/data/trigger_run.txt',  
        fs_conn_id='fs_default',
        poke_interval=10,
        timeout=600,
    )

    
    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='table_update_dag_2', 
        wait_for_completion=True,
    )

    with TaskGroup(group_id='process_results_task') as process_results_task:

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

        remove_run_file = BashOperator(
            task_id='remove_run_file',
            bash_command='rm -f /opt/airflow/data/trigger_run.txt'
        )

        create_finished_file = BashOperator(
            task_id='create_finished_file',
            bash_command='touch /opt/airflow/data/finished_{{ ts_nodash }}.txt'
        )

        print_result_task >> remove_run_file >> create_finished_file

    alert_to_slack = PythonOperator(
        task_id="send_slack_notification",
        python_callable=send_slack_notification,
    )

    wait_for_run_file >> trigger_dag_task >> process_results_task >> alert_to_slack
