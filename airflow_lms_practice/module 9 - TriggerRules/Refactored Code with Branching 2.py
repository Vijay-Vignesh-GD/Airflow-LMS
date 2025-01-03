from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator  
from airflow.utils.trigger_rule import TriggerRule

config = {
    'dag_id_1': {'schedule_interval': '@daily', "start_date": datetime(2024, 1, 1), "table_name": "table_name_1"},
    'dag_id_2': {'schedule_interval': '@hourly', "start_date": datetime(2024, 2, 1), "table_name": "table_name_2"},
    'dag_id_3': {'schedule_interval': None, "start_date": datetime(2024, 3, 1), "table_name": "table_name_3"}
}
def log_start_processing(dag_id, table_name):
    """Log the start of table processing."""
    print(f"{dag_id} start processing tables in database: {table_name}")
def check_table_exist(table_name):
    """Check if the table exists. If not, create it."""
    print(f"Checking if {table_name} exists...")

    if True:  
        return 'insert_new_row'
    return 'create_table'
for dag_id, params in config.items():
    with DAG(
        dag_id=dag_id,
        schedule_interval=params['schedule_interval'],
        start_date=params['start_date'],
        catchup=False
    ) as dag:

        print_process_start = PythonOperator(
            task_id='print_process_start',
            python_callable=log_start_processing,
            op_args=[dag_id, params['table_name']]
        )
      
        get_current_user = BashOperator(
            task_id='get_current_user',
            bash_command='whoami', 
        )
       
        check_table = BranchPythonOperator(
            task_id='check_table_exist',
            python_callable=check_table_exist,
            op_args=[params['table_name']]
        )
        
        insert_new_row = EmptyOperator(
            task_id='insert_new_row',
            trigger_rule=TriggerRule.NONE_FAILED  
        )
      
        create_table = EmptyOperator(
            task_id='create_table',
            trigger_rule=TriggerRule.NONE_FAILED  
        )

        query_the_table = EmptyOperator(
            task_id='query_the_table',
            trigger_rule=TriggerRule.NONE_FAILED  
        )
    
        print_process_start >> get_current_user >> check_table
        check_table >> insert_new_row  
        check_table >> create_table   
        insert_new_row >> query_the_table 
        create_table >> insert_new_row   
        insert_new_row >> query_the_table  
   
    globals()[dag_id] = dag  









