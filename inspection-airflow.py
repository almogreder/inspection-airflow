
from datetime import datetime, timedelta
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.decorators import task, dag

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


import boto3
import awswrangler as wr 
import os
import logging
import json
from datetime import datetime




bucket_name = 'aiola-834657444538-data-sync-ap-southeast-1'
s3_bucket = f's3://{bucket_name}'
prefix='Gad-dynamoDB-download/tables/'
workspace_home = '/opt/airflow'
dbt_home=f"{workspace_home}/dbt"
profile=f"--profiles-dir {dbt_home}/.dbt"
dbt_sh_command = f'dbt run --project-dir {dbt_home} --profiles-dir {dbt_home}/.dbt'

last_update='GaD-KDS-S3-1-2022-01-01-01-01-01'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    'sla': timedelta(minutes=10),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
def two_decimals(n):
    #Turns an integer into a 2 decimal integer for the filepath format
    if(len(str(n))==1):
        return f'0{n}'
    return str(n)

def get_current_time_filepath(prefix):
    return prefix+'/raw'+str(datetime.now().year)+'/'+two_decimals(datetime.now().month)+'/'+two_decimals(datetime.now().day)+'/'+two_decimals(datetime.now().hour)+'/*'

def _validate_new_data():
    all_files=wr.list_objects(s3_bucket+'/'+get_current_time_filepath(prefix))
    assert(max(all_files)>last_update)
    last_update=max(all_files)
@dag( 
    default_args=default_args,
    description='Inspection_gad',
    schedule_interval='@monthly',
    start_date=datetime(2022, 1, 1),
    catchup=False
)
def inspection_tasks_flow(): 
        s3_stream_sensor = S3KeySensor(
            task_id='s3_stream_sensor',
            poke_interval=30,
            mode='reschedule',
            timeout=60*60,
            wildcard_match=False,
            soft_fail=True, 
            bucket_key=get_current_time_filepath(prefix),
            bucket_name=bucket_name,
            retries=2*60
        )
        validate_new_data= PythonOperator(task_id='validate_new_data', 
        python_callable=_validate_new_data
        failure_callback=BashOperator(task_id='no_new_data','airflow trigger_dag inspection_tasks_flow'))
    
        s3_stream_sensor>>validate_new_data>> dbt_run

        

dag=inspection_tasks_flow()