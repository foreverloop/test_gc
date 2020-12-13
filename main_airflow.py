import os
from airflow import DAG
import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


dt_yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

def get_composer_bucket():
    return os.environ['GCS_BUCKET']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': dt_yesterday
}

dag = DAG(dag_id="bq_example", 
	schedule_interval="@daily", 
	default_args=default_args, 
	catchup=False) 

with dag:
    
    is_file_present = GoogleCloudStorageObjectSensor(
        task_id='is_file_present',
        bucket='gs://input_bucket',
        object='airflow/test.csv',
        google_cloud_conn_id='google_cloud_default',
        timeout=60, # timeout after 60 seconds 
        poke_interval=30, #check every 30 seconds
        #soft_fail=True #skip to next task if file not found
    )

    #can try comment this out and just do the first one to test gcp default
    launch_dataflow_job = DataFlowPythonOperator(
    	task_id='launch_dataflow_job',
    	py_file='/home/airflow/gcs/my_dataflow.py',
    	task_id = 'my_task',
    	gcp_conn_id='google_cloud_default',
    	options={"input":"gs://input_file_path", #get buckets
    			 "output":"gs://output_location"},
    	dataflow_default_options={
    	"project": 'my-project',
    	"runner":'DataflowRunner',
        "staging_location": 'gs://path/Staging/',#get buckets
        "temp_location": 'gs://path/Temp/'},
        dag=dag
    )

    confirm_end_bash = BashOperator(
    	task_id="confirm_end_bash",
    	bash_command="echo hello world"
    )

is_file_present >> launch_dataflow_job >> confirm_end_bash