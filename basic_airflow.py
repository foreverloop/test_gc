from airflow import DAG
import datetime
from airflow.operators.bash_operator import BashOperator

dt_yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

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

dag = DAG(dag_id="basic_bash_airflow", 
	schedule_interval="@daily", 
	default_args=default_args, 
	catchup=False) 

with dag:

	#variable name does not matter, only task_id, they are not related
	#but just makes it a little more consistent to use this convention
	hello_bash = BashOperator(
        task_id="hello_bash",
        bash_command="echo hello world"
    )

hello_bash