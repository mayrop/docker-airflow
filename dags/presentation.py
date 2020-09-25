"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import airflow.hooks.S3_hook
from datetime import datetime, timedelta
import time

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("presentation", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

def print_context(ds, **kwargs):
    print("PRINTING THE CONTEXT!!")
    print(kwargs['random_base'])
    return 'Whatever you return gets printed in the logs'

t4 = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    op_kwargs={'random_base': 'mytest-task'},
    dag=dag,
)

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t2)

# def upload_file_to_S3_with_hook(filename, key, bucket_name):
#     hook = airflow.hooks.S3_hook.S3Hook('s3_challenge')
#     hook.load_file(filename, key, bucket_name)

# s3_task = PythonOperator(
#     task_id='upload_to_S3',
#     python_callable=upload_file_to_S3_with_hook,
#     op_kwargs={
#         'filename': '/test.csv',
#         'key': 'files/test.csv',
#         'bucket_name': 'challenge-airflow',
#     },
#     dag=dag)

# s3_task.set_upstream(t1)
