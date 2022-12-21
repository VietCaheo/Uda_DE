# /opt/airflow/start.sh
#  examples of data validation:
""" Ensuring the number of rows in Redshift match the number of records in S3
Ensuring that the number of rows in a table are greater than zero
 """

# Ex1: Airflow DAG
# after finish py code, run the Bash command : /opt/airflow/start.sh

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello Ben!")


dag = DAG(
        'lesson1.solution1',
        start_date=datetime.datetime.now())

greet_task = PythonOperator(
    task_id="hello_world_task",
    python_callable=hello_world,
    dag=dag
)

""" DAGs: Directed acyclic graphs
Apache AirFlow  https://airflow.apache.org/

recommend to use AirFlow WebSever: Use Google Chrome, some time get issue when use another browser
make sure to toggle DAG to ON 
"""

# How AirFlow works:
""" 
Apache Airflow consists of:
-> Scheduler:          https://airflow.apache.org/scheduler.html
-> Work Queue: 
-> Worker Processses
-> Database:           https://www.sqlalchemy.org/
-> Web Interface:      http://flask.pocoo.org/

How AirFlow works:
-> Once all tasks have been completed, the DAG is completed:
    creat_table -> load_from_s3_to_redshift -> calculate_location_traffic """

# Building a Data Pipeline (a DAG)
""" 
-> Schedules are optional, and may be defined with cron strings or AirFlow Presets.

-> Creat a new DAG: just give
    + a name
    + a description
    + a start date
    + an interval
-> Creating Operators to Perform Tasks
    + Operators: define the atomic steps of work that make up a DAG. Instantiated operators are referred to as Tasks.
    + Schedules: @once, @hour, @daily, ...

 """
#  Ex2 _ add interval daily
import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")

#
# TODO: Add a daily `schedule_interval` argument to the following DAG
#
dag = DAG(
        "lesson1.exercise2",
        start_date=datetime.datetime.now() - datetime.timedelta(days=2)),
        schedule_interval='@daily'

task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
        dag=dag)

# Operators and Tasks:
    """ 
    -> Operator define the atomic steps of work that make up a DAG. AirFlow comes with many Operators that can perform coomon operator: 
        `PythonOperator`
        `PostgresOperator`
        `RedshiftToS3Operator`
        `S3ToRedshiftOperator`
        `BashOperator`
        `SimpleHttpOperator`
        `Sensor`
    -> Task Dependencies:
        Nodes = Tasks
        Edges = Ordering and dependencies between tasks
    ->  a >> b or  a.set_downstream(b)
        a << b or  a.set_upstream(b)
     """

# Ex3 _ Task Dependencies:
import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")


def addition():
    logging.info(f"2 + 2 = {2+2}")


def subtraction():
    logging.info(f"6 -2 = {6-2}")


def division():
    logging.info(f"10 / 2 = {int(10/2)}")


dag = DAG(
    "lesson1.exercise3",
    schedule_interval='@hourly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1))

hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    dag=dag)

addition_task = PythonOperator(
    task_id="addition",
    python_callable=addition,
    dag=dag)

subtraction_task = PythonOperator(
    task_id="subtraction",
    python_callable=subtraction,
    dag=dag)

division_task = PythonOperator(
    task_id="division",
    python_callable=division,
    dag=dag)

#
# TODO: Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

hello_world_task >> addition_task
hello_world_task >> subtraction_task
addition_task >> division_task
subtraction_task >> division_task

# Hooks and Connections:
""" 
-> Airflow allows users to manage connections and configuration of DAGs in the UI
-> Connections: access in code via hooks
-> Airflow come with many Hooks that can be integrate with common systems
    + HttpHook
    + PostgresHook  (works with RedShift)
    + MySqlHook
    + SlackHook
    + PrestoHook
 """

#  Handle with AWS
# Step1: reating IAM User in AWS
""" 
-> Goto IAM
-> ... (to see in lecture21)
-> Attach existing policies: 
    + Admisnistrator Access
    + AmazonRedshiftFullAccess
    + AmazonS3FullAccess
 """
# Step2: Add Airflow connections

# Ex4 _ Connecting to AWS
# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

#
# TODO: There is no code to modify in this exercise. We're going to create a connection and a
# variable.
# 1. Open your browser to localhost:8080 and open Admin->Variables
# 2. Click "Create"
# 3. Set "Key" equal to "s3_bucket" and set "Val" equal to "udacity-dend"
# 4. Set "Key" equal to "s3_prefix" and set "Val" equal to "data-pipelines"
# 5. Click save
# 6. Open Admin->Connections
# 7. Click "Create"
# 8. Set "Conn Id" to "aws_credentials", "Conn Type" to "Amazon Web Services"
# 9. Set "Login" to your aws_access_key_id and "Password" to your aws_secret_key. Use the "Launch AWS Console / Open Cloud Gateway" button in the classroom to generate these credentials.
# 10. Click save
# 11. Run the DAG

def list_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    prefix = Variable.get('s3_prefix')
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")

dag = DAG(
        'lesson1.exercise4',
        start_date=datetime.datetime.now())

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
)

# Templating with Context Variables:










