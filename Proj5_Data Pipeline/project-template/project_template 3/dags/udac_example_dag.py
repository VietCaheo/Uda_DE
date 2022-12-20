from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


# The DAG does not have dependencies on past runs
# On failure, the task are retried 3 times
# Retries happen every 5 minutes
# Catchup is turned off
# Do not email on retry

default_args = {
    'depends_on_past': False,
    'owner': 'udacity',
    'start_date': datetime.now(),
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# creat tables if not exist
creat_tables_task = PostgresOperator(
    task_id='creat_tables',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id='redshift'
)

# Note: it's existing log json path of log data
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    region="us-west-2",
    s3_key="log_data",
    s3_json_path = "s3://udacity-dend/log_json_path.json"
)

#  try run with small data set in song log
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    region="us-west-2",
    s3_key="song_data/A/A/A",
    s3_json_path = "auto"
)

#  the fact table implicity allow append only
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = 'songplays',
    sql_insert_table = SqlQueries.songplay_table_insert
)

# dim tables can be choose `truncate` or `append` when inserting data
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    sql_insert_table = SqlQueries.user_table_insert,
    insert_mode = 'truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'songs',
    sql_insert_table = SqlQueries.song_table_insert,
    insert_mode = 'truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = 'artists',
    sql_insert_table = SqlQueries.artist_table_insert,
    insert_mode = 'truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = 'time',
    sql_insert_table = SqlQueries.time_table_insert,
    insert_mode = 'truncate'
)

#  need provide table lists to be Check quality
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables = ["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]
    # tables = ["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Make task dependencies
start_operator >> creat_tables_task

creat_tables_task >> stage_events_to_redshift
creat_tables_task >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table


load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table


load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
