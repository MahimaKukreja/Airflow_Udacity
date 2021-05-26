from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

### Creating DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2018, 11, 1), 
    'catchup': True,
    'end_date': datetime(2018,11,2)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
)

### Starting operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

### Creating tables if not exist
create_staging_events = CreateTableOperator(
    task_id='create_staging_events',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_events',
    sql_command=SqlQueries.create_staging_events
)

create_staging_songs = CreateTableOperator(
    task_id='create_staging_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_songs',
    sql_command=SqlQueries.create_staging_songs
)

create_songplays = CreateTableOperator(
    task_id='create_songplays',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songplays',
    sql_command=SqlQueries.create_songplays
)

create_artists = CreateTableOperator(
    task_id='create_artists',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    sql_command=SqlQueries.create_artists
)

create_songs = CreateTableOperator(
    task_id='create_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    sql_command=SqlQueries.create_songs
)

create_time = CreateTableOperator(
    task_id='create_time',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    sql_command=SqlQueries.create_time
)

create_users = CreateTableOperator(
    task_id='create_users',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    sql_command=SqlQueries.create_users
)

### Staging event data from S3 to Amazon Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_buckt='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}',
    s3_format='json',
    s3_format_mode='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    target_table='staging_events',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    render_s3_key=True,
    truncate=True
)

### Staging songs data from S3 to Amazon Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_buckt='udacity-dend',
    s3_key='song_data',
    s3_format='json',
    s3_format_mode='auto',
    region='us-west-2',
    target_table='staging_songs',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    render_s3_key=False,
    truncate=True
)

### Loading fact tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    target_table='songplays',
    target_columns='(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.songplay_table_insert,
    truncate=False
)

### Loading dimension tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    target_table='users',
    target_columns='(userid, first_name, last_name, gender, level)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    target_table='songs',
    target_columns='(songid, title, artistid, year, duration)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    target_table='artists',
    target_columns='(artistid, name, location, lattitude, longitude)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table='time',
    target_columns='(start_time, hour, day, week, month, year, weekday)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.time_table_insert,
    truncate=True
)
### Data Quality Check
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["artists", "songplays", "songs"]
)

### End operator
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

### Defining dependencies for DAG
start_operator >> [create_staging_events, create_staging_songs, create_songplays, \
                    create_artists, create_songs, create_time, create_users] >> sync_operator_1

sync_operator_1 >>[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, \
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator