import datetime

from airflow.decorators import dag
from airflow.operators.postgres_operator import PostgresOperator
# Custom Operators
from custom_operators.stage_redshift import StageToRedshiftOperator
from custom_operators.load_fact_mart import LoadFactMartOperator
from custom_operators.load_dimension import LoadDimensionOperator

from sql.sql_queries import SqlQueries

default_args = {
    'owner': 'adrian.pasek',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2022, 12, 1, 0, 0, 0, 0),
    default_args=default_args,
    schedule_interval='@monthly',
    max_active_runs=1,
    )
def yellow_taxi_dag():
    
    
    create_staging_tables = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_staging_tables
    )
    
    create_dim_tables = PostgresOperator(
        task_id="create_dim_tables",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_dim_tables
    )
    
    create_fact_tables = PostgresOperator(
        task_id="create_fact_tables",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_fact_table
    )
    
    create_mart_tables = PostgresOperator(
        task_id="create_mart_tables",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_mart_table
    )
    
    stage_trips = StageToRedshiftOperator(
        task_id="stage_trips",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table_name="staging_trips",
        s3_bucket="capstone-project-taxi-data",
        s3_key="yellow_trip/{{ data_interval_start.year }}/{{ data_interval_start.month }}/",
        format="parquet",
        overwrite=False,
    )
    
    stage_locations = StageToRedshiftOperator(
        task_id="stage_locations",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table_name="staging_locations",
        s3_bucket="capstone-project-taxi-data",
        s3_key="yellow_trip/taxi_zone_lookup.csv",
        format="csv",
        ignore_headers=1,
        delimiter=",",
        overwrite=True,
    )
    
    [create_staging_tables,
     create_dim_tables,
     create_fact_tables,
     create_mart_tables] >> stage_trips >> stage_locations
    
yellow_taxi_dag()
    