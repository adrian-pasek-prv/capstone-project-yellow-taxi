import datetime

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
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
    'retry_delay': datetime.timedelta(minutes=2),
    'catchup': False,
    'email_on_retry': False
}

@dag(start_date=datetime.datetime(2019, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2022, 12, 1, 0, 0, 0, 0),
    default_args=default_args,
    schedule_interval='@monthly',
    max_active_runs=1,
    )
def yellow_taxi_dag():
    
    with TaskGroup('create_tables') as create_tables:
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
    
    with TaskGroup('stage_to_redshift') as stage_to_redshift:
        stage_trips = StageToRedshiftOperator(
            task_id="stage_trips",
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table_name="staging_trips",
            s3_bucket="capstone-project-taxi-data",
            s3_key="yellow_trip/{{ data_interval_start.year }}/{{ data_interval_start.month }}/",
            format="parquet",
            overwrite=True,
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
        
        stage_payment_types = StageToRedshiftOperator(
            task_id="stage_payment_types",
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table_name="staging_payment_types",
            s3_bucket="capstone-project-taxi-data",
            s3_key="yellow_trip/taxi_payment_types.csv",
            format="csv",
            ignore_headers=1,
            delimiter=",",
            overwrite=True,
        )
        
        stage_rate_codes = StageToRedshiftOperator(
            task_id="stage_rate_codes",
            redshift_conn_id="redshift",
            aws_credentials_id="aws_credentials",
            table_name="staging_rate_codes",
            s3_bucket="capstone-project-taxi-data",
            s3_key="yellow_trip/taxi_rate_codes.csv",
            format="csv",
            ignore_headers=1,
            delimiter=",",
            overwrite=True,
        )
    
    with TaskGroup('load_dim_tables') as load_dim_tables:
        load_dim_locations = LoadDimensionOperator(
            task_id="load_dim_locations",
            redshift_conn_id="redshift",
            table_name="dim_locations",
            sql=SqlQueries.insert_dim_tables['dim_locations'],
            append_only=False
        )
        
        load_dim_payment_types= LoadDimensionOperator(
            task_id="load_dim_payment_types",
            redshift_conn_id="redshift",
            table_name="dim_payment_types",
            sql=SqlQueries.insert_dim_tables['dim_payment_types'],
            append_only=False
        )

        load_dim_rate_codes = LoadDimensionOperator(
            task_id="load_dim_rate_codes",
            redshift_conn_id="redshift",
            table_name="dim_rate_codes",
            sql=SqlQueries.insert_dim_tables['dim_rate_codes'],
            append_only=False
        )
        
        load_dim_time = LoadDimensionOperator(
            task_id="load_dim_time",
            redshift_conn_id="redshift",
            table_name="dim_time",
            sql=SqlQueries.insert_dim_tables['dim_time'],
            append_only=False
        )
    
    load_fact_trips = LoadFactMartOperator(
        task_id="load_fact_trips",
        redshift_conn_id="redshift",
        table_name="fact_trips",
        sql=SqlQueries.insert_fact_table.format(interval_start="{{ data_interval_start.to_datetime_string() }}", interval_end="{{ data_interval_end.to_datetime_string() }}")
    )
    
    load_mart_trips_hourly = LoadFactMartOperator(
        task_id="load_mart_trips_hourly",
        redshift_conn_id="redshift",
        table_name="mart_trips_hourly",
        sql=SqlQueries.insert_mart_table.format(interval_start="{{ data_interval_start.to_datetime_string() }}", interval_end="{{ data_interval_end.to_datetime_string() }}")
    )
    
    create_tables >> stage_to_redshift >> load_dim_tables >> load_fact_trips >> load_mart_trips_hourly
    
yellow_taxi_dag()
    