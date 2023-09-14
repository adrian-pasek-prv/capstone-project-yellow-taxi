from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):
    
    def __init__(self,
                 redshift_conn_id: str = "",
                 has_rows_tables: list = [],
                 is_not_null_table_col_map: dict = {},
                 is_unique_table_col_map: dict = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.has_rows_tables = has_rows_tables
        self.is_not_null_table_col_map = is_not_null_table_col_map
        self.is_unique_table_col_map = is_unique_table_col_map

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.has_rows_tables:
            self.log.info(f'Performing "has_rows" data quality check on "{table}"')
            # Check if any rows were retrieved
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) == 0:
                err_msg = f'"has_rows" data quality check failed. {table} returned no results'
                self.log.error(err_msg)
                raise ValueError(err_msg)
            self.log.info(f'"has_rows" data quality check on "{table}" passed')
        
        for table_name, id_column_name in self.is_not_null_table_col_map.items():
            self.log.info(f'Performing "nullness" data quality check on "{table_name}"')
            # Check if id column is null
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table_name} WHERE {id_column_name} IS NULL")
            if len(records) > 0 and records[0][0] > 0:
                num_of_records = records[0][0]
                err_msg = f'"nullness" data quality check failed. Column "{id_column_name}" in table "{table_name}" contains {num_of_records:,} null values'
                self.log.error(err_msg)
                raise ValueError(err_msg)        
            self.log.info(f'"nulness" data quality check on "{table_name}" passed')
            
        for table_name, id_column_name in self.is_unique_table_col_map.items():
            self.log.info(f'Performing "uniqueness" data quality check on "{table_name}"')
            # Check if id column is unique
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table_name} GROUP BY {id_column_name} HAVING COUNT(*) > 1")
            if len(records) > 0 and records[0][0] > 0:
                num_of_records = records[0][0]
                err_msg = f'"uniqueness" data quality check failed. Column "{id_column_name}" in table "{table_name}" contains {num_of_records:,} duplicate values'
                self.log.error(err_msg)
                raise ValueError(err_msg)
            self.log.info(f'"uniqueness" data quality check on "{table_name}" passed')