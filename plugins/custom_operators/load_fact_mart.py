from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadFactMartOperator(BaseOperator):
    # What field will be templateable, in what field can I use Airflow context variables
    template_fields = ("sql",)
    
    def __init__(self,
                 redshift_conn_id: str = "",
                 table_name: str = "",
                 sql: str = "",
                 *args, **kwargs):

        super(LoadFactMartOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql = sql

    def execute(self, context):
        # Extract JINJA context variables like data_interval_start_date
        rendered_key = self.sql.format(**context)

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Loading fact table "{self.table_name}"')
        sql_stmt = f'''
            INSERT INTO {self.table_name}
            {rendered_key}
        '''
        redshift.run(sql_stmt)
        self.log.info(f'Finished loading fact table {self.table_name}')