from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class LoadDimensionOperator(BaseOperator):
    
    def __init__(self,
                 redshift_conn_id: str = "",
                 table_name: str = "",
                 sql: str = "",
                 append_only: bool = False,
                 insert_as_select: bool = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql = sql
        self.append_only = append_only
        self.insert_as_select = insert_as_select

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading dimension table: "{self.table_name}"')
        
        if self.append_only:
            self.log.info(f'Appending to table "{self.table_name}')
            if self.insert_as_select:
                sql_insert_stmt = f'''
                    INSERT INTO {self.table_name}
                    {self.sql}
                '''
            else:
                sql_insert_stmt = f'''
                    INSERT INTO {self.table_name} VALUES
                    {self.sql}
                '''
        else:
            self.log.info(f'Deleting table "{self.table_name}"')
            sql_del_stmt = f'DELETE FROM {self.table_name}'
            redshift.run(sql_del_stmt)
            self.log.info(f'Inserting into table "{self.table_name}"')
            if self.insert_as_select:
                sql_insert_stmt = f'''
                    INSERT INTO {self.table_name}
                    {self.sql}
                '''
            else:
                sql_insert_stmt = f'''
                    INSERT INTO {self.table_name} VALUES
                    {self.sql}
                '''
            
        redshift.run(sql_insert_stmt)


            
            
            