from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class StageToRedshiftOperator(BaseOperator):
    # What field will be templateable, in what field can I use Airflow context variables
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        JSON '{}'
        REGION '{}'
    """

    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 table_name: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 aws_region: str = "",
                 format: str = "parquet",
                 ignore_headers: int = 1,
                 delimiter: str = ",",
                 overwrite: bool = True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_region = aws_region
        self.format = format
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter
        self.overwrite = overwrite
        

    def execute(self, context):
        
        if self.format == "parquet":
            format = 'PARQUET'
            opt_params = ''
        elif self.format == "csv":
            format = 'CSV'
            opt_params = f"DELIMITER AS '{self.delimiter}'"
        else:
            raise NotImplementedError("Format not implemented")
        
        # Extract AWS credentials from Airflow connections
        aws_hook = AwsGenericHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # Extract JINJA context variables like data_interval_start_date
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        copy_stmt = f"""
            COPY {self.table_name}
            FROM {s3_path}
            ACCESS_KEY_ID {credentials.access_key}
            SECRET_ACCESS_KEY {credentials.secret_key}
            IGNOREHEADER {self.ignore_headers}
            REGION {self.aws_region}
            FORMAT {format}
            {opt_params}
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.overwrite:
            self.log.info(f"Clearing data from {self.table_name} table")
            redshift.run(f"DELETE FROM {self.table_name}")
        
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(copy_stmt)





