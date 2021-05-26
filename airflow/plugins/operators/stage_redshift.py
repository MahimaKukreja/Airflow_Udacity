from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to copy data (JSON format) from Source file to staging tables in Redshift 
    
    :param redshift_conn_id: Redshift connection ID for database
    :param table: Target staging table in Redshift to copy data into
    :param s3_bucket: S3 bucket location
    :param s3_path: Path in S3 bucket 
    :param aws_credentials_id: AWS credentials ID
    :param region: AWS Region for S3 bucket
    """
    
    ui_color = '#358140'
     
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_path="",
                 copy_json_option="auto",
                 region="",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.copy_json_option = copy_json_option
        self.region = region
    
    def execute(self, context):
        self.log.info("StageToRedshiftOperator execution starts")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        ### Connecting to redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connected with Redshift table")
        
        sql_statement = f"""
            COPY {self.table} 
                FROM 's3://{self.s3_bucket}/{self.s3_path}' 
                ACCESS_KEY_ID '{self.aws_key}'
                SECRET_ACCESS_KEY '{self.aws_secret}'
                REGION '{self.region}'
                JSON '{self.copy_json_option}'
                TIMEFORMAT as 'epochmillisecs'
        """
        self.log.info(f"copy sql: {sql_statement}")

        redshift_hook.run(sql_statement)
        self.log.info(f"StageToRedshiftOperator copy is completed - {self.table}")
        
        