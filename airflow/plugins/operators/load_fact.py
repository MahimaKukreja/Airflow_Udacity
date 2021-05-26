from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    This operator is used to populate fact tables.
    :param redshift_conn_id: Redshift connection ID
    :param table:  Redshift table to be loaded
    :param select_sql: SQL statement to get the data which will be loaded in the target table
    
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sqlselect="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sqlselect = sqlselect

    def execute(self, context):
        self.log.info('Execution of LoadFactOperator begins for loading the fact tables')
        
        # connecting to Amazon Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Connected with redshift')

        # Writing insert query
        sql_statement2 = f"INSERT INTO {self.table} {self.sqlselect}"
        self.log.info(f"insert sql: {sqlselect}")

        redshift_hook.run(sql_statement2)
        self.log.info(f"StageToRedshiftOperator insert operation is completed - {self.table}")