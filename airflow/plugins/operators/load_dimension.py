from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    This operator is used to populate Dimension tables
    :param string  redshift_conn_id: Redshift connection ID
    :param string  table: Redshift table to be loaded
    :param string  sqlselect: SQL statement to get the songplay data
    :param string  truncatetable: 'YES' means truncates table before load; 'NO' means appends
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sqlselect,
                 truncatetable,
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sqlselect = sqlselect
        self.truncatetable = truncatetable

    def execute(self, context):
        self.log.info('Execution of LoadDimensionOperator begins for loading the dimension tables')
        # Cconnecting to Amazon Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Connected with redshift')

        if self.truncatetable == 'YES':
            self.log.info(
                f"...Truncating the table {self.table} before adding the records")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")

        # Creating insert query
        sql_statement3 = f"INSERT INTO {self.table} {self.sqlselect}"
        self.log.info(f"...insert sql -- {sql_statement3}")

        redshift_hook.run(sql_statement3)
        self.log.info(
            f"LoadDimensionOperator insert is completed -- {self.table}")