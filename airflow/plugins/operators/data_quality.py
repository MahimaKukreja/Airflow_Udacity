from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", table=[], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        NumberOfrecords = redshift_hook.get_records(
            f"SELECT COUNT(*) FROM {self.table};"
        )
        if len(NumberOfrecords) < 1 or len(NumberOfrecords[0]) < 1:

            raise ValueError("Data QC failed. {} returned no results".format(table))

        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("No records present in")
        self.log.info(
            f"Data  QC on table {self.table} check passed - Found {records[0][0]} records"
        )
