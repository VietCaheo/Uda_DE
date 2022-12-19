from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# operator use for run checks on the data itself
#  for each test if there no match resutl -> should raise exception
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    # Init to test a single table in a table list
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="aws_credentials",
                 redshift_conn_id="redshift",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id=aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    # target to check:
    # contains NULL values by counting all the rows that have NULL in the column.
    def execute(self, context):
        self.log.info('DataQualityOperator is implementing ... ')
        redshift_hook=PostgresHook(self.redshift_conn_id)

        # loop all table to be tested
        for table in self.tables:
            self.log.info("Print the testing table {}".format(table))
            table_record = "SELECT COUNT(*) FROM {}".format(table)

            # fetching list of record from testing table
            # record to be fetched with:
            # 1st index is row, 2nd index: field in the row
            record_list = redshift_hook.get_records(table_record)

            # could not fetch any row, or first row is empty
            if len(record_list) or len(record_list[0])< 1:
                raise ValueError("Check failed. {} returned no table record".format(table))

            # the first field in 1st row is empty
            if (len(record_list[0][0]) < 1):
                raise ValueError("Check failed. table {} has nothing row".format(table))

        self.log.info("Finished DataQuality check for table list {}".format(self.tables))
