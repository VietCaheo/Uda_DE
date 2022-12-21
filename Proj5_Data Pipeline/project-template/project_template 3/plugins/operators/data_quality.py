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
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id=aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks


    # target to check:
    # contains NULL values by counting all the rows that have NULL in the column.
    def execute(self, context):
        self.log.info('DataQualityOperator is implementing ... ')
        redshift_hook=PostgresHook(self.redshift_conn_id)

        for dq_check in self.dq_checks:
            t_sql = dq_check['test_sql']
            records = redshift_hook.get_records(t_sql)[0][0]

            if (dq_check['expected_result'] == records):
                raise ValueError("Data quality check failed in table row count should > 0 .........")
            

        self.log.info("Finished DataQuality check for table list {}".format(self.tables))
