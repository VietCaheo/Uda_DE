from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


#  to load factTable songplays: 
# method execute of class: using SQL command in sql_queries`songplay_table_insert`
# columns name must be followed the name in CREATE-TABLE
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table = "",
                 columns = "(playid, \
                            start_time, \
                            userid, \
                            level, \
                            songid, \
                            artistid, \
                            sessionid, \
                            location, \
                            user_agent)",
                 sql_insert_table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.columns = columns
        self.sql_insert_table = sql_insert_table

    def execute(self, context):
        self.log.info('LoadFactOperator is implementing ...')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # define the INSERT SQL STATEMENT
        # for Fact table: it need to be define columns attribute
        sql_statement = "INSERT INTO {} {} {}".format(self.table, self.columns, self.sql_insert_table)

        redshift_hook.run(sql_statement)

        self.log.info("Finished to LoadFactOperator for the fact_table {}".format(self.table))
        self.log.info("Move to load Dim tables ... >>> ")
