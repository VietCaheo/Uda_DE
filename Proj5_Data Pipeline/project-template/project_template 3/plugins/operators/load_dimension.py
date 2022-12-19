from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# Dim table should be able to switching btw `truncate` or `append` : using `insert_mode`
# trucate: clear data existing dim table prior to insert new data
# append: just append new data to dim table
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table = "",
                 sql_insert_table = "",
                 insert_mode = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_mode = insert_mode
        self.sql_insert_table = sql_insert_table

    def execute(self, context):
        self.log.info("LoadDimensionOperator for dim_table {} is in progress >>>>>>>>>>>>> ...".format(self.table))
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # do switching mode truncate or append
        if (self.insert_mode == "append"):
            redshift_hook.run(" INSERT INTO {} {} ".format(self.table, self.sql_insert_table) )

        # truncate: means clear data inside table prior to insert new data, still keep table structure
        if (self.insert_mode == "truncate"):
            redshift_hook.run(" TRUNCATE TABLE  {}".format(self.table))
            redshift_hook.run(" INSERT INTO {} {} ".format(self.table, self.sql_insert_table))

        self.log.info("<<<<<<<<<<<<<<<<< .......................Finished to loading dim_table {}".format(self.table))
        self.log.info("Move to Run Quality Check ... >>>")

