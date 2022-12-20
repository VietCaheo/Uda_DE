from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



# For"STAGING TABLES"
# region:  us-west-2
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """  COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    REGION '{}'
                    FORMAT AS JSON '{}'
                    TIMEFORMAT as 'epochmillisecs'              
                    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                    COMPUPDATE OFF; 
    """

    # default params of operator
    # `redshift_conn_id` use same as "Conn Id" in Airflow Connections to Redshift Cluster console
    # `aws_credentials_id` use same as "Conn Id" in Aifflow Connections to AWS console
    @apply_defaults
    def __init__(self,
                redshift_conn_id='redshift',
                aws_credentials_id='aws_credentials',
                region='',
                table='',
                s3_bucket='udacity-dend',
                s3_key="",
                s3_json_path = "",
                *args, **kwargs):

        # map params for class
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_json_path = s3_json_path


    def execute(self, context):
        self.log.info('StageToRedshiftOperator is implementing here ...')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # handle with json data only

        # clear table before copying ...
        self.log.info("Truncating data for destination Redshift table >>>>>>>>>>>>>>>>>>>")
        redshift_hook.run("TRUNCATE {}".format(self.table))

        self.log.info("Prepare copying data from S3 to Redshift >>>>>>>>>>>>>>>>>>>")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)


        formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table,
                                                                s3_path,
                                                                credentials.access_key,
                                                                credentials.secret_key,
                                                                self.region,
                                                                self.s3_json_path)
        self.log.info("<<<<<<<<<<<<<<<< SQL generated already, see SQL command here")
        self.log.info(formatted_sql)

        self.log.info("Start staging data from S3 path:{} to Redshift table {} >>>>>>>>>>>>>>>".format(s3_path, self.table))
        redshift_hook.run(formatted_sql)

        self.log.info("<<<<<<<<<<<<<<<<<<< Data has pushed to Staging Tables")
        self.log.info("Finished Staging Data from S3, move to Load Fact table songplays ... >>>")
