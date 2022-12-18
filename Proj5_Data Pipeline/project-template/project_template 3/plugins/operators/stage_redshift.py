from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        DELIMITER '{}'
    """

    # default params of operator
    # `redshift_conn_id` use same as "Conn Id" in Airflow Connections to Redshift Cluster console
    # `aws_credentials_id` use same as "Conn Id" in Aifflow Connections to AWS console
    @apply_defaults
    def __init__(self,
                redshift_conn_id="redshift",
                aws_credentials_id="aws_credentials",
                table="",
                s3_bucket="udacity-dend",
                s3_key="",
                filetype='',
                *args, **kwargs):

        # map params for class
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.filetype=filetype
        self.aws_credentials_id = aws_credentials_id


    def execute(self, context):
        self.log.info('StageToRedshiftOperator implementing here ...')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # adapt either json or csv
        if (self.filetype=='json'):
            file_format=" json 'auto';"
        else:
            file_format=" csv  IGNOREHEADER 1;"

        # clear table before copying ...
        self.log.info("Clearing data from destination Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)


        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            file_format
        )
        logging.info("SQL generated already ...")
        logging.info(formatted_sql)
        redshift_hook.run(formatted_sql)
        logging.info("Data has pushed to Staging Tables")

