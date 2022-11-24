""" Exercise 3 - Data Lake on S3 """
from pyspark.sql import SparkSession
import os
import configparser

# Make sure that your AWS credentials are loaded as env vars
config = configparser.ConfigParser()

#Normally this file should be in ~/.aws/credentials
config.read_file(open('aws/credentials.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

# Creat spark session with hadoop-aws package
spark = SparkSession.builder\
                     .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
                     .getOrCreate()

# Load Data from S3
df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv")

df.printSchema()
df.show(5)

#  Infer schema, fix header and separator
df = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv",sep=";", inferSchema=True, header=True)

df.printSchema()
df.show(5)

# Fix Data yourself
import  pyspark.sql.functions as F
dfPayment = df.withColumn("payment_date", F.to_timestamp("payment_date"))
dfPayment.printSchema()
dfPayment.show(5)

# Extract the month
dfPayment = dfPayment.withColumn("month", F.month("payment_date"))
dfPayment.show(5)

# Computer aggregate revenue per month
dfPayment.createOrReplaceTempView("payment")
spark.sql("""
    SELECT month, sum(amount) as revenue
    FROM payment
    GROUP by month
    order by revenue desc
""").show()

# Fix the schema
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
paymentSchema = R([
    Fld("payment_id",Int()),
    Fld("customer_id",Int()),
    Fld("staff_id",Int()),
    Fld("rental_id",Int()),
    Fld("amount",Dbl()),
    Fld("payment_date",Date()),
])

dfPaymentWithSchema = spark.read.csv("s3a://udacity-dend/pagila/payment/payment.csv",sep=";", schema=paymentSchema, header=True)

dfPaymentWithSchema.printSchema()
df.show(5)

dfPaymentWithSchema.createOrReplaceTempView("payment")
spark.sql("""
    SELECT month(payment_date) as m, sum(amount) as revenue
    FROM payment
    GROUP by m
    order by revenue desc
""").show()



