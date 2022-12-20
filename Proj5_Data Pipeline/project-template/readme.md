/opt/airflow/start.sh

# Data Pipelines with Airflow
## Purpose of project:
    -> To apply a automation and monitoring to their data warehouse ETL pipelines : Apache Airflow/
    -> Creat high grade data pipelines that are dynamic and built from `reusable tasks`; `be monitored`; `easy backfills`
    -> after ETL steps executed: run tests against their datasets
    -> data (song & log): resides in S3 ->> need to be processed in Sparkify's data warehouse in Amazon Redshift

## Project Overview:
To introduce core concepts of Apache Airflow:
[Task] Create own custom operatorsto perform tasks such as : 
    + staging the data
    + filling the data warehouse.
    + running check the data in the final stl
[Help]:
    + four empty operators: to implement into functional pieces of a data pipeline.
    + set of tasks: need to be linked to achieve a coherent and sensible data flow within the pipeline.
    + contains all the SQL transformations
[Prerequisites]:
    + Create an IAM User 
    + Create Redshift Cluster in AWS
[Connections]:
    + Connect Airflow and AWS
    + Connect Airflow to AWS Redshift Cluster
    !Note creating cluster in `us-west-2` region, (same region of s3 bucket that locates)

## Steps TODO (Project Instruction):

Building the Operators to: Stage / Transform/ Run check 
!Note: Can reuse the code from Prj2 ...
+ All of the operators and task instances will run SQL statements against the Redshift database.

[1] Stage Operator:
    -> load any JSON formatted files from S3 to Amazon Redshift
    -> Operator creates and runs a SQL COPY statement base on parameters provided.
    -> Operator's parameters should specify where in S3 the file loaded and What target table.
    [!]  the parameters should use to distinguish between JSOn file
    [!] stage operator containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills
[2] Fact and Dimension Operators:
    -> use SQL helper class to run data transformations
    -> most of logic is within the SQL xformation, operator is expecte to take as input a SQL statement and target database on which to run the query
    -> can also define a target table that will contain the results of the xformation.
    -> Dimension loads are oftern done with the truncate-insert pattern where the target table is emptied berfore the load. -> thus could have a parameter that allows switching between insert modes when loading dimesions.
    -> Fact tables are usually so massive that they should only allow append type functionally.
[3] Data Quality Operator
    -> final operator to run checks on the data itself
    -> to run as some test cases ...


### referrence points:
https://knowledge.udacity.com/questions/256441

<!-- issue with template field -->
https://knowledge.udacity.com/questions/61537

<!-- issue with load_fact   -->
https://knowledge.udacity.com/questions/792553

<!-- creat tables issue -->
https://knowledge.udacity.com/questions/60209

<!-- issue with default_postgre conn Id -->
https://knowledge.udacity.com/questions/476414

<!-- issue with NULL value playid -->
https://knowledge.udacity.com/questions/325028




