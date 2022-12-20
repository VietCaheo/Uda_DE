

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
    + creat_tables.sql: complete CREAT TABLE sql command  that can be you directly in Redshift console for creating tables
    + SQL transformations
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

### Note about project:
    -> Creat IAM users and Redshift Cluster: should be keep same btw IAM username and Redshift cluster (Database user name): just for easy tracking when set up connection later.
    -> Each time re-creat Redshift Cluster: should choose Database user name and Password different clearly with old one (otherwise, error with subnet mask coming )
    -> When "reset data" in workspace and Run new session with AirFlow; or long time come back to wspace : 
        + It need to Creat Connection Again with AirFlow for both : AWS IAM conection and RedShift Cluster Connection.

    -> Note about small data song log with (Ex: A/A/A : it takes only few seconds to staging, if full song_data/: takes ~30mins for staging job)
    -> Each time delete Redshift Cluster and re-create with new one, it need to creat all tables  again with completed SQL commands in creat_tables.sql
        There are two way to re creat tables:
            + [way 1]: Copy the creat_tables file into same folder with _dag.py, and add the creat table dag task in _dag schenarino (not suggestion by mentor)
            + [say 2]: After create and config successfully for Redshift Cluster: using Query_Editor in Redshift Cluster console, and run the creat tables SQL command (See snap shot attached)
    -> Sometime: there are some unexpected data or config come to AirFlow working enviroment or Redshift Cluster:
        + Need to reset data in workspace 
        + Re create connection between AirFlow and AWS IAM ; Redshift Cluster.
        + Do run again
        + some like above referrence points above.




