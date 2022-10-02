# Project Overview:
Apply what you've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, you will need to model your data by creating tables in Apache Cassandra to run queries. You are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

We have provided you with a project template that takes care of all the imports and provides a structure for ETL pipeline you'd need to process this data.

# Datasets: event_data
<!-- event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv -->

# Project Steps:
1. Modeling your NoSQL database or Apache Cassandra database
    -> Design tables to answer the queries outlined in the project template
    -> Write Apache Cassandra CREAT KEYSPACE and SET KEYSPACE
    -> develop CREAT statement for each of the tables to address each question
    -> Load the data by INSERT statement
    -> Include IF NOT EXISTS ; recommend DROP TABLE for each table
    -> Test running with proper WHERE clause

2. Build ETL Pipeline
    -> Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
    -> Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT statements to load processed records into relevant tables in your data model
    -> Test by running SELECT statements after running the queries on your database











