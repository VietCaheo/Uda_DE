import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Load data json files

    Function to load data from json files from S3 bucket, 
    then staging into two `staging_table`
    input:
        cur: current cursor
        conn: database connection var
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Do INSERT command in Redshift Database

    Function execute the INSERT command for insert data from 
    staging tables into five analytic tables.
    input:
        cur: current cursor
        conn: database connection var.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Mainfuction
     
    Function do main sequence for implement ETL by a Computing Cloud services.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Copy json files frin S3 to become two Staging Tables
    load_staging_tables(cur, conn)
    print("Finish staging data from S3 into two Staging tables ... ")

    # From here already have two staging table, Do properly INSERT SQL command for load to analytics Tables

    # Do Insert Analytics from two Staging Tables
    insert_tables(cur, conn)
    print("Inserting Analytics tables successfully ... ")

    conn.close()


if __name__ == "__main__":
    main()