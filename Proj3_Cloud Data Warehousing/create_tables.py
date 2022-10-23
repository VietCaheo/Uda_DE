import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Implement connect to DB on Redshift
    # Get DB info in [CLUSTER] in file dwh.cfg
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    # afger get conn, Is it needed this as local db:>> 
    # conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    # cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    # cur.execute("CREAT DATABASE sparkifydb TH ENCODING 'utf8' TEMPLATE template0")

    # Interract with tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()