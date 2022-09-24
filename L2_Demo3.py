""" Creating Fact and Dimension Tables with Star Schema
1. Creat both Fact and Dimension tables
2. Show how this is a basic element of the Star Schema
"""

import psycopg2

# Create a connection to the database
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)






