#-----------------------------------------------------
# Get Start and Installation PostgreSQL on local machine
# file:///C:/Program%20Files/PostgreSQL/14/doc/postgresql/html/tutorial-start.html
###################################################################################
""" Data Modeling: """
# ---------------------------------------------------

""" Demo0:  PostgreSQL and Autocommit: """
#import postgreSQL adapter for the Python
import psycopg2

# Creat a connection to the database
#  1. Connect to the local instance of PostgreSQL(127.0.0.1)
#  2. Use the database/schema from the instance
#  3. The connection reaches out to the database (studentdb) and use the connect privilages to connect to the database (user and pw = student)

conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")

#  Use the connection to get a cursor that will be used to execute queries
cur = conn.cursor()

# Creat a database to work in
# -> error will occur but it was to be expected due to table has not been created
cur.execute("select * from test")

# Creat table
# Error may coming because: we have not committed the transaction and had transaction block until restarting the connection
cur.execute("CREAT TABLE test (col1 int, col2 int, col3 int);")
conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
cur = conn.cursor()

# using autocommit=True : One action = one transaction
conn.set_session(autocommit=True)
cur.execute("select * from test")
cur.execute("CREAT TABLE test(col1 int, col2 int, col3 int);")

cur.execute("select * from test")
cur.execute("select count(*) from test")
print(cur.fetchall())

# drop the table
cur.execute("drop table test")


""" Demo1: Creating a Table with PostgreSQL 
Ceating a table
Inserting rows of data
Running a simple SQL query to validate the information
"""

#Import the library
import psycopg2

# Creat a connection to the database (3 steps are same as Demo 0)
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

# Use the connection to get a cursor that can be used to execute queries
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

# Use autocommit=True, otherwise need to call conn.commit() after each command
# [!] ability to rollback and commit transactions are a feature of Relational Databases
conn.set_session(autocommit=True)

# Test the connection and Error Handling Code
try:
    cur.execute("select * from udacity.music_library")
except psycopg2.Error as e:
    print(e)

#Creat a database to work in
try:
    cur.execute("creat database udacity")
except: psycopg2.Error as e:
    print(e)

# Close our Connection to the default database, reconnect to the Udacity database, and get new cursor
try:
    conn.close()
except psycopg2.Error as e:
    print(e)

try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=udacity user=student password=student")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

conn.set_session(autocommit=True)

# TO Creat this table
# column 1: Album Name
# Table Name: music_library
# column 2: Artist Name
# column 3: Year
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS music_library (album_name varchar, artist_name varchar, year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

# To check table was created by select count (*)
try: 
    cur.execute("select count(*) from music_library")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
print(cur.fetchall())

#Insert two rows:
try: 
    cur.execute("INSERT INTO music_library (album_name, artist_name, year) \
                 VALUES (%s, %s, %s)", \
                 ("Let It Be", "The Beatles", 1970))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_library (album_name, artist_name, year) \
                 VALUES (%s, %s, %s)", \
                 ("Rubber Soul", "The Beatles", 1965))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

#Validate the data was inserted into the table
try:
    cur.execute("SELECT * FROM music_library;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

# Drop the table to avoid duplictate and clean up
try:
    cur.execute("DROP table music_library")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print(e)

# Close the cursor and Connectoion
cur.close()
conn.close()


