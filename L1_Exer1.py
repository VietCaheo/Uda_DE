""" Lesson 1 Exercise 1: Creating a Table with PostgreSQL
 """
# Import the lib using for SQLDataBase type
import psycopg2
# followed command just for Jupyter
#!echo "alter user student createdb;" | sudo -u postgres psql

# Creat a connection to database
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

#TO-DO Set autocommit to be True: each action is commiitted without having to call conn.commit() after each command
conn.set_session(autocommit=True)

#TO-DO Creat a database to do the work in
try: 
    cur.execute("create database l1ex1")
except psycopg2.Error as e:
    print(e)

# TO-DO Add the database name in the connect statement. Let's close our connection to the default database, reconnect to L1Ex1 database, and get new cursor
try: 
    conn.close()
except psycopg2.Error as e:
    print(e)

try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=l1ex1 user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

conn.set_session(autocommit=True)

# TO-DO Creat a Song Library that contains a list of songs, including the song name, artist name, year, album if was from and if it was a single
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS song_library (song_title varchar,artist_name varchar, year int, album_name varchar, single boolean);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

# To check for ensure table was created
try:
    cur.execute("select count(*) from song_library")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

print(cur.fetchall())

# TO-DO Insert two rows in the table
try: 
    cur.execute("INSERT INTO Song_Library (song_title, artist_name, year, album_name, single) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 ("Across The Universe","The Beatles",1970,"Let It Be","FALSE"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO Song_Library (song_title, artist_name, year, album_name, single) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 ("Think For Yourself","The Beatles",1965,"Rubber Soul","FALSE"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# TO-DO Validate data was inserted into the table
try: 
    cur.execute("SELECT * FROM Song_Library;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

#Drop the table after each time run test
try:
    cur.execute("DROP table Song_Library")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print(e)

#Close cursor and connection
cur.close()
conn.close()
