#-------------------------------------------------
# https://docs.datastax.com/en/developer/python-driver/3.25/

# For Installation Apache Cassandra:
# http://cassandra.apache.org/doc/latest/getting_started/installing.html
#-------------------------------------------------
"""Lesson1 Demo2: Creating a Table with Apache Cassandra
Creating a table
Inserting rows of data
Running a simple SQL query to validate the information
 """
 
import cassandra

 #Creat a connection to database
from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

# Test the connection and Error Handling code
# try select * on table that not been createt yet:
try:
    session.execute("""select * from music_library""")
except Exception as e:
    print(e)

# Create a keyspace to the work in
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)
except Exception as e:
    print(e)

# Connect to our Keyspace.
# To Creat a Music Library of albums, but need to note that: Apache Cassandra is a NoSQL database
# [!] There are no duplicates in Apache Cassandra
# Table Name: music_library
# column 1: Album Name
# column 2: Artist Name
# column 3: Year
# PRIMARY KEY(year, artist name)
# TO DO: Creat Table Statement

query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# Insert two rows
query = "INSERT INTO music_library (year, artist_name, album_name)"
query = query + " VALUES (%s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
except Exception as e:
    print(e)

#Validate data was inserted into the table
query = 'SELECT * FROM music_library'
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)

# Validate the Data Model with the original query.
# select * from music_library WHERE YEAR=1970

query = "select * from music_library WHERE YEAR=1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)

# Drop the table to avoid duplicates and clean up
query = "drop table music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

# Close the session and cluster connection
session.shutdown()
cluster.shutdown()
#-----------------------------------------