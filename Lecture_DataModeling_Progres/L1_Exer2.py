# Creating a table with Apache Cassandra


import cassandra

#Creating a connection to the database
from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

# TO-DO: Create a keyspace to do the work in
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS l1ex2 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)
except Exception as e:
    print(e)

# TO-DO: Connect to the Keyspace
try:
    session.set_keyspace('l1ex2')
except Exception as e:
    print(e)

# Create a Song Library that contains a list of songs, including the song name, artist name, year, album it was from, and if it was a single

# TO-DO: You need to create a table to be able to run the following query
# select * from songs WHERE year=1970 AND artist_name="The Beatles"
query = "CREATE TABLE IF NOT EXISTS songs "
query = query + "(year int, song_title text, artist_name text, album_name text, single boolean, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# TO-DO: Insert the following two rows in your table
query = "INSERT INTO songs (year, song_title, artist_name, album_name, single)" 
query = query + " VALUES (%s, %s, %s, %s, %s)"

try:
    session.execute(query, (1970, "Let It Be","The Beatles","Across THe Universe",False))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "Think for Yourself","The Beatles","Rubber Soul",False))
except Exception as e:
    print(e)

#  TO-DO: Validate your data was inserted into the table.
query = 'SELECT * FROM songs'
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)

# TO-DO: Validate the Data Model with the original query.
#  select * from songs WHERE YEAR=1970 AND artist_name="The Beatles"
query = "select * from songs WHERE YEAR=1970 AND artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.album_name, row.artist_name)

# And Finally close the session and cluster connection
session.shutdown()
cluster.shutdown()



