""" Using the WHERE Clause"""

import cassandra

# Creat connection to database
from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

# Creat a keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

#Connect to keyspace
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# Creat table
# Table Name: music_library
# column 1: Year
# column 2: Artist Name
# column 3: Album Name
# Column 4: City
# PRIMARY KEY(year, artist_name, album_name)

query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, city text, PRIMARY KEY (year, artist_name, album_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# Insert data to table
query = "INSERT INTO music_library (year, artist_name, album_name, city)"
query = query + " VALUES (%s, %s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be", "Liverpool"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul", "Oxford"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, (1965, "The Who", "My Generation", "London"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees", "Los Angeles"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You", "San Diego"))
except Exception as e:
    print(e)

# Query 1. Give me every album in my music library that was released in a given year (1970)
query = "select * from music_library WHERE YEAR=1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

# Query 2: Give me the album that is in my music library that was released in a given year by "The Beatles"
query = "select * from music_library WHERE YEAR=1970 AND ARTIST_NAME = 'The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

# Query 3: Give me all the albums released in a given year in a give location
# query = "select * from music_library WHERE YEAR = 1970 AND LOCATION = 'Liverpool'"
query = "select city from music_library WHERE YEAR = 1970 AND ARTIST_NAME = 'The Beatles' AND ALBUM_NAME='Let it Be'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

# Drop the table
query = "drop table music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

# Close the session and cluster connection
session.shutdown()
cluster.shutdown()


