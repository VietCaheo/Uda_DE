""" Clustering Column """

import cassandra

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

# Creat key space
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

# Connect our key space
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# Query needed: 
#  Give me every album in my music library that was released by an Artist with Albumn Name in DESC Order and City In DESC Order
# Table Name: music_library
# column 1: Year
# column 2: Artist Name
# column 3: Album Name
# Column 4: City
# PRIMARY KEY(artist name, album name, city)

query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, city text, PRIMARY KEY (artist_name, album_name, city))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# Insert Data into table
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
    session.execute(query, (1964, "The Beatles", "Beatles For Sale", "London"))
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

# Validate our data Model
query = "select * from music_library WHERE ARTIST_NAME='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name, row.album_name, row.city, row.year)

# Drop the table
query = "drop table music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

# Close Session and Cluster connection
session.shutdown()
cluster.shutdown()

