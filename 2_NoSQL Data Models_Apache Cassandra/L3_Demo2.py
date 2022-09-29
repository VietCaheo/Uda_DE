""" Focus on Primary Key """

import cassandra

# Let first creat a connection to the database
from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

# Let's create a keyspace to do our work in
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

# Connect to our Keyspace. Compare this to how we had to create a new session in PostgreSQ
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# Table Name: music_library
# column 1: Year
# column 2: Artist Name
# column 3: Album Name
# Column 4: City
# PRIMARY KEY(year)

# This case make table with PRIMARY KEY by `year`
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, city text, PRIMARY KEY (year))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# Insert data
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


# Let's Validate our Data Model -< this will not return enough 2 rows, because PRIMARY KEY  (year) is not unique
query = "select * from music_library WHERE YEAR=1965"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

# this time creat table with cluster (year, album_name) to make PRIMARY KEY
query = "CREATE TABLE IF NOT EXISTS music_library1 "
query = query + "(year int, artist_name text, album_name text, city text, PRIMARY KEY (year, album_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "INSERT INTO music_library1 (year, artist_name, album_name, city)"
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

# Validate again
query = "select * from music_library1 WHERE YEAR=1965"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

#drop the table
query = "drop table music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table music_library1"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

# Close session
session.shutdown()
cluster.shutdown()















