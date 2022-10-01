""" Focus on Primary Key """

import cassandra

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

# Creat Key Space to work in
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

# Connect to Key Space
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# To give every album in the music_library that was created by a given artist
# select * from music_library WHERE artist_name="The Beatles
# this time using only year for PRIMARY KEY
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, city text, artist_name text, album_name text, PRIMARY KEY (year))"
try:
    session.execute(query)
except Exception as e:
    print(e)

#Insert rows to table
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

# to validate the table just created
query = "select * from music_library WHERE artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

# Creat new table using Coposite key (clustering by year and another to make PRIMARY KEY)
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(artist_name text, year int, album_name text, city text, PRIMARY KEY (artist_name,album_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# insert to table
query = "INSERT INTO music_library (artist_name, year, album_name, city)"
query = query + " VALUES (%s, %s, %s, %s)"

try:
    session.execute(query, ( "The Beatles", 1970, "Let it Be", "Liverpool"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, ("The Beatles", 1965, "Rubber Soul", "Oxford"))
except Exception as e:
    print(e)
    
try:
    session.execute(query, ("The Who", 1965, "My Generation", "London"))
except Exception as e:
    print(e)

try:
    session.execute(query, ("The Monkees", 1966, "The Monkees", "Los Angeles"))
except Exception as e:
    print(e)

try:
    session.execute(query, ("The Carpenters", 1970, "Close To You", "San Diego"))
except Exception as e:
    print(e)

# Validate again
query = "select * from music_library WHERE artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)


