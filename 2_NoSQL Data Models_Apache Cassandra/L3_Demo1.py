""" 2 Queries 2 Tables """

# Import Apacje Cassandra python package
import cassandra

# Creat a connection to the database
from cassandra.cluster import Cluster
try:
    cluster =  Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print(e)

# Let's creat a keyspace to do our work in:
try:
    session.execute("""
    CREAT KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION =
    {'class' : 'SimpleStrategy', 'replication_factor': 1} """)
except Exception as e:
    print(e)

# Connect to our Keyspace. Compare this to how we had to create a new session in PostgreSQL
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# Imagine we need two queries "Give me every album in my music library that was released in a given year" and  \
# "Give me every album in my music library that was created by a given artis "
# -> need to creat two tables that correspond to the above queries
# Because I want to do two different quries, I am going to do need different tables that partition the data differently

# Table Name: music_library
# column 1: Year
# column 2: Artist Name
# column 3: Album Name
# PRIMARY KEY(year, artist name)

# Table Name: album_library
# column 1: Artist Name
# column 2: Year
# column 3: Album Name
# PRIMARY KEY (artist name, year)

query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "CREATE TABLE IF NOT EXISTS album_library "
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (artist_name,  year))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# Insert some data into both tables:
# [Note]: Seem do duplicate into 2 tables: because due to the no JOINS in Apache Cassandra
query = "INSERT INTO music_library (year, artist_name, album_name)"
query = query + " VALUES (%s, %s, %s)"

query1 = "INSERT INTO album_library (artist_name, year, album_name)"
query1 = query1 + " VALUES (%s, %s, %s)"

try:
    session.execute(query, (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Beatles", "Rubber Soul"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1965, "The Who", "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1966, "The Monkees", "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query, (1970, "The Carpenters", "Close To You"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Beatles", 1970, "Let it Be"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Beatles", 1965, "Rubber Soul"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Who", 1965, "My Generation"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Monkees", 1966, "The Monkees"))
except Exception as e:
    print(e)

try:
    session.execute(query1, ("The Carpenters", 1970, "Close To You"))
except Exception as e:
    print(e)


# Validate the table created by a query:
query = "select * from music_library WHERE YEAR=1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print (row.year, row.artist_name, row.album_name,)


# Validate by the second query
query = "select * from album_library WHERE ARTIST_NAME='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print (row.artist_name, row.year, row.album_name)

#Drop the tables
query = "drop table music_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table album_library"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

# Finally close the connection and cluster connection
session.shutdown()
cluster.shutdown()



