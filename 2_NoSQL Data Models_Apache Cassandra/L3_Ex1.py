""" Three queries three tables """

# Import Apache Cassandra
import cassandra

# Creat connection to the database
from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

# Creat a keyspace to work in
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION =
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )
except Exception as e:
    print(e)

# Connect to our Keyspace. Compare this to how we had to create a new session in PostgreSQL.
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

#Assume that  we need 3 questions of the data \
# 1. Give every album in the music library that was released in a given year
# select * from music_library WHERE YEAR=1970

# 2. Give every album in the music library that was created by a given artist
# select * from artist_library WHERE artist_name="The Beatles"

# 3. Give all information frim the music library about a given album
# select * from album_library WHERE album_name="Close To You"


# TODO Creat the tables:
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, PRIMARY KEY (year, artist_name))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# Should consider to choice which `column` will be clustered to become unique and make PRIMARY KEY
query1 = "CREATE TABLE IF NOT EXISTS artist_library "
query1 = query1 + "(artist_name text,  year int, album_name text, PRIMARY KEY (artist_name, year))"
try:
    session.execute(query1)
except Exception as e:
    print(e)

query2 = "CREATE TABLE IF NOT EXISTS album_library "
query2 = query2 + "(album_name text, artist_name text, year int, PRIMARY KEY (album_name, artist_name))"
try:
    session.execute(query2)
except Exception as e:
    print(e)

# TODO INSERT data into the tables:
query = "INSERT INTO music_library (year, artist_name, album_name)"
query = query + " VALUES (%s, %s, %s)"

query1 = "INSERT INTO artist_library (artist_name, year, album_name)"
query1 = query1 + " VALUES (%s, %s, %s)"

query2 = "INSERT INTO album_library (album_name, artist_name, year)"
query2 = query2 + " VALUES (%s, %s, %s)"

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
    
try:
    session.execute(query2, ("Let it Be", "The Beatles", 1970))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("Rubber Soul", "The Beatles", 1965))
except Exception as e:
    print(e)
    
try:
    session.execute(query2, ("My Generation", "The Who", 1965))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("The Monkees", "The Monkees", 1966))
except Exception as e:
    print(e)

try:
    session.execute(query2, ("Close To You", "The Carpenters", 1970))
except Exception as e:
    print(e)


# Validate the Data Model
query = "select * from music_library WHERE YEAR=1970"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name)

# Validate the Data Model 2nd table
query = "select * from artist_library WHERE artist_name='The Beatles' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name, row.album_name, row.year)

# Validate the 3rd table
query = "select * from album_library WHERE album_name='Close To You' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name, row.year, row.album_name)

#Close the session and cluster connection
session.shutdown()
cluster.shutdown()




