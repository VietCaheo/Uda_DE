""" Using the WHERE Clause """

# Import Apache Cassandra python package
import cassandra

#First let's create a connection to the database
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

# Connect to our Keyspace. Compare this to how we had to create a new session in PostgreSQL
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# Asking 4 questions of our data:
# 1. Give me every album in my music library that was released in a 1965 year
# 2. Give me the album that is in my music library that was released in 1965 by "The Beatles"
# 3. Give me all the albums released in a given year that was made in London
# 4. Give me the city that the album "Rubber Soul" was recorded
query = "CREATE TABLE IF NOT EXISTS music_library "
query = query + "(year int, artist_name text, album_name text, city text, PRIMARY KEY (album_name, artist_name, year))"
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

# Query 1. Give me every album in my music library that was released in a 1965 year
query = "SELECT * FROM music_library WHERE YEAR=1965"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

# Query 2. Give me the album that is in my music library that was released in 1965 by "The Beatles"
query = "SELECT * FROM music_library WHERE YEAR=1965 AND artist_name='The Beatles'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)

# Query 3. Give me all the albums released in a given year that was made in London
# incase small Data amount were clearly known, -> can be used ALLOW FILTERING
# query = "SELECT * FROM music_library WHERE YEAR=1965 AND city='Lodon'"
query = "SELECT * FROM music_library WHERE YEAR=1965 AND artist_name='The Who'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)
# Query 4: Give me the city that the album "Rubber Soul" was recorded
query = "SELECT * FROM music_library WHERE album_name='Rubber Soul'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.year, row.artist_name, row.album_name, row.city)


