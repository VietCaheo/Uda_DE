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

# 2. Give every album in the music library that was created by a given artist

# 3. Give all information frim the music library about a given album

# TODO Creat the tables:
query = "##### "
query = query + "####"
try:
    session.execute(query)
except Exception as e:
    print(e)

query1 = "#### "
query1 = query1 + "#####"
try:
    session.execute(query1)
except Exception as e:
    print(e)

query2 = "#### "
query2 = query2 + "#####"
try:
    session.execute(query2)
except Exception as e:
    print(e)



