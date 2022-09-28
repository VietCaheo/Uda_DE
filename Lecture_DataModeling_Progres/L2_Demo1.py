""" Creating Normalized Tables (using PostgreSQL)"""

import psycopg2

#Creat a connection to the database, get a cursor, and set autocommit to true
try:
    conn = psycopg2.connect("dbname = l2demo1")
except:
    psycopg2.Error as e:
    print("Error: Could not make connection to the Postgre database")
    print(e)
try:
    cur =  conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)

#Let's imagine we have a table called Music Library
# column 0: Album Id
# column 1: Album name
# column 2: Artist name
# column 3: Year
# column 4: list of songs

# Creat table and insert data
try:
    cur.execute("CREATE TABLE IF NOT EXISTS music_library (album_id int, \
                                                           album_name varchar, artist_name varchar, \
                                                            year int, songs text[]);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("INSRER INTO music_library (album_id, album_name, artist_name, year, songs) \
                VALUES (%s, %s, %s, %s, %s)", \
                (1, "Rubber Soul", "The Beatles", 1965, ["Michelle", "Think for yourself", "In My Life"])
               )
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)
try:
    cur.execute("INSRER INTO music_library (album_id, album_name, artist_name, year, songs) \
                VALUES (%s, %s, %s, %s, %s)", \
                (1, "Let It Be", "The Beatles", 1970, ["Let It Be", "Across The Universe"])
               )
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print(e)

# To validate table just created
try:
    cur.execute("SELECT * FROM music_library;")
except psycopg2.Error as e:
    print ("Error: select *")
    print(e)
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()
# Moving to 1st Normal Form (1NF): remove any collection or list of data -> to ensure rule1: data is atomic
# We are going to creat music_library2 with
# col 0: Album ID
# col 1: Album Name
# col 2: Artist Name
# col 3: Year
# col 4: Song Name
try: 
    cur.execute("CREAT TABLE IF NOT EXISTS music_library2 (album_id int, \
                                                           album_name varchar, artist_name varchar, \
                                                           year int, song_name varchar);")
except: psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name) \
                VALUES (%s, %s, %s, %s, %s)", \
                (1, "Rubber Soul", "The Beatles", 1965, "Michelle"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name) \
                VALUES (%s, %s, %s, %s, %s)", \
                (1, "Rubber Soul", "The Beatles", 1965, "Think For Yourself"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, "Rubber Soul", "The Beatles", 1965, "In My Life"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2, "Let It Be", "The Beatles", 1970, "Let It Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2, "Let It Be", "The Beatles", 1970, "Across The Universe"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# Do validate table that just created by print it out
try: 
    cur.execute("SELECT * FROM music_library2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# Moving to 2nd Normal Form (2NF)
# Primary Key album_id is not unique in 1NF -> We need break up into two tables: Album_Library and Song_Library

# Table Name: album_library 
# column 0: Album Id
# column 1: Album Name
# column 2: Artist Name
# column 3: Year

# Table Name: song_library
# column 0: Song Id
# column 1: Song Name
# column 3: Album Id

""" Creat Tables and Insert Rows for each Table """
try:
    cur.execute("CREAT TABLE IF NOT EXISTS album_library(album_id int, album_name varchar, artist_name varchar, year int);")
except: psycopg2.Error as e:
    print("Error: Issue when creating table")
    print(e)

try:
    cur.execute("CREAT TABLE IF NOT EXISTS song_library(song_id int, album_id int, song_name varchar);")
except: psycopg2.Error as e:
    print("Error: Issue when creating table")
    print(e)

# Insert rows for table album_library
try:
    cur.execute("INSERT INTO album_library (album_id, album_name, artist_name, year) \
                VALUES (%s, %s, %s, %s)", \
                (1, "Rubber Soul", "The Beatles", 1965))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO album_library (album_id, album_name, artist_name, year) \
                VALUES (%s, %s, %s, %s)", \
                (2, "Let It Be", "The Beatles", 1970))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# Insert rows for table song_library
try:
    cur.execute("INSERT INTO song_library (song_id, album_id, song_name) \
                VALUES (%s, %s, %s)", \
                (1, 1, "Michelle"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try:
    cur.execute("INSERT INTO song_library (song_id, album_id, song_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, "Think For Yourself"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO song_library (song_id, album_id, song_name) \
                 VALUES (%s, %s, %s)", \
                 (3, 1, "In My Life"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO song_library (song_id, album_id, song_name) \
                 VALUES (%s, %s, %s)", \
                 (4, 2, "Let It Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO song_library (song_id, album_id, song_name) \
                 VALUES (%s, %s, %s)", \
                 (5, 2, "Across the Universe"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# To Validate tables that have created
print("Table: album_library\n")
try: 
    cur.execute("SELECT * FROM album_library;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: song_library\n")
try: 
    cur.execute("SELECT * FROM song_library;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)
row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# Do JOIN on just created tables to get all information we had in our first table
try: 
    cur.execute("SELECT * FROM album_library JOIN\
                 song_library ON album_library.album_id = song_library.album_id ;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# Moving to 3rd Normal Form (3NF)

# Table name: album_library2
# column 0: Album Id
# column 1: Album Name
# column 2: Artist Id
# column 3: Year

# Table Name: song_library
# column 0: Song Id
# column 1: Song Name
# column 3: Album Id

# Table Name: artist_library
# column 0: Artist Id
# column 1: Artist Name


#Creat new two tables: album_library2 and artist_library
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS album_library2 (album_id int, \
                                                           album_name varchar, artist_id int, \
                                                           year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS artist_library (artist_id int, \
                                                           artist_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

# Insert Rows for new tables
try: 
    cur.execute("INSERT INTO album_library2 (album_id, album_name, artist_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Rubber Soul", 1, 1965))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO album_library2 (album_id, album_name, artist_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Let It Be", 1, 1970))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO artist_library (artist_id, artist_name) \
                 VALUES (%s, %s)", \
                 (1, "The Beatles"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    

# Validate tables just created and existing one: `song_library`
print("Table: album_library2\n")
try: 
    cur.execute("SELECT * FROM album_library2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: song_library\n")
try: 
    cur.execute("SELECT * FROM song_library;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

##Doublechecking that data is in the table
print("\nTable: artist_library\n")
try: 
    cur.execute("SELECT * FROM artist_library;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# Do JOIN to get all information
try: 
    cur.execute("SELECT * FROM (artist_library JOIN album_library2 ON \
                               artist_library.artist_id = album_library2.artist_id) JOIN \
                               song_library ON album_library2.album_id=song_library.album_id;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# Drop the tables
try: 
    cur.execute("DROP table music_library")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table music_library2")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table album_library")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table song_library")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table album_library2")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table artist_library")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)

# Close cursor and connection
cur.close()
conn.close()



