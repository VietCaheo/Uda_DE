## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
song_playlist_session_create = "CREATE table IF NOT EXISTS song_playlist_session "
song_playlist_session_create = song_playlist_session_create + "(userId int, sessionId int, itemInSession int, artist text, song text, firstName text, lastName text, PRIMARY KEY ((userId,sessionId), itemInSession))"
try:
    session.execute(song_playlist_session_create)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header

for line in csvreader:
    ## TO-DO: Assign the INSERT statements into the `query` variable
    song_playlist_session_insert = "INSERT INTO song_playlist_session ( userId, sessionId, itemInSession, artist, song, firstName, lastName)"
    song_playlist_session_insert = song_playlist_session_insert + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    session.execute(song_playlist_session_insert, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))


query2 = "SELECT artist, song, firstName, lastName FROM song_playlist_session WHERE sessionId = 182 and userId = 10"
try:
    rows = session.execute(query2)
except Exception as e:
    print(e)
for row in rows:
    print(row.artist, row.song, row.FirstName, row.LastName)