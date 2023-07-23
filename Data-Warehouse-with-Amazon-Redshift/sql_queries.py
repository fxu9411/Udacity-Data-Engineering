import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST = config.get("CLUSTER","HOST")
ARN = config.get("IAM_ROLE", "ARN")

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = """
DROP TABLE IF EXISTS staging_events;
"""
staging_songs_table_drop = """
DROP TABLE IF EXISTS staging_songs;
"""
songplay_table_drop = """
DROP TABLE IF EXISTS songplays;
"""
user_table_drop = """
DROP TABLE IF EXISTS users;
"""
song_table_drop = """
DROP TABLE IF EXISTS songs;
"""
artist_table_drop = """
DROP TABLE IF EXISTS artist;
"""
time_table_drop = """
DROP TABLE IF EXISTS time;
"""

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INTEGER sortkey distkey,
    song VARCHAR,
    status VARCHAR,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR SORTKEY,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR DISTKEY,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
    (songplay_id INTEGER PRIMARY KEY IDENTITY(0,1),
    start_time TIMESTAMP SORTKEY,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id VARCHAR NOT NULL DISTKEY,
    location VARCHAR,
    user_agent VARCHAR)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
    (user_id INTEGER PRIMARY KEY SORTKEY DISTKEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR NOT NULL,
year int SORTKEY DISTKEY,
duration FLOAT)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(artist_id VARCHAR PRIMARY KEY DISTKEY,
name varchar SORTKEY,
location VARCHAR,
latitude FLOAT,
longitude FLOAT)
""")

time_table_create = ("""
CREATE TABLE time
(start_time TIMESTAMP PRIMARY KEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {0}
credentials 'aws_iam_role={1}'
compupdate off region 'us-west-2'
json {2};
""".format(LOG_DATA, ARN, LOG_JSONPATH))

staging_songs_copy = ("""
COPY staging_songs FROM {0}
credentials 'aws_iam_role={1}'
compupdate off region 'us-west-2'
JSON 'auto';
""".format(SONG_DATA, ARN))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
    e.userId, 
    e.level, 
    s.song_id,
    s.artist_id, 
    e.sessionId,
    e.location, 
    e.userAgent
FROM staging_events e, staging_songs s
WHERE e.page = 'NextSong' 
AND e.song = s.title 
AND e.artist = s.artist_name 
AND e.length = s.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  
    userId, 
    firstName, 
    lastName, 
    gender, 
    level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
