import configparser
from redshiftbuilder import RedshiftBuilder


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS stg_event_table"
staging_songs_table_drop = "DROP table IF EXISTS stg_song_table"
songplay_table_drop = "DROP table IF EXISTS songplay_table"
user_table_drop = "DROP table IF EXISTS user_table"
song_table_drop = "DROP table IF EXISTS song_table"
artist_table_drop = "DROP table IF EXISTS artist_table"
time_table_drop = "DROP table IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stg_event_table (
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length NUMERIC,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration NUMERIC,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts NUMERIC,
userAgent VARCHAR,
userId VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_song_table (
num_songs INTEGER,
artist_id text,
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration NUMERIC,
year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
songplay_id int identity (1,1) sortkey,
start_time TIMESTAMP not null,
user_id VARCHAR not null distkey,
level VARCHAR,
song_id VARCHAR not null,
artist_id VARCHAR not null,
session_id INTEGER not null,
location VARCHAR,
user_agent VARCHAR,
primary key(songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
user_id VARCHAR not null sortkey distkey,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR,
primary key(user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
song_id VARCHAR not null sortkey,
title VARCHAR,
artist_id VARCHAR,
year INTEGER,
duration NUMERIC,
primary key(song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
artist_id VARCHAR not null sortkey,
name VARCHAR,
location VARCHAR,
latitude NUMERIC,
longitude NUMERIC,
primary key(artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
start_time TIMESTAMP not null sortkey,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER,
primary key (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY stg_event_table from {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
json {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'arn'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY stg_song_table FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'arn'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (
start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent
)
SELECT
TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time,
e.userId,
e.level,
s.song_id,
s.artist_id,
e.sessionId,
e.location,
e.userAgent
FROM stg_event_table AS e
JOIN stg_song_table AS s
ON (e.artist = s.artist_name)
AND (e.song = s.title)
AND (e.length = s.duration)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO user_table (
user_id,
first_name,
last_name,
gender,
level)
SELECT DISTINCT
e.userId,
e.firstName,
e.lastName,
e.gender,
e.level
FROM stg_event_table AS e;
""")

song_table_insert = ("""
INSERT INTO song_table (
song_id,
title,
artist_id,
year,
duration)
SELECT DISTINCT
s.song_id,
s.title,
s.artist_id,
s.year,
s.duration
FROM stg_song_table AS s;
""")

artist_table_insert = ("""
INSERT INTO artist_table (
artist_id,
name,
location,
latitude,
longitude
)
SELECT DISTINCT
s.artist_id,
s.artist_name,
s.artist_location,
s.artist_latitude,
s.artist_longitude
FROM stg_song_table AS s;
""")

# https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
time_table_insert = ("""
INSERT INTO time_table (
start_time,
hour,
day,
week,
month,
year,
weekday)
SELECT DISTINCT
t.start_time,
EXTRACT (HOUR FROM t.start_time),
EXTRACT (DAY FROM t.start_time),
EXTRACT (WEEK FROM t.start_time),
EXTRACT (MONTH FROM t.start_time),
EXTRACT (YEAR FROM t.start_time),
EXTRACT (WEEKDAY FROM t.start_time)
FROM (SELECT start_time FROM songplay_table) AS t;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
