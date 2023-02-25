import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
arn = config.get("IAM_ROLE", 'arn')
log_data = config.get("S3", 'log_data')
log_jsonpath = config.get("S3", 'log_jsonpath')
song_data = config.get("S3", 'song_data')

# DROP TABLES


staging_events_table_drop = "DROP TABLE IF EXISTS ""staging_events_table"";"
staging_songs_table_drop = "DROP TABLE IF EXISTS ""staging_songs_table"";"
songplay_table_drop = "DROP TABLE IF EXISTS ""songplay_table"";"
user_table_drop = "DROP TABLE IF EXISTS ""user_table"";"
song_table_drop = "DROP TABLE IF EXISTS ""song_table"";"
artist_table_drop = "DROP TABLE IF EXISTS ""artist_table"";"
time_table_drop = "DROP TABLE IF EXISTS ""time_table"";"



# CREATE TABLES


staging_events_table_create= ("""
CREATE TABLE staging_events_table (
    artist            VARCHAR(MAX),
    auth              VARCHAR(MAX),
    firstName         VARCHAR(MAX),
    gender            VARCHAR(MAX),
    itemInSession     INT, 
    lastName          VARCHAR(MAX),
    length            FLOAT, 
    level             VARCHAR(MAX),
    location          VARCHAR(MAX),
    method            VARCHAR(MAX),
    page              VARCHAR(MAX),
    registration      BIGINT,
    sessionId         INT,
    song              VARCHAR(MAX),
    status            INT,
    ts                BIGINT,
    userAgent         VARCHAR(MAX),
    userId            INT
);
""")



staging_songs_table_create= ("""
CREATE TABLE staging_songs_table (
    num_songs         INT,
    artist_id         VARCHAR(MAX),
    artist_latitude   FLOAT,
    artist_longitude  FLOAT, 
    artist_location   VARCHAR(MAX),
    artist_name       VARCHAR(MAX), 
    song_id           VARCHAR(MAX), 
    title             VARCHAR(MAX),
    duration          FLOAT,
    year              INT
    );
""")


#TO REVIEW PRIMARY TABLE ACCORDING TO STAGING TABLES
songplay_table_create = ("""
CREATE TABLE songplay_table (
    songplay_id         BIGINT IDENTITY(0, 1),
    start_time          TIMESTAMP,
    user_id             INT, 
    level               VARCHAR(MAX), 
    song_id             VARCHAR(MAX), 
    artist_id           VARCHAR(MAX), 
    session_id          INT,
    location            VARCHAR(MAX),
    user_agent          VARCHAR(MAX)
);
""")



user_table_create = ("""
CREATE TABLE user_table (
    user_id             INT,  
    first_name          VARCHAR(MAX), 
    last_name           VARCHAR(MAX),
    gender              VARCHAR(MAX),
    level               VARCHAR(MAX)
);
""")

song_table_create = ("""
CREATE TABLE song_table (
    song_id             VARCHAR(MAX),
    title               VARCHAR(MAX),    
    artist_id           VARCHAR(MAX),
    year                INT,
    duration            FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE artist_table (
    artist_id           VARCHAR(MAX),
    name                VARCHAR(MAX),
    location            VARCHAR(MAX),
    lattitude           FLOAT,
    longitude           FLOAT
);
""")

time_table_create = ("""
CREATE TABLE time_table (
    start_time          TIMESTAMP, 
    hour                INT,
    day                 INT,
    week                INT,
    month               INT,
    year                INT,
    weekday             INT
);
""")



# STAGING TABLES


staging_events_copy = ("""
copy staging_events_table
from '{}'
iam_role '{}'
json '{}';
""").format(log_data, arn, log_jsonpath)


staging_songs_copy = ("""
copy staging_songs_table
from '{}'
iam_role '{}'
json 'auto';
""").format(song_data, arn)





# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  TIMESTAMP 'epoch' + (event.ts/1000 * INTERVAL '1 second') AS start_time,
        event.userid AS user_id,
        event.level AS level,
        song.song_id AS song_id,
        song.artist_id AS artist_Did,
        event.sessionid AS session_id,
        event.location AS location,
        event.useragent AS user_agent    
FROM staging_events_table event
LEFT JOIN staging_songs_table song
    ON event.song = song.title;
""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT userid AS user_id, 
    firstname AS first_name,
    lastname AS last_name,
    gender AS gender,
    level AS level  
FROM staging_events_table event;    
""")

song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
SELECT 
    song_id AS song_id,
    title AS title,
    artist_id AS artist_id,
    year AS year,
    duration AS duration
FROM staging_songs_table;
""")

artist_table_insert = ("""
INSERT INTO artist_table (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT
    artist_id AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS lattitude,
    artist_longitude AS longitude
FROM staging_songs_table;
""")

time_table_insert = ("""
INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
WITH temp_timestamp AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts_new FROM staging_events_table)
SELECT DISTINCT ts_new AS start_time,
    extract(hour from ts_new) AS hour,
    extract(day from ts_new) AS day,
    extract(week from ts_new) AS week,
    extract(month from ts_new) AS month,
    extract(year from ts_new) AS year,
    extract(weekday from ts_new) AS weekday
FROM temp_timestamp;
""")


#COUNT ROWS
staging_events_count = ("""
SELECT COUNT(1) FROM staging_events_table;
""")

staging_songs_count = ("""
SELECT COUNT(1) FROM staging_songs_table;
""")

songplay_table_count  = ("""
SELECT COUNT(1) FROM songplay_table;
""")

user_table_count = ("""
SELECT COUNT(1) FROM user_table;
""")

song_table_count = ("""
SELECT COUNT(1) FROM song_table;
""")

artist_table_count = ("""
SELECT COUNT(1) FROM artist_table;
""")

time_table_count = ("""
SELECT COUNT(1) FROM time_table;
""")


# Insight Finding
top_5_most_popular_artist = ("""
SELECT TOP 5 artist.name AS artist_name, COUNT(1) songplay_count
FROM songplay_table songplay
JOIN artist_table artist
    ON songplay.artist_id = artist.artist_id
WHERE songplay.artist_id IS NOT NULL
GROUP BY artist.name
ORDER BY COUNT(1) DESC;
"""
)

top_5_highest_traffic_hour = ("""
SELECT TOP 5 time.hour, COUNT(1) songplay_count
FROM songplay_table songplay
JOIN time_table time
    ON songplay.start_time = time.start_time
GROUP BY time.hour
ORDER BY COUNT(1) DESC;
"""
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_rows_queries = [staging_events_count, staging_songs_count, songplay_table_count, user_table_count, song_table_count, artist_table_count, time_table_count]
insight_queries = [top_5_most_popular_artist, top_5_highest_traffic_hour]
