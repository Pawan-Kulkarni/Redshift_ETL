"""A collection of SQL queries necessary for this project, including table creation, Redshift data copy, data insertion, and business queries."""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN =  config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_data"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_data"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_log_data (
    artist          text ,
    auth            text ,
    firstName       text ,
    gender          text, 
    itemInSession   numeric, 
    lastName        text ,
    length          numeric,
    level           text ,
    location        text ,
    method          text ,
    page            text ,
    registration    numeric,
    sessionId       numeric,
    song            text ,
    status          numeric,
    ts              numeric,
    userAgent       text ,
    userId          text 
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_song_data (
    artist_id           text  NOT NULL,
    artist_latitude     text ,
    artist_location     text,
    artist_longitude    text ,
    artist_name         text ,
    duration            numeric, 
    num_songs           numeric ,
    song_id             text ,
    title               text ,
    year                numeric );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id   INT IDENTITY(0,1) NOT NULL        PRIMARY KEY,
    start_time    TIMESTAMP                             sortkey,
    user_id       text,
    level         text,
    song_id       text                                  distkey,
    artist_id     text,
    session_id    numeric,
    location      text,
    userAgent text  );
    
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id     text   NOT NULL   PRIMARY KEY,
    first_name  text,
    last_name   text,
    gender      text,
    level       text    
)
DISTSTYLE ALL
;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id         text       PRIMARY KEY,  
    title           text,
    artist_id       text  NOT NULL,
    year            numeric,
    duration        numeric)
    DISTSTYLE EVEN;

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id       text NOT NULL   PRIMARY KEY,
    name            text ,
    location        text ,
    latitude        text ,
    longitude       text 
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times(
    start_time      TIMESTAMP NOT NULL   PRIMARY KEY   sortkey,    
    hour            numeric,
    day             numeric,
    week            numeric,
    month           numeric,
    year            numeric,
    weekday         numeric);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_log_data from {}
    credentials 'aws_iam_role={}'
    JSON {}
     region 'us-west-2';

""".format(LOG_DATA,ARN,LOG_JSONPATH))

staging_songs_copy = ("""
copy staging_song_data from {}
    credentials 'aws_iam_role={}'
    format json as 'auto'
    region 'us-west-2';

""".format(SONG_DATA,ARN))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id,
    artist_id, session_id, location, userAgent)
SELECT
    TIMESTAMP 'epoch' + e.ts * INTERVAL '0.001 second' AS start_time,
    e.userId AS user_id,
    e.level AS level,
    s.song_id AS song_id,
    s.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.userAgent AS userAgent
FROM staging_log_data e 
LEFT JOIN staging_song_data s 
ON e.artist = s.artist_name 
AND e.song = s.title
AND e.length = s.duration
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,first_name,last_name,gender,level)
    Select distinct userId , firstName , lastName , gender , level 
    FROM staging_log_data 
    where staging_log_data.page = 'NextSong'
    AND userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs ( song_id,title,artist_id,year,duration )
    Select distinct song_id , title , artist_id,year,duration 
    FROM staging_song_data 
    WHERE song_id IS NOT NULL;
    
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude)
    Select distinct artist_id , artist_name , artist_location,artist_latitude,artist_longitude 
    FROM staging_song_data
    WHERE artist_id IS NOT NULL;

""")

time_table_insert = ("""
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT
    distinct TIMESTAMP 'epoch' + e.ts * INTERVAL '0.001 second' AS start_time,
    EXTRACT(HOUR FROM (TIMESTAMP 'epoch' + e.ts * INTERVAL '0.001 second')) AS hour,
    EXTRACT(DAY FROM (TIMESTAMP 'epoch' + e.ts * INTERVAL '0.001 second')) AS day,
    EXTRACT(WEEK FROM (TIMESTAMP 'epoch' + e.ts * INTERVAL '0.001 second')) AS week,
    EXTRACT(MONTH FROM (TIMESTAMP 'epoch' + e.ts * INTERVAL '0.001 second')) AS month,
    EXTRACT(YEAR FROM (TIMESTAMP 'epoch' + e.ts * INTERVAL '0.001 second')) AS year,
    EXTRACT(DOW FROM (TIMESTAMP 'epoch' + e.ts * INTERVAL '0.001 second')) AS weekday
FROM staging_log_data AS e
WHERE e.page='NextSong'
AND e.ts IS NOT NULL;
""")

# Some of  Business Questions answered.
most_famous_artists = """select a.name , count(*) as number_of_times_songs_of_artists_played from songplays s
INNER JOIN artists a on a.artist_id = s.artist_id
Group by a.name
ORDER BY number_of_times_songs_of_artists_played desc
LIMIT 10;"""

most_famous_songs = """
select sg.title , count(*) as number_of_times_songs_played from songplays s
INNER JOIN songs sg on s.song_id = sg.song_id
Group by sg.title 
ORDER BY number_of_times_songs_played desc
LIMIT 10;
"""

most_songplays_by_individual_users = """
select u.user_id , u.first_name , count(*) as user_time_spent from songplays s
INNER JOIN users u ON s.user_id = u.user_id
GROUP BY u.user_id , u.first_name
ORDER BY user_time_spent DESC
LIMIT 10;
"""

most_songplays_by_free_users = """
select u.user_id , u.first_name , count(*) as user_time_spent from songplays s
INNER JOIN users u ON s.user_id = u.user_id
WHERE u.level = 'free'
GROUP BY u.user_id , u.first_name
ORDER BY user_time_spent DESC
LIMIT 10;

"""

daily_songplay_trends = """

select t.day , count(*) as daily_songplay_count FROM songplays s
INNER JOIN times as t ON t.start_time = s.start_time 
GROUP BY t.day 
ORDER BY t.day ASC;

"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
example_questions = [most_famous_artists,most_famous_songs,most_songplays_by_individual_users,most_songplays_by_free_users,daily_songplay_trends]


