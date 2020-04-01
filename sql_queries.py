# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE  IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY,
start_time timestamp REFERENCES time(start_time),
user_id int REFERENCES users(user_id),
level varchar NOT NULL,
song_id varchar REFERENCES songs(song_id),
artist_id varchar REFERENCES artists(artist_id),
session_id varchar NOT NULL,
location varchar,
user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id int primary key,
 first_name varchar NOT NULL,
 last_name varchar,
 gender varchar,
 level varchar NOT NULL

)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar PRIMARY KEY,
title varchar NOT NULL,
artist_id varchar,
year int,
duration numeric NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY,
 name varchar NOT NULL,
 location varchar,
 latitude float,
 longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time timestamp PRIMARY KEY,
 hour int,
 day int,
 week int,
 month int,
 year int,
 weekday int)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    insert into users (user_id , first_name , last_name, gender, level) values (%s,%s,%s,%s,%s)
    ON CONFLICT (user_id) do update
    set first_name=excluded.first_name,
    last_name=excluded.last_name,
    gender=excluded.gender,
    level=excluded.level
""")


song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration) values (%s,%s,%s,%s,%s) ON CONFLICT (song_id) do nothing
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude) values (%s,%s,%s,%s,%s) ON CONFLICT (artist_id) do nothing
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day , week, month, year , weekday) values  (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (start_time) do nothing
""")

# FIND SONGS

song_select = ("""
SELECT
    s.song_id, s.artist_id
FROM
    songs s
        JOIN artists a ON s.artist_id = a.artist_id
WHERE
    s.title = %s
    AND a.name = %s
    AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]