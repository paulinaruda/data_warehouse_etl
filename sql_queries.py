import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
    
key = config.get('AWS', 'KEY')
secret = config.get('AWS', 'secret')
roleArn = config.get('IAM_ROLE', 'ARN')
log_data = config.get('S3', 'log_data')
song_data = config.get('S3', 'song_data')
log_jsonpath = config.get('S3', 'log_jsonpath')

# DROP TABLES

staging_events_table_drop = "drop table if exists logdata;"
staging_songs_table_drop = "drop table if exists songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists song;"
artist_table_drop = "drop table if exists artist;"
time_table_drop = "drop table if exists time;"


###### Log data
# itemInSession : int64
# sessionId : int64
# status : int64
# ts : int64
# length : float64
# registration : float64

# Song data
# artist_latitude : float64
# artist_longitude : float64
# duration : float64
# num_songs : int64
# year : int64

######


# CREATE TABLES

## Columns with null values in source table - log data: 
# artist
# firstName
# gender
# lastName
# length
# location
# registration
# song
# userAgent

staging_logdata_table_create= ("""
create table  logdata
(
artist        varchar(255),
auth          varchar(20),
firstName     varchar(20),
gender        varchar(4),
itemInSession bigint,      
lastName      varchar(10), 
length        float, 
level         varchar(5)  not null,
location      varchar(50), 
method        varchar(5)  not null, 
page          varchar(30) not null, 
registration  float, 
sessionId     bigint    not null, 
song          varchar(255), 
status        bigint    not null, 
ts            bigint    not null, 
userAgent     varchar(255),
userId        varchar(20) not null 
);
""")

## Columns with null values in source table - song data:
# artist_latitude
# artist_location
# artist_longitude

staging_songs_table_create = ("""
create table      songs
(
num_songs         integer     not null, 
artist_id         varchar(20),
artist_latitude   float,
artist_longitude  float,
artist_location   varchar(255),
artist_name       varchar(255), 
song_id           varchar(20),
title             varchar(255) not null,  
duration          float,
year              integer
);
""")



songplay_table_create = ("""
CREATE TABLE songplays (
    sp_songplay_id    BIGINT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    sp_start_time     TIMESTAMP,
    sp_user_id        varchar(20) not null,
    sp_level          VARCHAR(5),
    sp_song_id        varchar(20),
    sp_artist_id      varchar(20),
    sp_session_id     bigint,
    sp_location       VARCHAR(50),
    sp_user_agent     VARCHAR(255)
);
""")


user_table_create = ("""
create table      users 
(
user_id           integer     not null,
first_name        varchar(20),
last_name         varchar(20), 
gender            varchar(4),
level             varchar(5)   not null
);
""")


song_table_create = ("""
create table     song
(
song_id          varchar(20), 
title            varchar(255) not null,
artists_id       varchar(20), 
year             integer,
duration         float
);
""")


artist_table_create = ("""
create table     artist
(
artist_id        varchar(20),
name             varchar(255),
location         varchar(255),
latitude         float,
longitude        float
);
""")

time_table_create = ("""
create table     time
(
start_time       timestamp not null,
hour             integer not null,
day              integer not null,
week             integer not null,
month            integer not null,
year             integer not null,
weekday          integer not null
);
""")

# STAGING TABLES

staging_logdata_copy = (f"""
copy logdata from '{log_data}'
access_key_id '{key}' 
secret_access_key '{secret}'
region 'us-west-2'
JSON '{log_jsonpath}';
""")


staging_songs_copy = (f"""
copy songs from '{song_data}'
access_key_id '{key}' 
secret_access_key '{secret}'
region 'us-west-2'
JSON 'auto'
""")


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (sp_start_time, sp_user_id, sp_level, sp_song_id, sp_artist_id, sp_session_id, sp_location, sp_user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + INTERVAL '1 second' * (l.ts / 1000)::bigint AS sp_start_time,
    CAST(l.userId AS integer) AS sp_user_id,
    l.level as sp_level,
    s.song_id as sp_song_id,
    s.artist_id as sp_artist_id,
    l.sessionid AS sp_session_id,
    l.location as sp_location,
    l.userAgent AS sp_user_agent
FROM logdata l
LEFT JOIN songs s ON (l.song = s.title AND l.artist = s.artist_name and l.length = s.duration)
WHERE l.page = 'NextSong';
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select 
distinct cast(user_id as int) as user_id, first_name, last_name, gender, level
from (
select
userid AS user_id,
firstName as first_name, 
lastName as last_name, 
gender, 
level
from logdata
where userid != ' '
) 
order by cast(user_id as int)
;
""")

song_table_insert = ("""
insert into song (song_id, title, artists_id, year, duration)
select distinct * from 
(
select 
song_id as song_id, 
title, 
artist_id as artists_id, 
year, 
duration
from songs
)
;
""")

artist_table_insert = ("""
insert into artist (artist_id, name, location, latitude, longitude)
select distinct * from 
(
select 
artist_id as artist_id, 
artist_name as name, 
artist_location as location,  
artist_latitude as latitude, 
artist_longitude as longitude
from songs
)
;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT distinct start_time, hour, day, week, month, year, weekday from
(SELECT
  TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
  EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
  EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
  EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
  EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
  EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
  EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
FROM logdata)
;
""")

# TABLE COUNTS

songplay_table_count = ("select count(*) from songplays")
user_table_count = ("select count(*) from users")
song_table_count = ("select count(*) from song")
artist_table_count = ("select count(*) from artist")
time_table_count = ("select count(*) from time")
staging_songs_count =  ("select count(*) from songs")
staging_logdata_count = ("select count(*) from logdata")

# QUESTIONS AND QUERIES

questions_dict = {
    'question1': "What is the most played song?",
    'query1': ("""
        select title from (select s.title, count(*) 
        from songplays sp
        left join song s
        on sp.sp_song_id = s.song_id
        where sp_song_id is not null
        group by s.title
        order by count(*) desc
        limit 1)
    """),
    'question2': "What is the most popular artist?",
    'query2': ("""
        select name from (select a.name, count(*) 
        from songplays sp
        left join artist a
        on sp.sp_artist_id = a.artist_id
        where sp_artist_id is not null
        group by a.name
        order by count(*) desc
        limit 1)
    """),
    'question3': "What are the 3 hours in a day where traffic is the biggest?",
    'query3': ("""
        select hour from(select hour, count(*) as songplays_count
        from (select s.sp_songplay_id, t.hour
        from songplays s 
        join time t 
        on s.sp_start_time = t.start_time)
        group by hour
        order by count(*) desc) limit 3
    """),
    'question4': "What is the most played year for all songs?",
    'query4': ("""
        select year from (select s.year, count(*) 
        from songplays sp
        left join song s
        on sp.sp_song_id = s.song_id
        where sp_song_id is not null
        and s.year != 0
        group by s.year
        order by count(*) desc
        limit 1)
    """)
}

# QUERY LISTS

create_table_queries = [staging_logdata_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_logdata_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
table_counts_queries = [staging_logdata_count, staging_songs_count, songplay_table_count, user_table_count, song_table_count, artist_table_count, time_table_count]
