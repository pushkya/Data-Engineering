import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


#Getting this data from config file.
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")


# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events_table"
staging_songs_table_drop = "drop table if exists staging_songs_table"
songplay_table_drop = "drop table if exists songplay_table"
user_table_drop = "drop table if exists user_table"
song_table_drop = "drop table if exists song_table"
artist_table_drop = "drop table if exists artist_table"
time_table_drop = "drop table if exists time_table"


# CREATE TABLES

staging_events_table_create= (""" create table if not exists staging_events_table(
artist varchar,
auth varchar,
first_name varchar,
gender varchar,
itemInSession integer,
last_name varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration float,
sessionId integer,
song varchar,
status varchar,
ts timestamp,
userAgent varchar,
userId integer
)
""")

staging_songs_table_create = ("""create table if not exists staging_songs_table(
numsongs integer,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year integer
)
""")

songplay_table_create = ("""create table if not exists songplay_table(
songplay_id integer identity(0,1) primary key sortkey,
start_time timestamp not null,
user_id integer not null,
level varchar,
song_id varchar not null,
artist_id varchar not null,
session_id integer not null,
location varchar,
user_agent varchar
)
""")

user_table_create = ("""create table if not exists user_table(
user_id integer primary key sortkey,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)
""")

song_table_create = ("""create table if not exists song_table(
song_id varchar primary key,
title varchar sortkey,
artist_id varchar,
year integer,
duration float
)
""")

artist_table_create = ("""create table if not exists artist_table(
artist_id varchar primary key,
name varchar sortkey,
location varchar,
latitude float,
longitude float
)
""")

time_table_create = ("""create table if not exists time_table(
start_time timestamp primary key sortkey distkey,
hour integer,
day integer,
week integer,
month integer,
year integer,
weekday varchar

)

""")


# STAGING TABLES

staging_events_copy = (""" copy staging_events_table from {}
compupdate off region 'us-west-2'
credentials 'aws_iam_role={}'
format as json {}
timeformat as 'epochmillisecs';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = (""" copy staging_songs_table from {}
compupdate off region 'us-west-2'
credentials 'aws_iam_role={}'
format as JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)


# FINAL TABLES

songplay_table_insert = ("""insert into songplay_table(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct(sevt.ts) as start_time,
sevt.userId as user_id,
sevt.level as level,
sso.song_id as song_id,
sso.artist_id as artist_id,
sevt.sessionId as session_id,
sevt.location as location,
sevt.userAgent as user_agent
from staging_events_table sevt join
staging_songs_table sso on sevt.song = sso.title and sevt.artist = sso.artist_name;

""")

user_table_insert = ("""insert into user_table(user_id, first_name, last_name, gender, level)
select distinct(userId) as user_id,
first_name,
last_name,
gender,
level
from staging_events_table
where user_id is not null;
""")

song_table_insert = ("""insert into song_table(song_id, title, artist_id, year, duration)
select distinct(song_id) as song_id,
title,
artist_id,
year,
duration 
from staging_songs_table
where song_id is not null;
""")

artist_table_insert = ("""insert into artist_table(artist_id, name, location, latitude, longitude)
select distinct(artist_id) as artist_id,
artist_name as name,
artist_location as location,
artist_latitude as latitude,
artist_longitude as longitude
from staging_songs_table
where artist_id is not null;
""")

time_table_insert = ("""insert into time_table(start_time, hour, day, week, month, weekday)
select distinct(ts) as start_time,
extract(hour from ts),
extract(dy from ts),
extract(week from ts),
extract(month from ts),
extract(weekday from ts)
from staging_events_table
where ts is not null;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
