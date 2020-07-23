# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAY_TBL"
user_table_drop = "DROP TABLE IF EXISTS USER_TBL"
song_table_drop = "DROP TABLE IF EXISTS SONG_TBL"
artist_table_drop = "DROP TABLE IF EXISTS ARTIST_TBL"
time_table_drop = "DROP TABLE IF EXISTS TIME_TBL"
stg_table_drop ="DROP TABLE IF EXISTS SONG_STG"

# CREATE TABLES

#create_seq=(""""CREATE SEQUENCE SONGPLAY_TBL_seq"""";)
#SONGPLAY_ID SERIAL PRIMARY KEY ,

songplay_table_create = ("""
CREATE TABLE SONGPLAY_TBL(
SONGPLAY_ID SERIAL PRIMARY KEY ,
START_TIME TIMESTAMP NOT NULL ,
USER_ID int NOT NULL,
SONG_ID Varchar(30) NOT NULL,
ARTIST_ID Varchar(50) NOT NULL,
SESSION_ID Varchar(50),
LOCATION Varchar(100),
USER_AGENT Varchar(200),
LEVEL  varchar(10)
)

""")

song_table_create = ("""
CREATE TABLE SONG_TBL
 (SONG_ID Varchar(30) PRIMARY KEY, 
 TITLE Varchar(100), 
 ARTIST_ID Varchar(50), 
 YEAR int, 
 DURATION float
 );
""")
artist_table_create = ("""
CREATE TABLE ARTIST_TBL(
ARTIST_ID Varchar(50) PRIMARY KEY, 
NAME Varchar(100), 
LOCATION Varchar(100), 
LATITUDE float, 
LONGITUDE float
);
""")

time_table_create = ("""
CREATE TABLE TIME_TBL(
START_TIME TIMESTAMP PRIMARY KEY,
YEAR  int,
MONTH int,
WEEK  int,
DAY   int,
HOUR  int,
WEEKDAY Varchar(15)
)
""")

user_table_create = ("""
CREATE TABLE USER_TBL (
USER_ID int PRIMARY KEY, 
FNAME varchar(20), 
LNAME varchar(20), 
GENDER char(1), 
LEVEL varchar(10)
);
""")

stg_table_create = ("""
CREATE TABLE SONG_STG(
START_TIME TIMESTAMP,
SESSION_ID int ,
USER_AGENT varchar(200),
SONG_TITLE Varchar(300), 
ARTIST_NAME Varchar(100),
SONG_DURATION float,
USER_ID int,
LEVEL varchar(10)
);
""")

# INSERT RECORDS

song_table_insert = ("""INSERT INTO SONG_TBL (SONG_ID, TITLE,ARTIST_ID, YEAR, DURATION) values (%s,%s,%s,%s,%s) 
ON CONFLICT(SONG_ID) DO UPDATE SET 
TITLE=EXCLUDED.TITLE,
ARTIST_ID=EXCLUDED.ARTIST_ID,
YEAR=EXCLUDED.YEAR,
DURATION=EXCLUDED.DURATION
"""  )

artist_table_insert = ("""INSERT INTO ARTIST_TBL (ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE) values (%s,%s,%s,%s,%s)
ON CONFLICT(ARTIST_ID) DO UPDATE SET 
NAME=EXCLUDED.NAME,
LOCATION=EXCLUDED.LOCATION,
LATITUDE=EXCLUDED.LATITUDE,
LONGITUDE=EXCLUDED.LONGITUDE
""")

time_table_insert = ("""INSERT INTO TIME_TBL(START_TIME ,YEAR,MONTH,WEEK,DAY,HOUR,WEEKDAY) values (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (START_TIME) DO NOTHING
""")

user_table_insert=(""" INSERT INTO USER_TBL (USER_ID, FNAME, LNAME, GENDER, LEVEL) values (%s,%s,%s,%s,%s) 
ON CONFLICT(USER_ID) DO UPDATE SET 
FNAME=EXCLUDED.FNAME,
LNAME=EXCLUDED.LNAME,
GENDER=EXCLUDED.GENDER,
LEVEL=EXCLUDED.LEVEL""")

stg_table_insert= (""" INSERT INTO SONG_STG (SESSION_ID, USER_AGENT, SONG_TITLE,ARTIST_NAME, SONG_DURATION,USER_ID,LEVEL,START_TIME) values (%s,%s,%s,%s,%s,%s,%s,%s) """)


songplay_table_insert = ("""INSERT INTO SONGPLAY_TBL(START_TIME,USER_ID,ARTIST_ID,SONG_ID,SESSION_ID,LOCATION,USER_AGENT,LEVEL) 
SELECT G.START_TIME,G.USER_ID,S.SONG_ID,A.ARTIST_ID ,G.SESSION_ID,A.LOCATION,G.USER_AGENT,G.LEVEL 
FROM SONG_STG G LEFT JOIN ARTIST_TBL A ON G.ARTIST_NAME=A.NAME 
                LEFT JOIN SONG_TBL S   ON G.SONG_TITLE=S.TITLE AND G.SONG_DURATION =S.DURATION;
""")

# FIND SONGS

song_select = ("""SELECT * FROM SONGPLAY_TBL """)

# QUERY LISTS


create_table_queries = [song_table_create,artist_table_create,user_table_create,time_table_create,stg_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop,stg_table_drop]

#create_table_queries=[stg_table_create]
#drop_table_queries=[stg_table_drop]
