# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = ("""CREATE TABLE users (
                        user_id int PRIMARY KEY, 
                        first_name varchar NOT NULL, 
                        last_name varchar NOT NULL, 
                        gender varchar, 
                        level varchar
                        )
""")

song_table_create = ("""CREATE TABLE songs (
                        song_id varchar PRIMARY KEY, 
                        title varchar NOT NULL, 
                        artist_id varchar, 
                        year int, 
                        duration real
                        )
""")

artist_table_create = ("""CREATE TABLE artists (
                            artist_id varchar PRIMARY KEY, 
                            name varchar NOT NULL, 
                            location varchar, 
                            latitude decimal, 
                            longitude decimal
                            )
""")

time_table_create = ("""CREATE TABLE time (
                        start_time timestamp PRIMARY KEY, 
                        hour int, 
                        day int, 
                        week int, 
                        month int, 
                        year int, 
                        weekday int
                        )
""")

# SERIAL auto-increments the songplay_id as records are inserted
# start_time and user_id must be not null and refer to records in their respective tables
# ideally song_id and artist_id would have this constraint too, but the songs dataset is a small
# subset on all the songs appearing in the log files, so the constraint would be violated
songplay_table_create = ("""CREATE TABLE songplays (
                            songplay_id SERIAL PRIMARY KEY, 
                            start_time timestamp NOT NULL REFERENCES time (start_time), 
                            user_id int NOT NULL REFERENCES users (user_id), 
                            level varchar, 
                            song_id varchar, 
                            artist_id varchar, 
                            session_id int NOT NULL,
                            location varchar, 
                            user_agent varchar,
                            UNIQUE (start_time, user_id, session_id)
                            )
""")

# INSERT RECORDS

# used ON CONFLICT to avoid violating primary key constraint on song_id
song_table_insert = ("""INSERT INTO songs 
                        (song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT songs_pkey
                        DO NOTHING
""")

# used ON CONFLICT to avoid violating primary key constraint on artist_id
artist_table_insert = ("""INSERT INTO artists 
                        (artist_id, 
                        name, 
                        location, 
                        latitude, 
                        longitude
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT artists_pkey
                        DO NOTHING
""")

# used ON CONFLICT to handle duplicate record inserts given the primary key constraint on user_id
# if user_id already exists, it will update the name, gender and level instead
user_table_insert = ("""INSERT INTO users 
                        (user_id, 
                        first_name, 
                        last_name, 
                        gender, 
                        level
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT users_pkey
                            DO UPDATE
                                SET first_name = EXCLUDED.first_name,
                                    last_name = EXCLUDED.last_name,
                                    gender = EXCLUDED.gender,
                                    level = EXCLUDED.level
                            
""")

# use COPY for quicker bulk insert
songplays_table_copy = ('''COPY songplays (start_time, 
                                        user_id, 
                                        level, 
                                        song_id, 
                                        artist_id, 
                                        session_id,
                                        location,
                                        user_agent
                                        ) FROM STDIN
''')
                   

# FIND SONGS

song_select = ("""SELECT S.song_id, A.artist_id
                    FROM songs S
                    JOIN artists A on A.artist_id = S.artist_id
                    WHERE S.title = %s AND A.name = %s and S.duration = %s
""")

# etl.py will write the results of this query to a DataFrame, then merge it to the raw log data to set up the songs_table_copy bulk insert
select_artists_songs = ("""SELECT S.song_id, A.artist_id, S.title song_name, A.name artist_name, S.duration
                            FROM songs S
                            JOIN artists A on A.artist_id = S.artist_id
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]