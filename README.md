# Sparkify Analytic Project

## Overview
An AWS RedShift cluster is setup to store data to be used to analyze event log collect from Sparkify Application.
We can utilize this database to find insight from Sparkify's user behaviour. 
For example, we can find which artist is the most popular one or what day of week has the highest traffic.

## Database schema
- The database consists of 2 parts
    1. Staging - intermediary data of event logs and songs which are copied from AWS S3
    2. Analytics - main tables organized as star schema tables which is easy to query and join between tables


## ETL
- Event log and song data which are stored on AWS S3 will be loaded into staging tables in the cluster. 
    - List of staging tables
        - staging_events_table
        - staging_songs_table
- After all data are available in staging tables, data in the staging tables will be transform into 5 analytic table
    - List of analytic tables
        - songplay_table
        - song_table
        - artist_table
        - user_table
        - time_table



> ## Examples of query result
> Executing query...
> "
> SELECT COUNT(1) FROM staging_events_table;
> "
> [(8056,)]
> ----------
> 
> Executing query...
> "
> SELECT COUNT(1) FROM staging_songs_table;
> "
> [(14896,)]
> ----------
> 
> Executing query...
> "
> SELECT COUNT(1) FROM songplay_table;
> "
> [(8390,)]
> ----------
> 
> Executing query...
> "
> SELECT COUNT(1) FROM user_table;
> "
> [(107,)]
> ----------
> 
> Executing query...
> "
> SELECT COUNT(1) FROM song_table;
> "
> [(14896,)]
> ----------
> 
> Executing query...
> "
> SELECT COUNT(1) FROM artist_table;
> "
> [(10025,)]
> ----------
> 
> Executing query...
> "
> SELECT COUNT(1) FROM time_table;
> "
> [(8023,)]
> ----------
> 
> Finding insight from data...
> Executing query...
> "
> SELECT TOP 5 artist.name AS artist_name, COUNT(1) songplay_count
> FROM songplay_table songplay
> JOIN artist_table artist
>     ON songplay.artist_id = artist.artist_id
> WHERE songplay.artist_id IS NOT NULL
> GROUP BY artist.name
> ORDER BY COUNT(1) DESC;
> "
> [('Dwight Yoakam', 37), ('Carleen Anderson', 17), ('Working For A Nuclear Free City', 13), ('Frozen Plasma', 13), ('Gemma Hayes', 13)]
> ----------
> 
> Executing query...
> "
> SELECT TOP 5 time.hour, COUNT(1) songplay_count
> FROM songplay_table songplay
> JOIN time_table time
>     ON songplay.start_time = time.start_time
> GROUP BY time.hour
> ORDER BY COUNT(1) DESC;
> "
> [(16, 644), (18, 608), (15, 603), (17, 587), (14, 514)]
