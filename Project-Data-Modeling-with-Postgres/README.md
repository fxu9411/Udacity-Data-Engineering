# Context
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Objective
1. Define fact and dimension tables for a star schema for a particular analytic focus
2. Load the songs data and logs data into the database through the ETL pipeline

# Data File Walkthrough
In the "data" directory, the entire dataset is split into two parts:
1. log_data: data files in the log_data contain the detail of the songplays from each users, including the name of artist, first/last name of the user, gender, the time consumed on the app, level of membership, location, what song they listen to and the user agent(the browser information).  
2. song_data: data files in song_data contain the song and artist related information, including artist ID, location of the artist, title of the song, duration and the published year.

# Database Design
1. Fact Table: songplays
songplay_id SERIAL PRIMARY KEY,
start_time TIMESTAMP,
user_id varchar,
level varchar,
song_id varchar,
artist_id varchar,
session_id varchar,
localtion varchar,
user_agent varchar

2. Dimension Table: users
user_id varchar PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar

3. Dimension Table: songs
song_id varchar PRIMARY KEY,
title varchar,
artist_id varchar,
year int,
duration float

4. Dimension Table: artists
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude float,
longitude float

5. Dimension Table: time
start_time timestamp,
year int,
month int,
week int,
day int,
weekday int,
hour int

# ETL Process
1. Search all the files in data/song_data directory and upload the songs data and artists data
2. Search all the json files in data/log_data directory and upload the data in the following order:

    2.1. Filter on "Next Song" only
    
    2.2. Insert into time table
    
    2.3. Insert into user table
    
    2.4. Insert into songplay table

# How to run the project
```bash
python create_tables.py
python etl.py
```
