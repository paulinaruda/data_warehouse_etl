# Sparkify Data Warehouse 

Thie project is a data warehouse by Paulina R and is a part od Data Engineering Nanodegree with Udacity. <br>
In the ever-evolving world of music streaming, Sparkify, a dynamic startup, has successfully expanded its user base and amassed a vast collection of songs. To enhance their data management capabilities and leverage the power of the cloud, Sparkify has embarked on a journey to migrate their data processes to a cloud-based data warehouse. 
By effectively harnessing the potential of this data warehouse, Sparkify will be equipped with a robust foundation for their analytics endeavors, allowing them to stay ahead of the competition and continuously optimize their services based on a deep understanding of their users' preferences and listening habits.

# Implementation

The primary objective of this project was to design and implement an ETL pipeline within AWS virtual private cloud that extracts Sparkify's data from their S3 buckets, stages it in Redshift, and transforms it into a comprehensive set of dimensional tables. These tables will empower Sparkify's analytics team to derive valuable insights and uncover patterns in user behavior, enabling them to make data-driven decisions and enhance the overall user experience.

# Data Flow Diagram
The below diagram shows how the data flows and processes
![Data_Flow_Diagram](https://github.com/paulinaruda/data_warehouse_etl/assets/84568114/22b6a7b9-30f3-4201-983b-ef9d42ba4744)

# Database schema design and ETL pipeline

The above pipeline creates a star schema optimized for queries on song play analysis. This includes the following tables.<br>
### Fact Table<br>
songplays - records in event data associated with song plays i.e. records with page NextSong<br>
_songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_<br>
### Dimension Tables
users - users in the app<br>
_user_id, first_name, last_name, gender, level_<br>
songs - songs in music database<br>
_song_id, title, artist_id, year, duration_<br>
artists - artists in music database<br>
_artist_id, name, location, latitude, longitude_<br>
time - timestamps of records in songplays broken down into specific units<br>
_start_time, hour, day, week, month, year, weekday_<br>

The two staging tables are logdata and songs. I first focused on designing the songplays table, as it is the central, fact, table in this data warehouse. It is mostly based in on the staging logdata, with an addition of the artist and song id columns from the songs table. It is then joined on columns describing artist name, song title and duration of the song, as the combination of these 3 was the best way to identify each record. In order to address the issue of multiple artists having the same title, I had to make some adjustments to the joining field.
I then designed the users table (it is impossible to call a table "user" due to Redshift restrictions). 
A combination of the user_id and level were the best option of the joining fields while joining with the fact table, as some user ids have/had multiple plans. In real life projects this should be talked through with the stakeholders and I should find out if there are any underlying processes that could infuence how this table is created. I noticed here that here is an option to create a table user_level_history, that would include historical user data of their plan (free/paid).
Same as the users table, song, artist and time tables are mostly based on single tables, and the creation is straightforward. 

# Data warehouse ERD
Using the criteria above I arrived to such and ERD
<img width="917" alt="ERD_Sparkify_database" src="https://github.com/paulinaruda/data_warehouse_etl/assets/84568114/76750fcf-f40b-40b7-b239-c54ea8f2d00b">

# Other important considerations
For the script to work correctly there is a few requirements that must be fulfilled before running it:
1. Before starting the script go to AWS IAM service and click on the "Add user" button to create a new IAM user in your AWS account. Write a name "data-warehouse-user" Select "Programmatic access" as the access type. Click Next. Choose the Attach existing policies directly tab, and select the "AdministratorAccess". Click Next. Skip adding any tags. Click Next. Review and create the user. It will show you a pair of access key ID and secret which you should write to the dwh.cfg file in the [AWS] secion. You should copy and paste them straight from the console to avoid issues with running the code. 
2. Open the file dwh.cfg and make sure that in the HOST and ARN line the value after = sign is equal to xx. If it it is not, delete the value and replace it with xx(do not add coma or any other signs, just the double x letter).
3. For the script to work you should have all the provided files in one directory.<br>
Files needed: <br>
* create_tables.py (creates fact and dimension tables for the star schema in Redshift)<br>
* etl.py (loads data from S3 into staging tables on Redshift and then processes that data into your analytics tables on Redshift)<br>
* sql_queries.py (defines SQL statements, which will be imported into the two other files above)<br>
* dwh.cfg (stores configurations variables)
