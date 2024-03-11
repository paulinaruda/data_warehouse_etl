
Thie project is a data warehouse by Paulina R and is a part od Data Engineering Nanodegree with Udacity. 

# Purpose of this database 
The data warehouse is desigend to be used by data scientists on their further projects in a fictional company Sparkify.
The script will create a redshift databased and copy data from s3 buckets to the staging area and then based on staging, it will create production tables for further analysies. After growing the user base, 
Sparkify is a streaming startup and has grown their user base, and as a result they need to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
This script creates the database in a virtual private cloud using Redshift and s3 AWS services. The data is copied from s3 buckets, and then ETL processes transform the data into dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

# Database schema design and ETL pipeline
The two staging tables are logdata and songs and to arrive to the star schema I focused on designing the songplays table first, as it is the central, fact, table in this data warehouse. I mostly based in on the logdata, with an addition of the artist and song id columns from the songs table. I joined these two on columns describing artist name, song title and duration of the song, because the combination of these 3 was the best way to identify each record. Multiple aritsts having the same title stood against using just song title as a joining field, so I had to readjust.
I then designed the users table (it is impossible to call a table "user" due to Redshift restrictions). 
A combination of the user_id and level seem to be the best option of the joining fields while joining with the fact table, as some user ids have/had multiple plans. In real life this should be talked through more in depth with the stakeholders and I should find out if there are any underlying processes that could infuence how this table is created. I notice here that here is an option to create a table user_level_history, that would include historical user data of their plan (free/paid).
Same as the users table, song, artist and time tables are mostly based on single tables, and the creation is straightforward. 

# Other important considerations
For the script to work correctly there is a few requirements that must be fulfilled before running it:
1. Before starting the script go to AWS IAM service and click on the "Add user" button to create a new IAM user in your AWS account. Write a name "data-warehouse-user" Select "Programmatic access" as the access type. Click Next. Choose the Attach existing policies directly tab, and select the "AdministratorAccess". Click Next. Skip adding any tags. Click Next. Review and create the user. It will show you a pair of access key ID and secret which you should write to the dwh.cfg file in the [AWS] secion. You should copy and paste them straight from the console to avoid issues with running the code. 
2. Open the file dwh.cfg and make sure that in the HOST and ARN line the value after = sign is equal to xx. If it it is not, delete the value and replace it with xx(do not add coma or any other signs, just the double x letter).
3. For the script to work you should have all the provided files in one directory. Files needed: dwh.cfg, create_tables.py, sql_queries.py, etl.py.