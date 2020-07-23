# Project Objectives :
The Main Objective of this project is to process Server Log Data of Million Song Dataset, and ingest the processed data into Postgresql Database

# Project Phases :

##### 1 -  Create DDL for tables Creation and DDL for drop Tables in sql_queries.py . 
##### 2 -  Develop DML for propagating the tables with data in sql_queries.py . 
##### 3 -  Develop Python script to Create Database and Database tables in create_tables.py .
##### 4 -  Develop Python script to read song's data and log's data from josn files and load it into tables in etl.py

# ERD for Dimension Model :

<img src="./ERD.jpg?raw=false" width="600" />

# Processing Steps:
 - Create Connection to Database .
 - Start Reading the Files exist in Log Files Directory, Count them and prints to the console how many files found for processing,
 - Iterate through log files, and for each of the files extract the required data and process it whether this is an initial, an incremental Run or old File the code will handle it.

    * In case the data has been processed and ingested into the database, a message shows the name of the processed file, the number of processed Records in this file & Total Number of Processed Files, then Moves this Processed file to Archive directory to avoid reprocessing it again in the next execution of the code

    * In Case of Failure Prints a Warning Message on the console to show which file faced problems and leaves it in the same location
    
 - Load the data from staging Dataframe into the different dataframes which represent the actual Database tables to do some checks for the data before loading it into database tables (Like remove duplication from Dimensions tables).
 - Loading data into fact table .
 
 # Program Execution 
 
 - Run create_tables.py in order to Create Databasa
 - Run etl.py in order to propagate tables with data
