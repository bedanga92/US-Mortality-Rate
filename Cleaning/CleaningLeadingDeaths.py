import json
from pymongo import MongoClient
from pymongo import errors as error
import pandas as pd
import psycopg2
import sys

'''
The clean_Leading_Death_Data() method :
1. fetches the data from the mongo db 
2.  loads it in a pandas data frame 
3.  cleaning is carried out in the dataframe
4.  After cleaning the data is then Loaded to Postgres

'''
def clean_Leading_Death_Data():


    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)


    try:

        client = MongoClient()
        client = MongoClient("mongodb://localhost:27017/") # Mongo Client for connecting to the mongo server
        leading_deaths_Database = client['leading_deaths_database'] # Defining the database where the leading causes data is stored
        leading_death_causes_Table = leading_deaths_Database['leading_Death_causes_table'] # Defining the collection where the leading causes data is stored
        df = pd.DataFrame(list(leading_death_causes_Table.find())) # Data Frame where the data is stored
        print(df)
    except(error.PyMongoError,error.ConfigurationError,error.ConnectionFailure,error.ConfigurationError,error.DocumentTooLarge) as err:
        print(err)
        sys.exit()

    #Dropping the _id columns in the dataframe that are not required
    df = df.drop('_id',1)


    #Checking for na values
    print(df.isna().sum())

    #Checking for null values
    print(df.isnull().sum())

    lst = df.columns # Listing the columns in the data frame
    lst = list(lst)

    # Checking if the data contains Blank values
    for item in lst:
        print(item +" "+ str(len(df[df[item] == ''])))
        
    # Checking if the data contains None values    
    for item in lst:
        print(item +" "+ str(len(df[df[item] == 'None'])))
        
    #Checking the datatypes of all the columns in the DataFrame
    df.dtypes


    #Changing the data types of the dataframe
    df['Year'] = df['Year'].astype(int)
    df['State'] = df['State'].astype(str)
    df['Deaths'] = df['Deaths'].astype(int)
    df['Primary Death Cause Name'] = df['Primary Death Cause Name'].astype(str)
    df['Cause Name'] = df['Cause Name'].astype(str)
    df['Age-adjusted Death Rate'] = df['Age-adjusted Death Rate'].astype(float)

    df.dtypes

     #Loading to Postgres

    # Creating the database , if the database is already there then it is skipped
    try:
        #Defining the connection string for connecting to the postgres
        dbConnection = psycopg2.connect(
            user = "power_user",
            password = "poweruserpassword",
            host = "127.0.0.1",
            port = "5432",
            database = "postgres")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("select exists(select * from pg_database where datname='temp2')")#Checks if the databae already exists return True if already exists
        if bool(dbCursor.fetchone()[0]) == True:
            print("The database exists")
            dbCursor.close()# Closing the connection
        else:
            dbCursor.execute('CREATE DATABASE temp2;')# Query that creates the database in Postgres
            dbCursor.close()# Closing the connection
        
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
        
    finally:
        if(dbConnection): dbConnection.close()

    #Creating the table:
        
    try:
        #Defining the connection string for connecting to the postgres
        dbConnection = psycopg2.connect(
            user = "power_user",
            password = "poweruserpassword",
            host = "127.0.0.1",
            port = "5432",
            database = "temp2")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('leading_cause_death',)) #Checks if the table already exists, return True if it aready exists
        if bool(dbCursor.fetchone()[0]) == True:
            print("The table  exists")
            dbCursor.close() #Closing the database connection
        else:
            # Query that creats the table in the above created database     
            dbCursor.execute("""CREATE TABLE leading_cause_death( 
                Year integer,
                Primary_Death_Cause_Name varchar(100),
                Cause_Name varchar(1000),
                State varchar(1000),
                Deaths integer,
                Age_adjusted_Death_Rate REAL);
                """)
            dbCursor.close() #Closes the database connection
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()

    #Inserting the data to table
        
    try:
        #Database connection string
        dbConnection = psycopg2.connect(
            user = "power_user",
            password = "poweruserpassword",
            host = "127.0.0.1",
            port = "5432",
            database = "temp2")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        cols = "','".join([str(i) for i in df.columns.tolist()])
        #Loop that inserts every row from the cleaned dataframe to the table in postgres
        for i,row in df.iterrows():
            # Query that inserts the data iteratively
            sql = "INSERT INTO leading_cause_death (Year,Primary_Death_Cause_Name,Cause_Name,State,Deaths,Age_adjusted_Death_Rate) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            dbCursor.execute(sql, tuple(row))
            dbConnection.commit() #Commits the inserted row in the table in database
            print(row)
        dbCursor.close() #Database connection is closed
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()