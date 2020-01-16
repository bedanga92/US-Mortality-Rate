import json
from pymongo import MongoClient
from pymongo import errors as error
import pandas as pd
import psycopg2
import sys

'''
The clean_Injury_Data() method :
1. fetches the data from the mongo db 
2. loads it in a pandas data frame 
3. cleaning is carried out in the dataframe
4. After cleaning the data is then Loaded to Postgres

'''
def clean_Injury_Data():

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)


    try:

        client = MongoClient()
        client = MongoClient("mongodb://localhost:27017/") # Mongo Client for connecting to the mongo server
        injury_Database = client['injury_database']# Defining the database where the leading causes data is stored
        injury_Table = injury_Database['injury_table']# Defining the collection where the leading causes data is stored
        df = pd.DataFrame(list(injury_Table.find()))# Data Frame where the data is stored
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

    #Dropping the null columns    

    df = df.drop('Age_Adjusted_Rate',1)
    df = df.drop('Age_Adjusted_Rate_Standard_Error',1)
    df = df.drop('Age_Adjusted_Rate_Lower_Confidence_Limit',1)
    df = df.drop('Age_Adjusted_Rate_Upper_Confidence_Limit',1)
    df = df.drop('Age_Specific_Rate_Standard_Error',1)

    df.dtypes
    #Changing the data types of the dataframe
    df['Year'] = df['Year'].astype(int)
    df['Sex'] = df['Sex'].astype(str)
    df['Race'] = df['Race'].astype(str)
    df['Injury_mechanism'] = df['Injury_mechanism'].astype(str)
    df['Injury intent'] = df['Injury intent'].astype(str)
    df['Deaths'] = df['Deaths'].astype(int)
    df['Population'] = df['Population'].astype(int)
    df['Age_Specific_Rate'] = df['Age_Specific_Rate'].astype(float)
    df['Age_Specific_Rate_Lower_Confidence_Limit'] = df['Age_Specific_Rate_Lower_Confidence_Limit'].astype(float)
    df['Age_Specific_Rate_Upper_Confidence_Limit'] = df['Age_Specific_Rate_Upper_Confidence_Limit'].astype(float)
    df['Unit'] = df['Unit'].astype(str)

    #Creating the database
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
        dbCursor.execute("select exists(select * from pg_database where datname='temp2')") #Checks if the databae already exists return True if already exists
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
        dbCursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('injury_table',))#Checks if the table already exists, return True if it aready exists
        if bool(dbCursor.fetchone()[0]) == True:
            print("The table  exists")
            dbCursor.close()# Closing the connection
        else:
            # Query that creats the table in the above created database     
            dbCursor.execute("""CREATE TABLE injury_table( 
            Year numeric,
            Sex varchar(255),
            Age_group varchar(255),
            Race varchar(255),
            Injury_mechanism varchar(255),
            Injury_intent varchar(255),
            Deaths integer,
            Population integer,
            Age_Specific_Rate REAL,
            Age_Specific_Rate_Lower_Confidence_Limit REAL,
            Age_Specific_Rate_Upper_Confidence_Limit REAL,
            Unit varchar(1000));
                """)
            dbCursor.close()# Closing the connection
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
            sql = "INSERT INTO injury_table (Year,Sex,Age_group,Race,Injury_mechanism,Injury_intent,Deaths,Population,Age_Specific_Rate,Age_Specific_Rate_Lower_Confidence_Limit,Age_Specific_Rate_Upper_Confidence_Limit,Unit) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            dbCursor.execute(sql, tuple(row))
            dbConnection.commit()#Commits the inserted row in the table in database
            print(row)
        dbCursor.close()#Database connection is closed
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()