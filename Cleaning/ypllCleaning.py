import json
from pymongo import MongoClient
from pymongo import errors as error
import pandas as pd
import psycopg2
import sys
import numpy as np

'''
The cleanYpllData() method :
1. fetches the data from the mongo db 
2. loads it in a pandas data frame 
3. cleaning is carried out in the dataframe
4. After cleaning the data is then Loaded to Postgres

'''
def cleanYpllData():

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    try:

        client = MongoClient()
        client = MongoClient("mongodb://localhost:27017/")# Mongo Client for connecting to the mongo server
        premature_deaths_db = client['premature_deaths']# Defining the database where the leading causes data is stored
        ypll_table = premature_deaths_db['ypll']# Defining the collection where the leading causes data is stored
        df = pd.DataFrame(list(ypll_table.find()))# Data Frame where the data is stored
        print(df)
    except(error.PyMongoError,error.ConfigurationError,error.ConnectionFailure,error.ConfigurationError,error.DocumentTooLarge) as err:
        print(err)
        sys.exit()

    #Dropping the _id and Unreliable columns in the dataframe that are not required
    df = df.drop('_id',1)
    df = df.drop('Unreliable',1)

    #Checking for na values
    print(df.isna().sum())

    #Checking for null values
    print(df.isnull().sum())

    lst = df.columns# Listing the columns in the data frame
    lst = list(lst)


    # Checking if the data contains Blank values
    for item in lst:
        print(item +" "+ str(len(df[df[item] == ''])))
    df.drop(df.loc[df['YPLL Rate']==''].index, inplace=True)    
    # Checking if the data contains None values    
    for item in lst:
        print(item +" "+ str(len(df[df[item] == 'None'])))

    #filling the blank values with nan and replacing the nan values with 0
        
    df['YPLL Rate'] = df['YPLL Rate'].fillna(0)
    df['YPLL Rate'] = pd.to_numeric(df['YPLL Rate'])
    df['YPLL Rate']=df['YPLL Rate'].mask(df['YPLL Rate'] == 0,  df['YPLL Rate'].mean(skipna=True))
    #Changing the data types of the dataframe
    df['FIPS'] = df['FIPS'].astype(int)
    df['State'] = df['State'].astype(str)
    df['County'] = df['County'].astype(str)
    df['YPLL Rate'] = df['YPLL Rate'].astype(float)

    #Creating the database
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
            dbCursor.execute('CREATE DATABASE temp2;')
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
        dbCursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('premature_ypll',))#Checks if the table already exists, return True if it aready exists
        if bool(dbCursor.fetchone()[0]) == True:
            print("The table  exists")
            dbCursor.close()# Closing the connection
        else:
            # Query that creats the table in the above created database
            dbCursor.execute("""CREATE TABLE premature_ypll(
                    FIPS  INTEGER,
                    State  VARCHAR(100),
                    County  VARCHAR(100),
                    YPLL_Rate  REAL
                    );
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
        
        for i,row in df.iterrows():
            #Loop that inserts every row from the cleaned dataframe to the table in postgres
            sql = "INSERT INTO premature_ypll (FIPS,State,County,YPLL_Rate) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            dbCursor.execute(sql, tuple(row))
            dbConnection.commit()#Commits the inserted row in the table in database
            print(row)
        dbCursor.close()#Database connection is closed
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()
