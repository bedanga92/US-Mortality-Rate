import json
from pymongo import MongoClient
from pymongo import errors as error
import pandas as pd
import psycopg2
import sys
import numpy as np


'''
The clean_premature_death_Data() method :
1. fetches the data from the mongo db 
2. loads it in a pandas data frame 
3. cleaning is carried out in the dataframe
4. After cleaning the data is then Loaded to Postgres

'''
 
def clean_premature_death_Data():

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    try:

        client = MongoClient()
        client = MongoClient("mongodb://localhost:27017/")# Mongo Client for connecting to the mongo server
        premature_deaths_db = client['premature_deaths']# Defining the database where the leading causes data is stored
        premature_deaths_table = premature_deaths_db['additional_measures']# Defining the collection where the leading causes data is stored
        df = pd.DataFrame(list(premature_deaths_table.find()))# Data Frame where the data is stored
        print(df)
    except(error.PyMongoError,error.ConfigurationError,error.ConnectionFailure,error.ConfigurationError,error.DocumentTooLarge) as err:
        print(err)

    #Dropping the _id columns in the dataframe that are not required
    df = df.drop('_id',1)


    #Checking for na values
    print(df.isna().sum())

    #Checking for null values
    print(df.isnull().sum())

    lst = df.columns# Listing the columns in the data frame
    lst = list(lst)

    # Checking if the data contains Blank values
    for item in lst:
        print(item +" "+ str(len(df[df[item] == ''])))
        
    # Checking if the data contains None values    
    for item in lst:
        print(item +" "+ str(len(df[df[item] == 'None'])))
        
    #Checking the datatypes of all the columns in the DataFrame
    df.dtypes
    #filling the blank values with nan and replacing the nan values with 0
    df.replace('', np.nan, inplace=True)
    df['HIV rate'] = df['HIV rate'].fillna(0)
    df['% Free lunch'] = df['% Free lunch'].fillna(0)
    df['% child Illiteracy'] = df['% child Illiteracy'].fillna(0)
    df['Rural'] = df['Rural'].fillna(0)
    df['median household income'] = df['median household income'].fillna(0)

     #Changing the data types of the dataframe
    df['FIPS'] = df['FIPS'].astype(int)
    df['State'] = df['State'].astype(str)
    df['County'] = df['County'].astype(str)
    df['Population'] = df['Population'].astype(int)
    df['< 18'] = df['< 18'].astype(float)
    df['65 and over'] = df['65 and over'].astype(float)
    df['African American'] = df['African American'].astype(float)
    df['Female'] = df['Female'].astype(float)
    df['Rural'] = df['Rural'].astype(float)
    df['%Diabetes'] = df['%Diabetes'].astype(int)
    df['HIV rate'] = df['HIV rate'].astype(int)
    df['Physical Inactivity'] = df['Physical Inactivity'].astype(int)
    df['mental health provider rate'] = df['mental health provider rate'].astype(int)
    df['median household income'] = df['median household income'].astype(int)
    df['% high housing costs'] = df['% high housing costs'].astype(int)
    df['% Free lunch'] = df['% Free lunch'].astype(int)
    df['% child Illiteracy'] = df['% child Illiteracy'].astype(float)
    df['% Drive Alone'] =df['% Drive Alone'].astype(int)

    #Imputing mean of the column in the 0 values.
    df['HIV rate']=df['HIV rate'].mask(df['HIV rate'] == 0, df['HIV rate'].mean(skipna=True))
    df['% Free lunch']=df['% Free lunch'].mask(df['% Free lunch'] == 0, df['% Free lunch'].mean(skipna=True))
    df['% child Illiteracy']=df['% child Illiteracy'].mask(df['% child Illiteracy'] == 0, df['% child Illiteracy'].mean(skipna=True))
    df['Rural']=df['Rural'].mask(df['Rural'] == 0, df['Rural'].mean(skipna=True))
    df['median household income']=df['median household income'].mask(df['median household income'] == 0, df['median household income'].mean(skipna=True))


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
        dbCursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('premature_death',))#Checks if the table already exists, return True if it aready exists
        if bool(dbCursor.fetchone()[0]) == True:
            print("The table  exists")
            dbCursor.close()# Closing the connection
        else:
            # Query that creats the table in the above created database     
            dbCursor.execute("""CREATE TABLE premature_death(
            FIPS  INTEGER,
            State  VARCHAR(100),
            County  VARCHAR(100),
            Population  INTEGER,
            less_than_18  REAL,
            sixtyfive_and_over  REAL,
            African_American  REAL,
            Female  REAL,
            Rural  INTEGER,
            Pct_Diabetes  INTEGER,
            HIV_rate  INTEGER,
            Physical_Inactivity  INTEGER,
            mental_health_provider_rate  INTEGER,
            median_household_income  INTEGER,
            Pct_high_housing_costs  INTEGER,
            Pct_Free_lunch  INTEGER,
            Pct_child_Illiteracy  REAL,
            Pct_Drive_Alone  INTEGER
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
            sql = "INSERT INTO premature_death (FIPS,State,County,Population,less_than_18,sixtyfive_and_over,African_American,Female,Rural,Pct_Diabetes,HIV_rate,Physical_Inactivity,mental_health_provider_rate,median_household_income,Pct_high_housing_costs,Pct_Free_lunch,Pct_child_Illiteracy,Pct_Drive_Alone) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            dbCursor.execute(sql, tuple(row))
            dbConnection.commit()#Commits the inserted row in the table in database
            print(row)
        dbCursor.close()#Database connection is closed
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()
