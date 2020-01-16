import json
from pymongo import MongoClient
from pymongo import errors as error
import pandas as pd
import psycopg2
import sys
'''
The cleanDrugData() method :
1. fetches the data from the mongo db 
2.  loads it in a pandas data frame 
3.  cleaning is carried out in the dataframe
4.  After cleaning the data is then Loaded to Postgres

'''
def cleanDrugData():

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    try:

        client = MongoClient()
        client = MongoClient("mongodb://localhost:27017/") # Mongo Client for connecting to the mongo server
        drug_poisoningDatabase = client['drug_poisoning_database'] # Defining the database where the drug data is stored
        drugTable = drug_poisoningDatabase['drug_poisoning_table'] # Defining the collection where the drug data is stored
        df = pd.DataFrame(list(drugTable.find())) # Data Frame where the data is stored
        print(df)
    except (error.PyMongoError,error.ConfigurationError,error.ConnectionFailure,error.ConfigurationError,error.DocumentTooLarge) as err:
        print(err)
        sys.exit()

    #Dropping the _id columns in the dataframe that are not required
    df = df.drop('_id',1)
    df = df.drop('State Crude Rate in Range',1)

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


    #Changing the datatypes of all the columns in the DataFrame

    df['State']=df['State'].astype(str)
    df['Year']=df['Year'].astype(int)
    df['Sex']=df['Sex'].astype(str)
    df['Age Group']=df['Age Group'].astype(str)
    df['Race and Hispanic Origin']=df['Race and Hispanic Origin'].astype(str)
    df['Deaths']=df['Deaths'].astype(int)
    df['Population']=df['Population'].astype(int)
    df['Crude Death Rate']=df['Crude Death Rate'].astype(float)
    df['Standard Error for Crude Rate']=df['Standard Error for Crude Rate'].astype(float)
    df['Lower Confidence Limit for Crude Rate']=df['Lower Confidence Limit for Crude Rate'].astype(float)
    df['Upper Confidence Limit for Crude Rate']=df['Upper Confidence Limit for Crude Rate'].astype(float)
    df['Age-adjusted Rate']=df['Age-adjusted Rate'].astype(float)
    df['Standard Error for Age-adjusted Rate']=df['Standard Error for Age-adjusted Rate'].astype(float)
    df['Lower Confidence Limit for Age-adjusted Rate']=df['Lower Confidence Limit for Age-adjusted Rate'].astype(float)
    df['Upper Confidence Limit for Age-adjusted Rate']=df['Upper Confidence Limit for Age-adjusted Rate'].astype(float)
    df['US Crude Rate']=df['US Crude Rate'].astype(float)
    df['US Age-adjusted Rate']=df['US Age-adjusted Rate'].astype(float)
    df['Unit']=df['Unit'].astype(str)

    df.dtypes

    '''
    Imputing the mean of the NaN values in the columns:
    Age-adjusted Rate
    Standard Error for Age-adjusted Rate
    Lower Confidence Limit for Age-adjusted Rate
    Upper Confidence Limit for Age-adjusted Rate
    '''

    df['Age-adjusted Rate'] = df['Age-adjusted Rate'].fillna((df['Age-adjusted Rate'].mean()))
    df['Standard Error for Age-adjusted Rate'] = df['Standard Error for Age-adjusted Rate'].fillna((df['Standard Error for Age-adjusted Rate'].mean()))
    df['Lower Confidence Limit for Age-adjusted Rate'] = df['Lower Confidence Limit for Age-adjusted Rate'].fillna((df['Lower Confidence Limit for Age-adjusted Rate'].mean()))
    df['Upper Confidence Limit for Age-adjusted Rate'] = df['Upper Confidence Limit for Age-adjusted Rate'].fillna((df['Upper Confidence Limit for Age-adjusted Rate'].mean()))


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
        dbCursor.execute("select exists(select * from pg_database where datname='temp2')") #Checks if the databae already exists return True if already exists
        if bool(dbCursor.fetchone()[0]) == True:
            print("The database exists")
            dbCursor.close()# Closing the connection
        else:
            dbCursor.execute('CREATE DATABASE temp2;') # Query that creates the database in Postgres
            dbCursor.close()# Closing the connection
        
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
        
    finally:
        if(dbConnection): dbConnection.close()

    #Creating Table
        
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
        dbCursor.execute("select exists(select * from information_schema.tables where table_name=%s)", ('drug_poisoning',)) #Checks if the table already exists, return True if it aready exists
        if bool(dbCursor.fetchone()[0]) == True:
            print("The table  exists")
            dbCursor.close() # Closing the database connection
        else:
            # Query that creats the table in the above created database     
            dbCursor.execute("""CREATE TABLE drug_poisoning(State varchar(1000), 
                Year integer,
                Sex varchar(100),
                Age_Group varchar(1000),
                Race_and_Hispanic_Origin varchar(1000),
                Deaths integer,
                Population integer,
                Crude_Death_Rate REAL,
                Standard_Error_for_Crude_Rate REAL,
                Lower_Confidence_Limit_for_Crude_Rate REAL,
                Upper_Confidence_Limit_for_Crude_Rate REAL,
                Age_adjusted_Rate REAL,
                Standard_Error_for_Age_adjusted_Rate REAL,
                Lower_Confidence_Limit_for_Age_adjusted_Rate REAL,
                Upper_Confidence_Limit_for_Age_adjusted_Rate REAL,
                US_Crude_Rate REAL,
                US_Age_adjusted_Rate REAL,
                Unit varchar(1000)
                );
                """)
        
            dbCursor.close() #Closes the database connection
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()

    #Inserting in to table :

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
            sql = "INSERT INTO drug_poisoning (state,year,sex,Age_Group,Race_and_Hispanic_Origin,Deaths,Population,Crude_Death_Rate,Standard_Error_for_Crude_Rate,Lower_Confidence_Limit_for_Crude_Rate,Upper_Confidence_Limit_for_Crude_Rate,Age_adjusted_Rate,Standard_Error_for_Age_adjusted_Rate,Lower_Confidence_Limit_for_Age_adjusted_Rate,Upper_Confidence_Limit_for_Age_adjusted_Rate,US_Crude_Rate,US_Age_adjusted_Rate,Unit) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            dbCursor.execute(sql, tuple(row))
            dbConnection.commit() #Commits the inserted row in the table in database
            print(row)
        dbCursor.close() #Database connection is closed

    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()