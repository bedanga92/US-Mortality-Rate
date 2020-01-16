import json
from pymongo import MongoClient
from pymongo import errors as error
import os
import sys

'''
The LoadDataMongoDB() method :
1. fetches the data from the json file
2.  loads it to mongo Db

'''
def LoadDataMongoDB():
        
    try:
        #Fetcing the data from the json file
        data ="" #creating empty data object
        with open(os.getcwd()+"/additional_measures_cleaned.json") as f:
            line = f.readline()#reading the json output from the file
            while line:
                data += line # appending the response output iteratively to the data object
                line = f.readline()
    except FileNotFoundError as e:
        print("Unable to load file")
        print(e)


    data = json.loads(data) # transforms the data object to json object

    lengthOfData = len(data) # number of rows in the data set
    #### Mongo connection
    '''
    for i in range(lengthOfData):
        print(data[i])
    '''

    try:
        print(">>> Connecting with Mongo DB <<<")
        client = MongoClient()
        client = MongoClient("mongodb://localhost:27017/") # Mongo Client for connecting to the mongo server
        premature_deaths_db = client['premature_deaths'] # Defining the database where the drug data will be stored
        print(">>> Creating a Collection in Mongo Db <<<")
        premature_deaths_table = premature_deaths_db['additional_measures'] # Defining the collection where the drug data will be stored
        print(">>> Inserting data into Mongo Db <<<")
        #for loop to insert the dictionary objects in the mongodb
        for i in range (lengthOfData):
            premature_deaths_table.insert_one(data[i]) # inserting the dictionary objects
            print(data[i])
        

    except(error.PyMongoError,error.ConfigurationError,error.ConnectionFailure,error.ConfigurationError,error.DocumentTooLarge) as err:
        print(err)
        sys.exit()
    try:
        #Fetcing the data from the json file
        dataset2 =""#creating empty data object
        with open(os.getcwd()+"/ypll.json") as f:
            line = f.readline()#reading the json output from the file
            while line:
                dataset2 += line # appending the response output iteratively to the data object
                line = f.readline()
    except FileNotFoundError as e:
        print("Unable to load file")
        print(e)


    dataset2 = json.loads(dataset2) # transforms the data object to json object

    lengthOfData = len(dataset2) # number of rows in the data set

    try:
        print(">>> Connecting with Mongo DB <<<")
        client = MongoClient()
        client = MongoClient("mongodb://localhost:27017/") # Mongo Client for connecting to the mongo server
        premature_deaths_db = client['premature_deaths'] # Defining the database where the drug data will be stored
        print(">>> Creating a Collection in Mongo Db <<<")
        ypll_table = premature_deaths_db['ypll'] # Defining the collection where the drug data will be stored
        print(">>> Inserting data into Mongo Db <<<")
        #for loop to insert the dictionary objects in the mongodb
        for i in range (lengthOfData):
            ypll_table.insert_one(dataset2[i])# inserting the dictionary objects
            print(dataset2[i])
        

    except(error.PyMongoError,error.ConfigurationError,error.ConnectionFailure,error.ConfigurationError,error.DocumentTooLarge) as err:
        print(err)
        sys.exit()