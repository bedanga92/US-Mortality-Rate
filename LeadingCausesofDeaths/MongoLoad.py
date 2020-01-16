import json
from pymongo import MongoClient
from pymongo import errors as error
import os
import sys
'''
The LoadMongoData() method :
1. fetches the data from the json file
2.  loads it to mongo Db

'''
def LoadMongoData():

    try:
        #Fetcing the data from the json file
        data = ""
        with open(os.getcwd()+"/leadingcausesofDeath_jsondata.json","r") as f:
            line = f.readline()#reading the response output fron the file
            while line:
                
                data += line# appending the response output iteratively to the data object
                line = f.readline()
    except FileNotFoundError as e:
        print("Unable to load file")
        print(e)
    data = json.loads(data)# transforms the data object to json object
    #print(data)

    length = len(data["data"])# number of rows in the data set

    print(">>> Connecting with Mongo DB <<<")
    client = MongoClient()
    client = MongoClient("mongodb://localhost:27017/")# Mongo Client for connecting to the mongo server
    leading_deaths_Database = client['leading_deaths_database']# Defining the database where the drug data is stored
    print(">>> Creating a Collection in Mongo Db <<<")
    leading_death_causes_Table = leading_deaths_Database['leading_Death_causes_table']# Defining the collection where the drug data is stored
    print(">>> Inserting data into Mongo Db <<<")

    try:
        #for loop used to insert the data iteratively in Mongo Db
        for i in range(length):
            #document object where the data is mapped with there respective values of a row
            post = {"Year" : data["data"][i][8],
            "Primary Death Cause Name" : data["data"][i][9],
            "Cause Name" : data["data"][i][10],
            "State" : data["data"][i][11],
            "Deaths" : data["data"][i][12],
            "Age-adjusted Death Rate" : data["data"][i][13],
            
            }
            leading_death_causes_Table.insert_one(post)#document object is inserted to mongo db
            print("Year : "+str(data["data"][i][8])+" "+"Primary Death Cause Name : "+str(data["data"][i][9])+" "+"Cause Name : "+str(data["data"][i][10])+" "+"State : "+str(data["data"][i][11])+" "+"Deaths : "+str(data["data"][i][12])+" "+"Age-adjusted Death Rate : "+str(data["data"][i][13]))

    except (error.PyMongoError,error.ConfigurationError,error.ConnectionFailure,error.ConfigurationError,error.DocumentTooLarge) as err:
        print(err)
        sys.exit()
