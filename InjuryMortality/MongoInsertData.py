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
        with open(os.getcwd()+"/injury_jsondata.json","r") as f:
            line = f.readline()#reading the response output from the file
            while line:
                
                data += line # appending the response output iteratively to the data object
                line = f.readline()
    except FileNotFoundError as e:
        print("Unable to load file")
        print(e)

    data = json.loads(data) # transforms the data object to json object
    #print(data)

    length = len(data["data"]) # number of rows in the data set

    print(">>> Connecting with Mongo DB <<<")
    client = MongoClient()
    client = MongoClient("mongodb://localhost:27017/")# Mongo Client for connecting to the mongo server
    injuryDatabase = client['injury_database'] # Defining the database where the drug data is stored
    print(">>> Creating a Collection in Mongo Db <<<")
    injuryTable = injuryDatabase['injury_table'] # Defining the collection where the drug data is stored
    print(">>> Inserting data into Mongo Db <<<")
    try:
        #for loop used to insert the data iteratively in Mongo Db
        for i in range(length):
            #document object where the data is mapped with there respective values of a row
            post = {"Year" : data["data"][i][8],
            "Sex" : data["data"][i][9],
            "Age_group" : data["data"][i][10],
            "Race" : data["data"][i][11],
            "Injury_mechanism" : data["data"][i][12],
            "Injury intent" : data["data"][i][13],
            "Deaths" : data["data"][i][14],
            "Population" : data["data"][i][15],
            "Age_Specific_Rate" : data["data"][i][16],
            "Age_Specific_Rate_Standard_Error" : data["data"][i][17],
            "Age_Specific_Rate_Lower_Confidence_Limit" : data["data"][i][18],
            "Age_Specific_Rate_Upper_Confidence_Limit" : data["data"][i][19],
            "Age_Adjusted_Rate" : data["data"][i][20],
            "Age_Adjusted_Rate_Standard_Error" : data["data"][i][21],
            "Age_Adjusted_Rate_Lower_Confidence_Limit" : data["data"][i][22],
            "Age_Adjusted_Rate_Upper_Confidence_Limit" : data["data"][i][23],
            "Unit" : data["data"][i][24]}
            injuryTable.insert_one(post)#document object is inserted to mongo db
            print("Year : "+str(data["data"][i][8])+" "+"Sex : "+str(data["data"][i][9])+" "+"Age_group : "+str(data["data"][i][10])+" "+"Race : "+str(data["data"][i][11])+" "+"Injury_mechanism : "+str(data["data"][i][12])+" "+"Injury_intent : "+str(data["data"][i][13])+" "+"Deaths : "+str(data["data"][i][14])+" "+"Population : "+str(data["data"][i][15])+" "+"Age_Specific_Rate : "+str(data["data"][i][16])+" "+"Age_Specific_Rate_Standard_Error :"+str(data["data"][i][17])+" "+"Age_Specific_Rate_Lower_Confidence_Limit : "+str(data["data"][i][18])+" "+"Age_Specific_Rate_Upper_Confidence_Limit : "+str(data["data"][i][19])+" "+"Age_Adjusted_Rate : "+str(data["data"][i][20])+" "+"Age_Adjusted_Rate_Standard_Error : "+str(data["data"][i][21])+" "+"Age_Adjusted Rate_Lower_Confidence_Limit : "+str(data["data"][i][22])+" "+"Age_Adjusted_Rate_Upper_Confidence_Limit : "+str(data["data"][i][23])+" "+"Unit : "+str(data["data"][i][24]))

    except (error.PyMongoError,error.ConfigurationError,error.ConnectionFailure,error.ConfigurationError,error.DocumentTooLarge) as err:
        print(err)
        sys.exit()