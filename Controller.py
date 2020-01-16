import sys

'''
 Importting all the Libraries that are required for Extraction of Data, Cleaning and Insertion
 of the transformed data in Postgres
'''
try:

    import Drug_Poisoning.GetJsonData # Gets the data from the api in data.gov for drug poisoning
    import Drug_Poisoning.MongoInsert # Inserts the drug poisoning data from the json respose file to Mongo DB
    import InjuryMortality.GetJsonFile # Gets the data from the api in data.gov for Injury Mortality
    import InjuryMortality.MongoInsertData # Inserts the Injury Mortality data from the json respose file to Mongo DB
    import LeadingCausesofDeaths.GetJsonData # Gets the data from the api in data.gov for Leading Causes of Death
    import LeadingCausesofDeaths.MongoLoad # Inserts the Leading Causes of Death data from the json respose file to Mongo DB
    import PrematureDeaths.GetData # Gets the data from the api in data.gov for Premature Deaths
    import PrematureDeaths.LoadMongoDB # Inserts the Premature Deaths data from the json file to Mongo DB
    import Cleaning.CleaningDrugdb # Extract the drug data from mongo db and cleans the data and inserts into postgres
    import Cleaning.CleaningLeadingDeaths # Extract the leading causes of death data from mongo db and cleans the data and inserts into postgres
    import Cleaning.injuryDataCleaning # Extract the injury data from mongo db and cleans the data and inserts into postgres
    import Cleaning.us_additional_con # Extract the Premature Death data from mongo db and cleans the data and inserts into postgres
    import Cleaning.ypllCleaning # Extract the ypll data from mongo db and cleans the data and inserts into postgres
except ImportError as e:
    print(e)

if __name__ == "__main__":
    try:
        Drug_Poisoning.GetJsonData.getData() # Methods that makes a call to the drug poisonig API and stores the response in a json file
        Drug_Poisoning.MongoInsert.LoadData() # Methods that stores the drug poisonig data to Mongo DB

        InjuryMortality.GetJsonFile.getData() # Methods that makes a call to the injury mortality API and stores the response in a json file
        InjuryMortality.MongoInsertData.LoadMongoData() # Methods that stores the injury mortality data to Mongo DB

        LeadingCausesofDeaths.GetJsonData.jsonData() # Methods that makes a call to the Leading Causes of death  API and stores the response in a json file
        LeadingCausesofDeaths.MongoLoad.LoadMongoData() # Methods that stores the injury mortality data to Mongo DB

        PrematureDeaths.GetData.transformData() # Methods that loads the Premature Death Data csv and converts to a json file
        PrematureDeaths.LoadMongoDB.LoadDataMongoDB() # Methods that stores the Premature Death Data from json file to Mongo Db

        #Cleaning Data and Loading to Postgres
        Cleaning.CleaningDrugdb.cleanDrugData() # Method that Cleans the Drug Poisoning data and commits the clean data to Postgres
        Cleaning.CleaningLeadingDeaths.clean_Leading_Death_Data() # Method that Cleans the Leading Causes of Death data and commits the clean data to Postgres
        Cleaning.injuryDataCleaning.clean_Injury_Data() # Method that Cleans the Injury Mortality data and commits the clean data to Postgres
        Cleaning.us_additional_con.clean_premature_death_Data() # Method that Cleans the premature death Data data and commits the clean data to Postgres
        Cleaning.ypllCleaning.cleanYpllData()# Method that Cleans the premature death Data data and commits the clean data to Postgres
    except (FileNotFoundError,ModuleNotFoundError) as e:
        print(e)
        print(">>>>>> Terminating <<<<<<")
        sys.exit()
        
