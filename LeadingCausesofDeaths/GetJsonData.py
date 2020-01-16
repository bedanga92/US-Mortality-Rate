import requests
import json
import os

def jsonData():
    #Url link of the api containing the data
    urlDataset = "https://data.cdc.gov/api/views/bi63-dtpu/rows.json?accessType=DOWNLOAD"

    try:

        response = requests.get(urlDataset,timeout=10000)# fetching the data using the request library
        #json_data = json.load(response)
        #print(response.text)
        
        try:
            #Creating a json file and writing the response in the file and saving it
            with open(os.getcwd()+"/leadingcausesofDeath_jsondata.json", "w") as f:
                f.writelines(response.text)#writing the response output
        except(FileNotFoundError,PermissionError,OSError) as err:
            print("Unable to write into the Json file")
            print()
            print()
            print(err)
        
    except(TimeoutError,ConnectionError,ConnectionRefusedError) as e:
        print("Unable to get data from the Api")
        print(e)