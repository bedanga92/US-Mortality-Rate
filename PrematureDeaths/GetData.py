import csv
import json
import os

def transformData():
        
    data = [] #creating an empty data object
    #path of the directory where the csv file resides
    path = os.getcwd()+"/us-county-premature-mortality-rate/"
    try:
        #Reading from the csv file and converting each row in to dictionary
        with open(path+"additional_measures_cleaned.csv",newline='') as file:
            reader = csv.DictReader(file)# using the Dict reader to map each row as a dictionary
            print(reader)
            for line in reader:
                data.append(line)    #appending the dictionaries in the data object

    except(FileNotFoundError,PermissionError,OSError) as err:
        print("Unable open the csv file")
        print()
        print()
        print(err)
    print(data)
    try:
        #Creating a json file to store the converted data in json format
        with open(os.getcwd()+"/additional_measures_cleaned.json","w") as file:
            file.write(json.dumps(data,indent= 3))#writing the data to the file
    except (FileNotFoundError,PermissionError,OSError) as erro:
        print("File unable to write")
        print("Unable to write into the Json file")
        print()
        print()
        print(erro)

    dataset2 = [] # data object
    try:
        #Reading from the csv file and converting each row in to dictionary
        with open(path+"ypll.csv",newline='') as file:
            reader = csv.DictReader(file)# using the Dict reader to map each row as a dictionary
            print(reader)
            for line in reader:
                dataset2.append(line)#appending the dictionaries in the data object
    except(FileNotFoundError,PermissionError,OSError) as err:
        print("Unable open the csv file")
        print()
        print()
        print(err)
    print(dataset2)
    try:
        #Creating a json file to store the converted data in json format
        with open(os.getcwd()+"/ypll.json","w") as file:
            file.write(json.dumps(dataset2,indent= 3))#writing the data to the file
    except(FileNotFoundError,PermissionError,OSError) as erro:
        print("File unable to write")
        print("Unable to write into the Json file")
        print()
        print()
        print(erro)