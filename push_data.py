import os
import sys
import json
from urllib.parse import quote_plus
import certifi
import pandas as pd
import numpy as np
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

from dotenv import load_dotenv
import pymongo
load_dotenv()

db_password = os.getenv("DB_PASSWORD")

# URL-encode username and password
username = quote_plus("saketgarodia1_db_user")
password = quote_plus(db_password)

MONGO_DB_URL = f"mongodb+srv://{username}:{password}@cluster0.fseyaat.mongodb.net/?appName=Cluster0"

print(MONGO_DB_URL)
ca = certifi.where()

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    
    def csv_to_json_convertor(self, file_path: str):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            logging.info(f"Data converted to JSON successfully")
            return records
            
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records: list[dict], database: str, collection: str):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            self.mongo_client = pymongo.MongoClient(
                MONGO_DB_URL,
                tls=True,
                tlsCAFile=ca
            )
            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            logging.info(f"Data inserted into MongoDB successfully")
            return(len(self.records))
        except Exception as e:
            raise NetworkSecurityException(e, sys)

if __name__ == "__main__":
    FILE_PATH = "Network_Data/phisingData.csv"
    DATABASE = "SaketGarodia"
    COLLECTION = "NetworkData"
    networkobj = NetworkDataExtract()
    records = networkobj.csv_to_json_convertor(FILE_PATH)
    print(f"Records: {records}")
    no_of_records = networkobj.insert_data_mongodb(records, DATABASE, COLLECTION)
    print(f"Data inserted into MongoDB successfully: {no_of_records} records")