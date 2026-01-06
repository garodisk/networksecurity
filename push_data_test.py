from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()
db_password = os.getenv("DB_PASSWORD")

# URL-encode username and password
username = quote_plus("saketgarodia1_db_user")
password = quote_plus(db_password)

uri = f"mongodb+srv://{username}:{password}@cluster0.fseyaat.mongodb.net/?appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)