from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os 
from dotenv import load_dotenv
import argparse

load_dotenv("../.env")
uri = os.environ["MONGO_URI"]

def get_db():
    db_client = MongoClient(uri, server_api=ServerApi('1'))
    db = db_client.get_database()

# show the number of stocks that had scrapes > 0

def main():
    db = get_db()

    parser = argparse.ArgumentParser(description="A simple Python CLI tool")
    parser.add_argument('--run-id', type=str, required=True, help="The run ID to process")

    args = parser.parse_args()
    run_id = args.run_id
    print(f"Received run ID: {run_id}")

main()
