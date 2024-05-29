import pandas as pd
from pymongo import MongoClient
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection string
MONGO_URI = ""

# Database and collection names
DATABASE_NAME = 'BDA_DB'
TARGET_COLLECTION = 'BDA_collection_filtered'


def save_csv_to_mongodb(csv_file_path):
    try:
        # Reading the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        # Converting DataFrame to list of dictionaries
        data = df.to_dict(orient='records')

        # Connecting to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[TARGET_COLLECTION]

        # Inserting data into the collection
        result = collection.insert_many(data)
        logging.info(f"Inserted document IDs: {result.inserted_ids}")

        # Closing the connection
        client.close()
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == '__main__':
    # Path to the filtered CSV file
    csv_file_path = 'data_filtered.csv'

    # Saving the data to MongoDB
    save_csv_to_mongodb(csv_file_path)
