import pandas as pd
from pymongo import MongoClient
import concurrent.futures

# MongoDB connection string
MONGO_URI = ""

# Database and collection names
DATABASE_NAME = 'BDA_DB'
COLLECTION_NAME = 'BDA_collection'

# Function to insert a chunk of data into MongoDB
def insert_data(chunk):
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    collection.insert_many(chunk)
    client.close()

# Function to read CSV and insert data in parallel
def main():
    # Reading data from CSV file
    df = pd.read_csv('data.csv')

    # Converting DataFrame to list of dictionaries
    data = df.to_dict(orient='records')

    # Chunk size for parallel insertion
    num_threads = 8
    chunk_size = len(data) // num_threads

    # Creating chunks of data
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Using ThreadPoolExecutor to insert data in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(insert_data, chunks)

    print("Data insertion completed.")

if __name__ == '__main__':
    main()
