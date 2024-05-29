import pandas as pd
from pymongo import MongoClient
import concurrent.futures
import os

# MongoDB connection string
MONGO_URI = ""

# Database and collection names
DATABASE_NAME = 'BDA_DB'
SOURCE_COLLECTION = 'BDA_collection'

# Connecting to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

# Function to filter noise from a chunk of data
def filter_noise(chunk):
    filtered_chunk = []
    for record in chunk:
        if (
            pd.notnull(record.get('Navigational status')) and
            pd.notnull(record.get('MMSI')) and
            pd.notnull(record.get('Latitude')) and
            pd.notnull(record.get('Longitude')) and
            pd.notnull(record.get('ROT')) and
            pd.notnull(record.get('SOG')) and
            pd.notnull(record.get('COG')) and
            pd.notnull(record.get('Heading'))
        ):
            filtered_chunk.append(record)
    return filtered_chunk

# Function to process data for a single vessel
def process_vessel_data(mmsi):
    vessel_data = list(db[SOURCE_COLLECTION].find({'MMSI': mmsi}))
    if len(vessel_data) >= 100:
        return filter_noise(vessel_data)
    return []

# Function to retrieve and filter data in parallel
def main():
    # Retrieving all distinct MMSI values
    mmsi_list = db[SOURCE_COLLECTION].distinct('MMSI')

    filtered_data = []

    # Processing each vessel's data in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for mmsi in mmsi_list:
            futures.append(executor.submit(process_vessel_data, mmsi))

        for future in concurrent.futures.as_completed(futures):
            filtered_data.extend(future.result())

    # Saving filtered data to CSV file
    if filtered_data:
        df_filtered = pd.DataFrame(filtered_data)
        df_filtered.to_csv('data_filtered.csv', index=False)

    print("Data filtering completed and results saved to data_filtered.csv")

if __name__ == '__main__':
    main()
