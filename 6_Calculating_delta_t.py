import logging
from pymongo import MongoClient, errors
from datetime import datetime
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection string
MONGO_URI = ""

# Database and collection names
DATABASE_NAME = 'BDA_DB'
TARGET_COLLECTION = 'BDA_collection_filtered'

def get_mongo_client(uri):
    return MongoClient(uri)

def fetch_data_from_mongodb():
    try:
        # Connecting to MongoDB
        client = get_mongo_client(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[TARGET_COLLECTION]

        # Fetching all data from the collection
        data = list(collection.find())

        # Closing the connection
        client.close()
        return data
    except errors.ConnectionFailure as e:
        logging.error(f"Connection error: {e}")
    except errors.ServerSelectionTimeoutError as e:
        logging.error(f"Server selection timeout error: {e}")
    except errors.PyMongoError as e:
        logging.error(f"PyMongo error: {e}")
        return []


def calculate_delta_t(data):

    delta_t_values = []

    # Ensuring all necessary fields ('MMSI' and '# Timestamp') exist before sorting and processing
    filtered_data = [record for record in data if 'MMSI' in record and '# Timestamp' in record]

    # Sorting the data by 'MMSI' and '# Timestamp' to ensure chronological order for each vessel
    filtered_data.sort(key=lambda x: (x['MMSI'], x['# Timestamp']))

    prev_mmsi = None
    prev_timestamp = None

    # Iterating through the filtered and sorted data
    for record in filtered_data:
        mmsi = record['MMSI']
        try:
            # Parsing the timestamp string into a datetime object
            timestamp = datetime.strptime(record['# Timestamp'].strip(), '%d/%m/%Y %H:%M:%S')
        except ValueError as e:
            # Log a warning and skip the record if the timestamp format is invalid
            logging.warning(f"Skipping record with invalid timestamp format: {record['# Timestamp']}")
            continue

        # Calculating delta t if the previous MMSI and timestamp are set and match the current MMSI
        if prev_mmsi == mmsi and prev_timestamp:
            # Calculating the time difference in milliseconds
            delta_t = (timestamp - prev_timestamp).total_seconds() * 1000
            delta_t_values.append(delta_t)

        # Updating the previous MMSI and timestamp for the next iteration
        prev_mmsi = mmsi
        prev_timestamp = timestamp

    return delta_t_values

def save_delta_t_values(delta_t_values):
    df = pd.DataFrame(delta_t_values, columns=['delta_t'])
    df.to_csv('delta_time.csv', index=False)
    logging.info("Delta t values saved to delta_time.csv")


def main():
    # Step 1: Fetching the filtered data from MongoDB
    data = fetch_data_from_mongodb()
    if not data:
        logging.error("No data retrieved from MongoDB.")
        return

    # Step 2: Calculating delta t values
    delta_t_values = calculate_delta_t(data)

    # Step 3: Saving delta t values to a CSV file
    save_delta_t_values(delta_t_values)


if __name__ == '__main__':
    main()
