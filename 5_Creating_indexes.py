from pymongo import MongoClient, errors
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection string
MONGO_URI = ""

# Database and collection names
DATABASE_NAME = 'BDA_DB'
TARGET_COLLECTION = 'BDA_collection_filtered'


def get_mongo_client(uri):
    return MongoClient(uri)


def create_indexes(db, collection_name):
    collection = db[collection_name]
    indexes = [
        ("MMSI", 1),  # Single field index on MMSI for identifying vessels
        ("MMSI", 1), ("Timestamp", 1)  # Compound index on MMSI and Timestamp for efficient time-based queries
    ]
    for index in indexes:
        if isinstance(index, tuple):
            collection.create_index([index])
            logging.info(f"Index created on {index[0]}")
        else:
            collection.create_index(index)
            logging.info(f"Compound index created on {index}")


def main():
    try:
        # Connecting to MongoDB
        client = get_mongo_client(MONGO_URI)
        db = client[DATABASE_NAME]

        # Creating indexes for efficient filtering
        create_indexes(db, TARGET_COLLECTION)

        # Closing the connection
        client.close()
    except errors.ConnectionError as e:
        logging.error(f"Connection error: {e}")
    except errors.ServerSelectionTimeoutError as e:
        logging.error(f"Server selection timeout error: {e}")
    except errors.PyMongoError as e:
        logging.error(f"PyMongo error: {e}")


if __name__ == '__main__':
    main()
