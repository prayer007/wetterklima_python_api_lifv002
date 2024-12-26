from pymongo import MongoClient

def get_mongo_client(uri="mongodb://localhost:27017", db_name="your_database_name"):
    """
    Creates and returns a MongoDB client and database object.

    Parameters:
        uri (str): MongoDB connection URI.
        db_name (str): Name of the database.

    Returns:
        tuple: A tuple containing the MongoClient instance and the database instance.
    """
    client = MongoClient(uri)
    db = client[db_name]
    return client, db
