from mongodb_connection import get_mongo_client

def fetch_annual_comparison_data(station_id, variable, period, uri="mongodb://localhost:27017", db_name="wetterklima"):
    """
    Fetches data from the 'annual_comparison' collection filtered by station_id, variable, and period.

    Parameters:
        station_id (int): The station ID to filter the data.
        variable (str): The variable to filter the data.
        period (str): The period to filter the data.
        uri (str): MongoDB connection URI.
        db_name (str): Name of the database.

    Returns:
        list: A list of matching documents from the collection.
    """
    # Get MongoDB client and database
    client, db = get_mongo_client(uri, db_name)

    try:
        # Access the 'annual_comparison' collection
        collection = db["annual_comparison"]

        # Query the collection
        query = {
            "station_id": station_id,
            "variable": variable,
            "period": period
        }
        result = list(collection.find(query))

        return result

    finally:
        # Close the MongoDB client
        client.close()
