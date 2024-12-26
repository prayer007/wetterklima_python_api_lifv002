from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

def initialize_database(db_name, uri="mongodb://localhost:27017/"):
    """
    Ensure the database exists by connecting to it.
    
    Args:
        db_name (str): Name of the database to initialize.
        uri (str): MongoDB connection URI. Defaults to localhost.
    """
    client = MongoClient(uri)
    db = client[db_name]
    print(f"Database '{db_name}' initialized.")
    return db


def create_annual_comparison_collection(db, indexes=None):
    """
    Create a MongoDB collection with schema validation and optional indexes.

    Args:
        db (Database): MongoDB database object.
        indexes (list of tuples): List of indexes to create. Each tuple contains field name(s) and order (1 for ascending, -1 for descending).
    """
    collection_name = "annual_comparison"

    # Define JSON schema for validation
    schema = {
        "bsonType": "object",
        "required": ["station_id", "variable", "period", "data"],
        "properties": {
            "station_id": {
                "bsonType": "int",  # station_id must be an integer
                "description": "Must be an integer and is required."
            },
            "variable": {
                "bsonType": "string",  # variable must be a string
                "description": "Must be a string and is required."
            },
            "period": {
                "bsonType": "string",  # period must be a string
                "enum": ["d", "m", "y"],  # Restrict values to 'd', 'm', or 'y'
                "description": "Must be one of 'd' (daily), 'm' (monthly), or 'y' (yearly)."
            },
            "data": {
                "oneOf": [  # Allow different structures based on period
                    {
                        "bsonType": "object",  # Dictionary for periods 'd' and 'm'
                        "description": "Dictionary for daily or monthly data.",
                        "additionalProperties": {
                            "bsonType": "object",
                            "description": "Year-value mapping for specific day or month.",
                            "additionalProperties": {"bsonType": "double"}
                        }
                    },
                    {
                        "bsonType": "array",  # Array of objects for period 'y'
                        "description": "Array of yearly data with date and value fields.",
                        "items": {
                            "bsonType": "object",
                            "required": ["date", "value"],
                            "properties": {
                                "date": {
                                    "bsonType": "string",
                                    "description": "Year in YYYY format."
                                },
                                "value": {
                                    "bsonType": "double",
                                    "description": "Value for the year."
                                }
                            }
                        }
                    }
                ]
            }
        }
    }

    try:
        db.create_collection(
            collection_name,
            validator={"$jsonSchema": schema}
        )
        print(f"Collection '{collection_name}' created with validation.")
    except CollectionInvalid:
        print(f"Collection '{collection_name}' already exists.")

    # Access the collection
    collection = db[collection_name]

    # Create indexes if provided
    if indexes:
        for index in indexes:
            field_names = index[:-1] if len(index) > 1 else [index[0]]
            order = index[-1] if len(index) > 1 else 1
            index_name = "_".join([f"{field}_{'asc' if order == 1 else 'desc'}" for field in field_names])
            existing_indexes = [i["name"] for i in collection.list_indexes()]
            if index_name not in existing_indexes:
                collection.create_index([(field, order) for field in field_names], name=index_name)
                print(f"Index '{index_name}' created on collection '{collection_name}'.")
            else:
                print(f"Index '{index_name}' already exists on collection '{collection_name}'.")

    # Ensure the uniqueness of the combination of station_id, variable, and period
    unique_index_name = "station_variable_period_unique"
    if unique_index_name not in [i["name"] for i in collection.list_indexes()]:
        collection.create_index(
            [("station_id", 1), ("variable", 1), ("period", 1)],
            unique=True,
            name=unique_index_name
        )
        print("Unique index on ('station_id', 'variable', 'period') created.")
    else:
        print("Unique index on ('station_id', 'variable', 'period') already exists.")
