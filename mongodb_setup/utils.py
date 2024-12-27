from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

def initialize_database(db_name, uri="mongodb://localhost:27017/"):
    """
    Ensure the database exists by connecting to it.
    """
    client = MongoClient(uri)
    db = client[db_name]
    print(f"Database '{db_name}' initialized.")
    return db

def create_annual_comparison_collection(db, indexes=None):
    """
    Create a MongoDB collection with schema validation and optional indexes.
    """
    collection_name = "annual_comparison"

    # Define JSON schema for validation
    schema = {
        "bsonType": "object",
        "required": ["station_id", "station_id_source", "variable", "variable_name_source", "period", "period_source", "data"],
        "properties": {
            "station_id": {
                "bsonType": "int",
                "description": "Station ID of the target station."
            },
            "station_id_source": {
                "bsonType": "int",
                "description": "Station ID of the source station."
            },
            "variable": {
                "bsonType": "string",
                "description": "Variable name at the target station."
            },
            "variable_name_source": {
                "bsonType": "string",
                "description": "Variable name at the source station."
            },
            "period": {
                "bsonType": "string",
                "enum": ["d", "m", "y"],
                "description": "Data aggregation period ('d', 'm', 'y')."
            },
            "period_source": {
                "bsonType": "string",
                "enum": ["d", "m", "y"],
                "description": "Source data aggregation period ('d', 'm', 'y')."
            },
            "data": {
                "oneOf": [
                    {
                        "bsonType": "object",
                        "description": "Dictionary for daily or monthly data.",
                        "additionalProperties": {
                            "bsonType": "object",
                            "description": "Year-value mapping for specific day or month.",
                            "additionalProperties": {
                                "bsonType": ["double", "null"],
                                "description": "Value for the specific year."
                            }
                        }
                    },
                    {
                        "bsonType": "array",
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
                                    "bsonType": ["double", "null"],
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
            # Correctly unpack the index tuple
            if isinstance(index, tuple):
                field_name, order = index
                index_name = f"{field_name}_{'asc' if order == 1 else 'desc'}_idx"
                existing_indexes = [i["name"] for i in collection.list_indexes()]
                if index_name not in existing_indexes:
                    collection.create_index([(field_name, order)], name=index_name)
                    print(f"Index '{index_name}' created on collection '{collection_name}'.")
                else:
                    print(f"Index '{index_name}' already exists on collection '{collection_name}'.")
            else:
                raise ValueError(f"Index format is invalid: {index}")

    # Ensure the uniqueness of the combination of IDs and variable
    unique_index_name = "station_variable_period_source_unique"
    if unique_index_name not in [i["name"] for i in collection.list_indexes()]:
        collection.create_index(
            [("station_id", 1), ("station_id_source", 1), ("variable", 1), ("variable_name_source", 1), ("period", 1)],
            unique=True,
            name=unique_index_name
        )
        print("Unique index on ('station_id', 'station_id_source', 'variable', 'variable_name_source', 'period') created.")
    else:
        print("Unique index on ('station_id', 'station_id_source', 'variable', 'variable_name_source', 'period') already exists.")
