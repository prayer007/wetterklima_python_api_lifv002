import utils
import dummy_data

if __name__ == "__main__":
    # Initialize the database
    db_name = "wetterklima"
    db = utils.initialize_database(db_name)

    # Create the collection
    utils.create_annual_comparison_collection(
        db=db,
        indexes=[
            ("station_id", 1),  # Ascending
            ("variable", 1),    # Ascending
            ("period", -1)      # Descending
        ]
    )

    # dummy_data.generate_dummy_data_annual_comparison(db, station_id=11035)
