import random
from datetime import datetime, timedelta


def generate_dummy_data_annual_comparison(db, station_id=11035):
    """
    Generate dummy data for the 'annual_comparison' collection.
    Ensures no duplicate data for the same station_id and adds all data in one go.
    """
    collection_name = "annual_comparison"
    collection = db[collection_name]
    start_year_init = 1950
    end_year_init  = 2024

    # Map variables to their corresponding periods
    variable_map = {
        "d": ["TL", "TLMAX_period_max", "RR"],
        "m": ["t_period_mean", "tmax_period_max"],
        "y": ["t"]
    }

    # Track already existing (variable, period) combinations for the station
    existing_combinations = set(
        (doc["variable"], doc["period"])
        for doc in collection.find({"station_id": station_id}, {"variable": 1, "period": 1})
    )

    dummy_data = []

    for period, variables in variable_map.items():
        for variable in variables:
            # Skip if the combination already exists
            if (variable, period) in existing_combinations:
                continue

            # Generate data based on the period
            data = {}
            if period == "d":
                # Generate data for all days of the year
                for day in range(1, 366):
                    day_str = (datetime(2020, 1, 1) + timedelta(days=day - 1)).strftime("%m-%d")
                    data[day_str] = {
                        str(year): round(random.uniform(-10, 100), 2) for year in range(start_year_init, end_year_init)
                    }
            elif period == "m":
                # Generate data for all months
                for month in range(1, 13):
                    month_str = f"{month:02d}"
                    data[month_str] = {
                        str(year): round(random.uniform(-10, 100), 2) for year in range(start_year_init, end_year_init)
                    }
            elif period == "y":
                # Generate yearly data as an array
                start_year = start_year_init
                data = [{"date": i, "value": round(random.uniform(-10, 100), 2)} for i in range(start_year_init, end_year_init)]
            # Append the new record
            dummy_data.append({
                "station_id": station_id,
                "variable": variable,
                "period": period,
                "data": data
            })

    # Insert the new records into the database
    if dummy_data:
        collection.insert_many(dummy_data)
        print(f"Inserted {len(dummy_data)} dummy records into collection '{collection_name}'.")
    else:
        print("No new records to insert. All combinations already exist.")
