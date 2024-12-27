import random
from datetime import datetime, timedelta

def generate_dummy_data_annual_comparison(db, station_id=11035, station_id_source=9501):
    """
    Generate dummy data for the 'annual_comparison' collection.
    Ensures no duplicate data for the same station and variable combination.
    """
    collection_name = "annual_comparison"
    collection = db[collection_name]
    start_year_init = 1950
    end_year_init = 2024

    # Map variables to their corresponding periods
    variable_map = {
        "d": [("TL", "tl_mittel", "d"), ("RR", "rr_sum", "d")],
        "m": [("TMAX", "tmax_mean", "m"), ("TMIN", "tmin_mean", "m")],
        "y": [("AVG_TEMP", "temp_avg", "y")]
    }

    existing_combinations = set(
        (doc["variable"], doc["variable_name_source"], doc["period"], doc["period_source"])
        for doc in collection.find(
            {"station_id": station_id, "station_id_source": station_id_source},
            {"variable": 1, "variable_name_source": 1, "period": 1, "period_source": 1}
        )
    )

    dummy_data = []

    for period, variables in variable_map.items():
        for variable, variable_source, period_source in variables:
            if (variable, variable_source, period, period_source) in existing_combinations:
                continue

            # Generate data
            if period == "d":
                data = {
                    f"{(datetime(2020, 1, 1) + timedelta(days=day)).strftime('%m-%d')}": {
                        str(year): round(random.uniform(-10, 100), 2) for year in range(start_year_init, end_year_init)
                    }
                    for day in range(0, 365)
                }
            elif period == "m":
                data = {
                    f"{month:02d}": {
                        str(year): round(random.uniform(-10, 100), 2) for year in range(start_year_init, end_year_init)
                    }
                    for month in range(1, 13)
                }
            elif period == "y":
                data = [
                    {"date": str(year), "value": round(random.uniform(-10, 100), 2)}
                    for year in range(start_year_init, end_year_init)
                ]

            dummy_data.append({
                "station_id": station_id,
                "station_id_source": station_id_source,
                "variable": variable,
                "variable_name_source": variable_source,
                "period": period,
                "period_source": period_source,
                "data": data
            })

    if dummy_data:
        collection.insert_many(dummy_data)
        print(f"Inserted {len(dummy_data)} dummy records into collection '{collection_name}'.")
    else:
        print("No new records to insert. All combinations already exist.")

