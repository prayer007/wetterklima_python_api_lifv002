import reqs

if __name__ == "__main__":
    # Example usage
    station_id = 11035
    variable = "TL"
    period = "d"

    res = reqs.fetch_annual_comparison_data(station_id, variable, period)[0]

    data = res['data']
