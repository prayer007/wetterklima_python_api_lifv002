from processors import TawesStationClimateStats
from usertypes_datahub import EndpointData

endpoints = TawesStationClimateStats.fetch_dataset_endpoints()

for key, value in endpoints.items():
    if value['type'] == 'station' and value['mode'] == 'historical':
        print(key)
        processor = TawesStationClimateStats(value)