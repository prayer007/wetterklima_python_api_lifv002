from processors import TawesStationClimateStats
from usertypes_datahub import EndpointData

endpoints = TawesStationClimateStats.fetch_dataset_endpoints()

daily_stats_processor = TawesStationClimateStats(endpoints['/station/historical/klima-v2-1d'])
