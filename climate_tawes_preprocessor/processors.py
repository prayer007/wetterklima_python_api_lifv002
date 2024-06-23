import requests
from usertypes_datahub import Metadata, EndpointData, DatasetType, Metadata_v2
import utils

GSA_DATAHUB_PROVIDER = "https://dataset.api.hub.geosphere.at/v1"
GSA_DATAHUB_ROUTE_HISTORICAL = f"{GSA_DATAHUB_PROVIDER}/station/historical"
GSA_DATAHUB_ROUTE_CURRENT = f"{GSA_DATAHUB_PROVIDER}/station/current"


class TawesStationClimateStats:
    
    
    def __init__(self, endpoint: dict, only_active_stations: bool = False):
        
        print(f"Starting processing endpoint {endpoint['url']}")
        
        self._only_active_stations: bool = only_active_stations
        self._endpoint: EndpointData = self._validate_and_set_endpoint(endpoint)
        self._metadata: Metadata = self._validate_and_set_metadata()
    
    
    @classmethod
    def fetch_dataset_endpoints(cls):
        url = GSA_DATAHUB_PROVIDER + '/datasets'
        response = requests.get(url)
        if response.status_code == 200:
            datasets = response.json()
            return datasets
        else:
            return {"error": f"Failed to fetch data, status code: {response.status_code}"}
        
    
    def calculate_stats():
        todo=1
     
        
    def _validate_and_set_endpoint(self, endpoint: dict):
        
        if endpoint['type'] not in DatasetType.__args__:
            raise ValueError(f"Endpoint type {endpoint['type']} not supported!")
        else:
            return EndpointData(**endpoint)
        
        
    def _validate_and_set_metadata(self) -> Metadata:
        
        url = f"{self._endpoint.url}/metadata"
        response = requests.get(url)
        
        meta = response.json()
        
        # Filter inactive station
        if self._only_active_stations:
            meta['stations'] = [station for station in meta['stations'] if station['is_active']]
        
        # Filter stations with no altitude information
        meta['stations'] = [station for station in meta['stations'] if station['altitude'] != None]
 
        if response.status_code == 200:
            dataset_version = utils.extract_dataset_version_from_endpoint_url(self._endpoint.url)
            
            if dataset_version == 'v1':
                metadata = Metadata(**meta)
            elif dataset_version == 'v2':
                metadata = Metadata_v2(**meta)
            else:
                raise ValueError(f"Dataset version {dataset_version} not supported!")
        
            return metadata
        
        

