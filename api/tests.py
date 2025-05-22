import requests
import json
import os
import datetime
import traceback

#%% Setup logging
log_dir = "/root/apis/wetterklima/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"api_tests_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def log_message(message, error=False):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_type = "ERROR" if error else "INFO"
    log_entry = f"[{timestamp}] [{log_type}] {message}\n"
    
    print(log_entry.strip())
    
    with open(log_file, "a") as f:
        f.write(log_entry)

#%% Host
host = 'http://143.224.185.119:5002'

#%% test login auth
log_message("Starting API tests")
log_message(f"Using host: {host}")

headers = {
    "Content-type":"application/json; charset=utf-8"
}

#%% get test
try:
    log_message("Running GET /test endpoint test")
    x = requests.get(f'{host}/test', headers = headers)

    if x.status_code == 200:
        js = x.json()
        log_message(f"Get test OK: {js}")
    else:
        log_message(f"Get test failed with status code: {x.status_code}, response: {x.text}", error=True)
except Exception as e:
    log_message(f"Exception during GET /test test: {str(e)}\n{traceback.format_exc()}", error=True)

#%% get timeseries of raster values
try:
    log_message("Running POST /gridTimeseries endpoint test")
    
    # Get current year and a date from last year
    current_year = datetime.datetime.now().year
    last_year = current_year - 1
    test_date = f"{last_year}-12-12 00:00:00"
    
    params={'dataset': 'spartacus-v2-1y-1km',
            'variable': 'TM',
            'layerDate': test_date,
            'lat': 47,
            'lng': 15
            }
    log_message(f"Request parameters: {params}")

    x = requests.post(f'{host}/gridTimeseries', data = json.dumps(params), headers = headers)

    if x.status_code == 200:
        js = x.json()
        log_message(f"Timeseries Raster value test OK: {json.dumps(js)[:200]}... (truncated)")
    else:
        log_message(f"Timeseries Raster value test failed with status code: {x.status_code}, response: {x.text}", error=True)
except Exception as e:
    log_message(f"Exception during POST /gridTimeseries test: {str(e)}\n{traceback.format_exc()}", error=True)

# #%% get annual comparison
# station_id = 11035
# variable = "TL"
# period = "d"

# res = reqs.fetch_annual_comparison_data(station_id, variable, period)[0]

# data = res['data']

#%% get selected layer stats
try:
    log_message("Running POST /rasterStats endpoint test")
    
    # Get current year for dynamic layer name
    current_year = datetime.datetime.now().year
    layer_name = f"SPARTACUS2-YEARLY_TM_{current_year}_CLIM_{current_year}0101T000000_1991_2020"
    
    params = {
        'dataset': 'spartacus-v2-1y-1km',
        'selectedLayerName': layer_name,
        'variable': 'TM'
    }
    log_message(f"Request parameters: {params}")

    x = requests.post(f'{host}/rasterStats', data=json.dumps(params), headers=headers)

    if x.status_code == 200:
        js = x.json()
        log_message(f"Get Selected Layer Stats test OK: {json.dumps(js)[:200]}... (truncated)")
    else:
        log_message(f"Get Selected Layer Stats test failed with status code: {x.status_code}, response: {x.text}", error=True)
except Exception as e:
    log_message(f"Exception during POST /rasterStats test: {str(e)}\n{traceback.format_exc()}", error=True)

log_message("All tests completed")

# A better approach would be to reorganize into a structure like:
"""
/root/apis/wetterklima/
  ├── tests/
  │   ├── __init__.py
  │   ├── conftest.py                 # Common fixtures and setup
  │   ├── test_api_endpoints.py       # API tests
  │   ├── test_utils.py               # Unit tests for utils
  │   └── test_processors.py          # Unit tests for processors
  ├── logs/
  └── api/
      └── ...
"""

# Example of how test_api_endpoints.py might look:
"""
import pytest
import requests
import json
import logging

# Configure logging
logging.basicConfig(
    filename='/root/apis/wetterklima/logs/tests.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@pytest.fixture
def api_client():
    # Setup common test client configuration
    base_url = 'http://143.224.185.119:5002'
    headers = {"Content-type": "application/json; charset=utf-8"}
    
    return {
        'base_url': base_url,
        'headers': headers
    }

class TestApiEndpoints:
    def test_health_check(self, api_client):
        # Test basic health check endpoint
        response = requests.get(f"{api_client['base_url']}/test", headers=api_client['headers'])
        assert response.status_code == 200
        logging.info(f"Health check passed: {response.json()}")

    def test_grid_timeseries(self, api_client):
        # Test grid timeseries endpoint
        params = {
            'dataset': 'spartacus-v2-1y-1km',
            'variable': 'TM',
            'layerDate': '2022-12-12 00:00:00',
            'lat': 47,
            'lng': 15
        }
        logging.info(f"Testing grid timeseries with params: {params}")
        
        response = requests.post(
            f"{api_client['base_url']}/gridTimeseries", 
            data=json.dumps(params), 
            headers=api_client['headers']
        )
        
        assert response.status_code == 200
        data = response.json()
        assert 'timeseries' in data
        logging.info("Grid timeseries test passed")

    def test_raster_stats(self, api_client):
        # Test raster stats endpoint
        params = {
            'dataset': 'spartacus-v2-1y-1km',
            'selectedLayerName': 'SPARTACUS2-YEARLY_TM_2022_CLIM_20220101T000000_1991_2020',
            'variable': 'TM'
        }
        logging.info(f"Testing raster stats with params: {params}")
        
        response = requests.post(
            f"{api_client['base_url']}/rasterStats", 
            data=json.dumps(params), 
            headers=api_client['headers']
        )
        
        assert response.status_code == 200
        data = response.json()
        # Add specific assertions about the expected data structure
        logging.info("Raster stats test passed")
"""