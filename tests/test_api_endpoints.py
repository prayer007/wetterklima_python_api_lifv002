import pytest
import requests
import json
import logging
import datetime

# RUN TEST WITH: pytest -v --tb=short tests/test_api_endpoints.py

class TestApiEndpoints:
    def log_error(self, test_name, error_message):
        """Log errors to a file for monitoring"""
        with open("error.log", "a") as error_log:
            error_log.write(f"{datetime.datetime.now()} - {test_name}: {error_message}\n")

    def test_health_check(self, api_client):
        """Test basic health check endpoint"""
        logging.info("Running test_health_check")
        try:
            response = requests.get(f"{api_client['base_url']}/test", headers=api_client['headers'])
            assert response.status_code == 200
            logging.info(f"Health check passed: {response.json()}")
        except AssertionError as e:
            self.log_error("test_health_check", str(e))
            raise

    def test_grid_timeseries_climate(self, api_client):
        """Test grid timeseries endpoint"""
        logging.info("Running test_grid_timeseries")
        try:
            now = datetime.datetime.now()
            current_year = now.year
            current_month = f"{now.month:02d}"
            variable = "TM"
            test_date = f"{current_year}-{current_month}-01 00:00:00"
            
            params = {
                'dataset': 'spartacus-v2-1m-1km',
                'variable': variable,
                'layerDate': test_date,
                'lat': 47,
                'lng': 15,
                'climate': True,
                'climate_period': '1991_2020'
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
        except AssertionError as e:
            self.log_error("test_grid_timeseries", str(e))
            raise

    def test_grid_timeseries(self, api_client):
        """Test grid timeseries endpoint"""
        logging.info("Running test_grid_timeseries")
        try:
            # Get current year and a date from last year
            now = datetime.datetime.now()
            current_year = now.year
            current_month = f"{now.month:02d}"
            variable = "TM"
            test_date = f"{current_year}-{current_month}-01 00:00:00"
            
            params = {
                'dataset': 'spartacus-v2-1m-1km',
                'variable': variable,
                'layerDate': test_date,
                'lat': 47,
                'lng': 15,
                'climate': False
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
        except AssertionError as e:
            self.log_error("test_grid_timeseries", str(e))
            raise

    def test_raster_stats(self, api_client):
        """Test raster stats endpoint"""
        logging.info("Running test_raster_stats_climate")
        try:
            # Get current year for dynamic layer name
            now = datetime.datetime.now()
            current_year = now.year
            current_month = f"{now.month:02d}"
            variable = "TM"
            layer_name = f"SPARTACUS2-MONTHLY_{variable}_{current_year}_{current_year}{current_month}01T000000"

            params = {
                'dataset': 'spartacus-v2-1m-1km',
                'selectedLayerName': layer_name,
                'variable': variable,
                'climate': False
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
            assert 'min' in data
            assert 'max' in data
            assert 'mean' in data
            logging.info("Raster stats test passed")
        except AssertionError as e:
            self.log_error("test_raster_stats", str(e))
            raise


    def test_raster_stats_climate(self, api_client):
        """Test raster stats endpoint"""
        logging.info("Running test_raster_stats_climate")
        try:
            # Get current year for dynamic layer name
            now = datetime.datetime.now()
            current_year = now.year
            current_month = f"{now.month:02d}"
            variable = "TM"
            layer_name = f"SPARTACUS2-MONTHLY_{variable}_{current_year}_CLIM_{current_year}{current_month}01T000000_1991_2020"
            
            params = {
                'dataset': 'spartacus-v2-1m-1km',
                'selectedLayerName': layer_name,
                'variable': variable,
                'climate': True
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
            assert 'min' in data
            assert 'max' in data
            assert 'mean' in data
            logging.info("Raster stats test passed")
        except AssertionError as e:
            self.log_error("test_raster_stats", str(e))
            raise

    def test_login(self, api_client):
        """Test login endpoint with basic auth"""
        logging.info("Running test_login")
        try:
            # Using a sample test user - replace with valid test credentials
            auth = ('test_user', 'test_password')
            
            response = requests.get(
                f"{api_client['base_url']}/login",
                auth=auth
            )
            
            # If authentication is successful
            if response.status_code == 200:
                data = response.json()
                assert 'token' in data
                assert 'expires' in data
                logging.info("Login test passed with valid credentials")
            # If using invalid test credentials, expect 401 or 404
            else:
                assert response.status_code in [401, 404]
                logging.info(f"Login test completed with status: {response.status_code}")
        except AssertionError as e:
            self.log_error("test_login", str(e))
            raise

    def test_invalid_login(self, api_client):
        """Test login endpoint with invalid credentials"""
        logging.info("Running test_invalid_login")
        try:
            # Using invalid credentials
            auth = ('invalid_user', 'invalid_password')
            
            response = requests.get(
                f"{api_client['base_url']}/login",
                auth=auth
            )
            
            assert response.status_code in [401, 404]
            logging.info(f"Invalid login test passed with status: {response.status_code}")
        except AssertionError as e:
            self.log_error("test_invalid_login", str(e))
            raise

    def test_grid_timeseries_invalid_params(self, api_client):
        """Test grid timeseries endpoint with invalid parameters"""
        logging.info("Running test_grid_timeseries_invalid_params")
        try:
            # Get current year and a date from last year
            current_year = datetime.datetime.now().year
            last_year = current_year - 1
            test_date = f"{last_year}-12-12 00:00:00"
            
            # Missing required parameters
            params = {
                'dataset': 'spartacus-v2-1y-1km',
                # Missing variable
                'layerDate': test_date,
                'lat': 47,
                'lng': 15
            }
            
            response = requests.post(
                f"{api_client['base_url']}/gridTimeseries", 
                data=json.dumps(params), 
                headers=api_client['headers']
            )
            
            # Expect error response (either 400 Bad Request or 500 Internal Server Error)
            assert response.status_code in [400, 500]
            logging.info(f"Grid timeseries invalid params test passed with status: {response.status_code}")
        except AssertionError as e:
            self.log_error("test_grid_timeseries_invalid_params", str(e))
            raise

    def test_raster_stats_nonexistent_layer(self, api_client):
        """Test raster stats endpoint with nonexistent layer"""
        logging.info("Running test_raster_stats_nonexistent_layer")
        try:
            # Get current year for dynamic layer name
            current_year = datetime.datetime.now().year
            nonexistent_layer = f"NONEXISTENT_LAYER_TM_{current_year}_CLIM_{current_year}0101T000000_1991_2020"
            
            params = {
                'dataset': 'spartacus-v2-1y-1km',
                'selectedLayerName': nonexistent_layer,
                'variable': 'TM',
                'climate': True
            }
            
            response = requests.post(
                f"{api_client['base_url']}/rasterStats", 
                data=json.dumps(params), 
                headers=api_client['headers']
            )
            
            # Expect 204 No Content response for non-existent layer
            assert response.status_code == 204
            logging.info("Raster stats nonexistent layer test passed")
        except AssertionError as e:
            self.log_error("test_raster_stats_nonexistent_layer", str(e))
            raise
