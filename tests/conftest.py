import pytest
import os
import logging
import datetime

@pytest.fixture(scope="session")
def api_client():
    """Fixture that provides the API client configuration"""
    # Setup logging
    log_dir = "/root/apis/wetterklima/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"api_tests_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Set up the base URL and headers for the API
    base_url = 'http://143.224.185.119:5002'
    headers = {"Content-type": "application/json; charset=utf-8"}
    
    return {
        'base_url': base_url,
        'headers': headers
    }
