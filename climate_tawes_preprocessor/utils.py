from urllib.parse import urlparse
import os
from typing import Optional

def extract_dataset_version_from_endpoint_url(url: str) -> Optional[str]:

    path = urlparse(url).path
    basename = os.path.basename(path)
    parts = basename.split('-')
    
    for part in parts:
        if part.startswith('v') and part[1:].isdigit():
            return part
    
    return None
