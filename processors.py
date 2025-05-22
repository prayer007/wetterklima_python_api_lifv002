import glob
import os
from multiprocessing import Pool
import rasterio
from pyproj import Transformer
from concurrent.futures import ThreadPoolExecutor
import asyncio

# import nest_asyncio
# nest_asyncio.apply()


class BaseGeoTIFFProcessor:
    """Base class for processing GeoTIFF files, encapsulating common functionalities."""

    def __init__(self, dataset_root: str, month: int = None, day: int = None):
        """Initializes the processor with the dataset root directory and optional filtering by month and/or day."""
        self.dataset_root = dataset_root
        self.month = month
        self.day = day
        self.transformer = Transformer.from_crs("epsg:4326", "epsg:31287", always_xy=True)


    def list_files_with_extension(self, directory: str, extension: str) -> list:
        """Lists all files in a directory with a specific extension, optionally filtering by month and/or day."""
        directory = os.path.join(directory, '')
        pattern = f"{directory}*{extension}"
        all_files = glob.glob(pattern)
        
        if self.month is None and self.day is None:
            return all_files
        
        filtered_files = []
        for file_path in all_files:
            filename = os.path.basename(file_path)
            
            if "CLIM" in filename:
                date_part = filename.split('_')[-3]
            else:
                date_part = filename.split('.')[-2].split("_")[-1]
                
            file_month = int(date_part[4:6])
            file_day = int(date_part[6:8])
            
            if self.month and self.day:
                if self.month == file_month and self.day == file_day:
                    filtered_files.append(file_path)
            elif self.month:
                if self.month == file_month:
                    filtered_files.append(file_path)
            elif self.day:
                if self.day == file_day:
                    filtered_files.append(file_path)
                    
        return filtered_files


    def extract_value_from_geotiff(self, args):
        """Extracts a value from a GeoTIFF file at specified latitude and longitude."""
        geotiff_path, lat, lng = args
        with rasterio.open(geotiff_path) as dataset:
            x, y = self.transformer.transform(lng, lat)
            if not (dataset.bounds.left <= x <= dataset.bounds.right and dataset.bounds.bottom <= y <= dataset.bounds.top):
                return None
            row, col = dataset.index(x, y)
            value = dataset.read(1, window=((row, row+1), (col, col+1)))[0, 0]
            return (geotiff_path, value)


class GeoTIFFMultiprocessingProcessor(BaseGeoTIFFProcessor):
    """Processes GeoTIFF files using multiprocessing to efficiently extract geographic data."""

    def __init__(self, dataset_root: str, month: int = None, day: int = None, cores: int = 4):
        super().__init__(dataset_root, month, day)
        self.cores = cores

    def process_geotiffs(self, extension: str, lat: float, lng: float) -> list:
        file_paths = self.list_files_with_extension(self.dataset_root, extension)
        tasks = [(path, lat, lng) for path in file_paths]
        with Pool(processes=self.cores) as pool:
            results = pool.map(self.extract_value_from_geotiff, tasks)
        return results


class GeoTIFFThreadingProcessor(BaseGeoTIFFProcessor):
    """Processes GeoTIFF files using threading."""

    def __init__(self, dataset_root: str, month: int = None, day: int = None, threads: int = 4):
        super().__init__(dataset_root, month, day)
        self.threads = threads

    def process_geotiffs(self, extension: str, lat: float, lng: float) -> list:
        file_paths = self.list_files_with_extension(self.dataset_root, extension)
        tasks = [(path, lat, lng) for path in file_paths]
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            results = list(executor.map(self.extract_value_from_geotiff, tasks))
        return results
    
    
class AsyncGeoTIFFProcessor(BaseGeoTIFFProcessor):
    
    """Processes GeoTIFF files using asyncio for asynchronous I/O operations."""

    def __init__(self, dataset_root: str, month: int = None, day: int = None, max_workers: int = 4):
        super().__init__(dataset_root, month, day)
        self.max_workers = max_workers

    async def async_extract_value_from_geotiff(self, args):
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self.extract_value_from_geotiff, args)
        return result

    async def process_geotiffs_async(self, extension: str, lat: float, lng: float) -> list:
        file_paths = self.list_files_with_extension(self.dataset_root, extension)
        tasks = [(path, lat, lng) for path in file_paths]
        async_tasks = [self.async_extract_value_from_geotiff(task) for task in tasks]
        results = await asyncio.gather(*async_tasks)
        return results

    def process_geotiffs(self, extension: str, lat: float, lng: float) -> list:
        return asyncio.run(self.process_geotiffs_async(extension, lat, lng))

