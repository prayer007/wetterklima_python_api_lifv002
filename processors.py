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

    def __init__(self, dataset_root: str):
        """Initializes the processor with the dataset root directory."""
        self.dataset_root = dataset_root
        self.transformer = Transformer.from_crs("epsg:4326", "epsg:31287", always_xy=True)

    @staticmethod
    def list_files_with_extension(directory: str, extension: str) -> list:
        """Lists all files in a directory with a specific extension."""
        directory = os.path.join(directory, '')
        pattern = f"{directory}*{extension}"
        return glob.glob(pattern)

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

    def __init__(self, dataset_root: str, cores: int = 4):
        """Initialize the multiprocessing processor with the specified dataset root directory and number of cores."""
        super().__init__(dataset_root)
        self.cores = cores

    def process_geotiffs(self, extension: str, lat: float, lng: float) -> list:
        """Process GeoTIFF files in the dataset directory using multiprocessing."""
        file_paths = self.list_files_with_extension(self.dataset_root, extension)
        tasks = [(path, lat, lng) for path in file_paths]
        # Use a multiprocessing Pool to execute the tasks in parallel
        with Pool(processes=self.cores) as pool:
            # The map function automatically handles distributing tasks across the pool
            results = pool.map(self.extract_value_from_geotiff, tasks)
        return results


class GeoTIFFThreadingProcessor(BaseGeoTIFFProcessor):
    """Processes GeoTIFF files using threading."""

    def __init__(self, dataset_root: str, threads: int = 4):
        super().__init__(dataset_root)
        self.threads = threads

    def process_geotiffs(self, extension: str, lat: float, lng: float) -> list:
        """Processes GeoTIFF files using threading."""
        file_paths = self.list_files_with_extension(self.dataset_root, extension)
        tasks = [(path, lat, lng) for path in file_paths]
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            results = list(executor.map(self.extract_value_from_geotiff, tasks))
        return results
    
    
class AsyncGeoTIFFProcessor(BaseGeoTIFFProcessor):
    """Processes GeoTIFF files using asyncio for asynchronous I/O operations."""

    def __init__(self, dataset_root: str, max_workers: int = 4):
        """Initialize the async processor with the specified dataset root directory and max workers."""
        super().__init__(dataset_root)
        self.max_workers = max_workers

    async def async_extract_value_from_geotiff(self, args):
        """An async wrapper around the `extract_value_from_geotiff` method."""
        loop = asyncio.get_event_loop()
        # Use run_in_executor to run the synchronous extract_value_from_geotiff method in a thread pool
        result = await loop.run_in_executor(None, self.extract_value_from_geotiff, args)
        return result

    async def process_geotiffs_async(self, extension: str, lat: float, lng: float) -> list:
        """Asynchronously process GeoTIFF files using asyncio."""
        file_paths = self.list_files_with_extension(self.dataset_root, extension)
        tasks = [(path, lat, lng) for path in file_paths]
        async_tasks = [self.async_extract_value_from_geotiff(task) for task in tasks]
        
        # Gather all tasks and run them concurrently
        results = await asyncio.gather(*async_tasks)
        return results

    def process_geotiffs(self, extension: str, lat: float, lng: float) -> list:
        """A synchronous wrapper to run the async process method."""
        return asyncio.run(self.process_geotiffs_async(extension, lat, lng))
