import xarray as xr
import os
import pandas as pd
import glob
import time
import re
from datetime import datetime
import numpy as np
import ast
import rasterio

import processors

GSA_DATAHUB_ROOT = "/home/shared/CRM/11_gsa_datahub/"


def get_raster_stats(raster_path):
    """
    Extract minimum, maximum, and mean statistics from a raster file.
    
    Parameters:
    raster_path (str): Path to the raster file.
    
    Returns:
    dict: A dictionary containing the minimum, maximum, and mean values of the raster data.
    """
    with rasterio.open(raster_path) as src:
        data = src.read(1) 

        if src.nodata is not None:
            data = data[data != src.nodata]
        
        min_val = np.min(data)
        max_val = np.max(data)
        mean_val = np.mean(data)
        
        return {
            'min': min_val,
            'max': max_val,
            'mean': mean_val
        }


def timeit(func):
    """
    A decorator that measures the execution time of a function.

    Args:
        func (Callable): The function to measure.

    Returns:
        Callable: A wrapper function that prints the execution time.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} executed in {end_time - start_time} seconds.")
        return result
    return wrapper


def extract_date_category_from_dataset_name(ds_name: str) -> str:
   return ds_name.split('-')[-2][1]
    

def convert_float32(data):
    """
    Recursively converts all float32 numbers in a dictionary to Python's default float type.

    Parameters
    ----------
    data : dict
        The dictionary potentially containing float32 values.

    Returns
    -------
    dict
        The dictionary with all float32 values converted to float64.
    """
    if isinstance(data, dict):
        return {k: convert_float32(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_float32(v) for v in data]
    elif isinstance(data, np.float32):
        return float(data)
    else:
        return data


def extract_datetime_from_filename(filename: str) -> datetime:
    """
    Extracts the datetime from a filename formatted like "SPARTACUS2-YEARLY_TM_1966_19660101T000000.tif".
    
    Args:
        filename (str): The filename to extract the datetime from.

    Returns:
        datetime: A datetime object representing the extracted datetime from the filename.
    """
    # Regular expression to match the datetime part in the filename
    datetime_pattern = r"\d{8}T\d{6}"
    match = re.search(datetime_pattern, filename)
    if match:
        datetime_str = match.group(0)
        # Convert the matched string to a datetime object
        return datetime.strptime(datetime_str, "%Y%m%dT%H%M%S")
    else:
        raise ValueError(f"Datetime could not be extracted from filename: {filename}")


def list_files_with_extension(directory: str, extension: str) -> list:
    """
    List all files in the specified directory with the given extension.

    Args:
        directory (str): The directory to search in.
        extension (str): The file extension to filter by, including the dot (e.g., '.txt').

    Returns:
        list: A list of paths to files with the specified extension.
    """
    # Ensure the directory path ends with a slash
    directory = os.path.join(directory, '')

    # Create the search pattern
    pattern = f"{directory}*{extension}"

    # Use glob to find files matching the pattern
    return glob.glob(pattern)


def extract_latlng_values_from_netcdf(file_path, lat, lng, variable_name):
    """
    Extract values from a netCDF file for given latitude and longitude using xarray.

    :param file_path: Path to the netCDF file.
    :param lat: Latitude coordinate.
    :param lng: Longitude coordinate.
    :param variable_name: The name of the variable to extract values from.
    :return: Extracted value(s) for the given coordinates.
    """
    # Open the netCDF file
    ds = xr.open_dataset(file_path)
    
    # Select the nearest point for the given latitude and longitude
    # method='nearest' allows selection of the nearest point based on given lat and lng
    point_value = ds.sel(lat=lat, lon=lng, method='nearest')[variable_name]
    
    # Load the data into memory and close the file
    value = point_value.load()
    ds.close()
    
    return value


def basename(filepath: str, with_extension: bool = True) -> str:
    """
    Returns the basename of a given absolute file path, with an option to include or exclude the file extension.

    Args:
        filepath (str): The absolute file path.
        with_extension (bool): If True, the returned basename includes the file extension; otherwise, it's excluded.

    Returns:
        str: The basename of the file, optionally including or excluding the file extension.
    """
    basename = os.path.basename(filepath)
    if with_extension:
        return basename
    else:
        # Split the basename and extension and return just the basename part
        return os.path.splitext(basename)[0]
    

def sort_tuple_array_by_datetime(array: list) -> list:
    """
    Sorts an array of tuples by the datetime object in each tuple.

    Args:
        array (list of tuples): Each tuple contains a datetime object and a numeric value.

    Returns:
        list of tuples: Sorted list based on the datetime objects.
    """
    return sorted(array, key=lambda x: x[0])


@timeit
def get_timeseries_from_dataset(dataset: str, variable: str, lat: float, lng: float, month = None, day = None) -> list:
    """
    Retrieves a time series of values from a dataset of GeoTIFF files for 
    a specific variable at given latitude and longitude coordinates.
    
    This function processes all GeoTIFF files within a specified dataset 
    and variable directory, extracting values at the specified geographic coordinates.
    It utilizes multiprocessing to enhance performance during file processing. 
    Each file's datetime is extracted from its filename, and results are 
    sorted in chronological order.
    
    Args:
        dataset (str): The name of the dataset directory within the GSA_DATAHUB_ROOT path.
        variable (str): The name of the variable directory within the dataset directory.
        lat (float): The latitude coordinate for which to extract the variable values.
        lng (float): The longitude coordinate for which to extract the variable values.
   
    Returns:
        list of tuples: A sorted list where each tuple contains a datetime object
        (representing the date and time extracted from the filename) and the extracted 
        value at the specified coordinates. The list is sorted in ascending order 
        based on datetime.
    
    Raises:
        ValueError: If the datetime cannot be extracted from any of the filenames.
    
    Example usage:
        dataset = "spartacus-v2-1y-1km"
        variable = "TM"
        lat = 47
        lng = 15
    
        timeseries = get_timeseries_from_dataset(dataset, variable, lat, lng)
        for timestamp, value in timeseries:
            print(f"{timestamp}: {value}")
    """
    
    data_dir = f"{GSA_DATAHUB_ROOT}/{dataset}/{variable}"

    processor = processors.GeoTIFFThreadingProcessor(data_dir, month = month, day = day, threads = 4)
    results = processor.process_geotiffs('.tif', lat, lng)
    
    if not all(res is None for res in results):
        results_processed = [(extract_datetime_from_filename(basename(res[0])),res[1]) for res in results]
        results_processed_sorted = sort_tuple_array_by_datetime(results_processed)
        #TODO: add SA transformation here
        if variable == 'SA':
            if month is not None:
                results_processed_sorted = [(date, duration / 3600 / 30) for date, duration in results_processed_sorted]
            
    else:
        return None
        
    return results_processed_sorted


def calculate_stats_for_timeseries(df):
    """
    Calculates mean, minimum, and maximum values for the entire 
    period and for two specific climate reference periods: 
    1961-1991 and 1991-2020. For min and max, the datetime and 
    value are provided as strings.

    Args:
        data (list of tuples): A list where each tuple contains a
        datetime object and a numeric value.

    Returns:
        dict: A dictionary containing the overall mean, min, max, 
        and the mean, min, max for each reference period, with datetimes
        formatted as strings.
    """

    # Convert datetime to string
    if pd.api.types.is_datetime64_any_dtype(df['datetime']):
        df['datetime'] = df['datetime'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        
    # Calculate overall statistics
    overall_mean = df['value'].mean()
    overall_min = df.loc[df['value'].idxmin()].values.tolist()
    overall_max = df.loc[df['value'].idxmax()].values.tolist()
    overall_top5 = df.sort_values(by='value', ascending=False)[:5] 
    overall_bottom5 = df.sort_values(by='value', ascending=True)[:5] 

    # Define the climate reference periods
    period1_start = "1961-01-01 00:00:00"
    period1_end = "1991-12-31 23:59:59"
    period2_start = "1991-01-01 00:00:00"
    period2_end = "2020-12-31 23:59:59"

    # Calculate statistics for the first climate reference period
    period1_df = df[(df['datetime'] >= period1_start) & (df['datetime'] <= period1_end)]
    period1_mean = period1_df['value'].mean()
    period1_min = period1_df.loc[period1_df['value'].idxmin()].values.tolist()
    period1_max = period1_df.loc[period1_df['value'].idxmax()].values.tolist()

    # Calculate statistics for the second climate reference period
    period2_df = df[(df['datetime'] >= period2_start) & (df['datetime'] <= period2_end)]
    period2_mean = period2_df['value'].mean()
    period2_min = period2_df.loc[period2_df['value'].idxmin()].values.tolist()
    period2_max = period2_df.loc[period2_df['value'].idxmax()].values.tolist()

    # Format results
    format_result = lambda result: {'date': result[0], 'value': result[1]}

    # Return the statistics as a dictionary
    return {
        'current_location_stats': {
            'overall': {'mean': overall_mean, 'min': format_result(overall_min), 'max': format_result(overall_max),
                        'top5': {'date': list(overall_top5['datetime'].values), 'value': list(overall_top5['value'].values)}, 
                        'bottom5': {'date': list(overall_bottom5['datetime'].values), 'value': list(overall_bottom5['value'].values)}
                        },
            '1961_1991': {'mean': period1_mean, 'min': format_result(period1_min), 'max': format_result(period1_max)},
            '1991_2020': {'mean': period2_mean, 'min': format_result(period2_min), 'max': format_result(period2_max)}
        }
    }


def convert_timeseries_tuple_to_dict(data):
    """
    Converts a list of tuples containing datetime objects and
    numeric values into a dictionary
    with 'dates' and 'values' as keys. The datetime objects are
    converted to string format.

    Args:
        data (list of tuples): Each tuple contains a datetime 
        object and a numeric value.

    Returns:
        dict: A dictionary with two keys, 'dates' and 'values', where 'dates' 
        is a list of datetime objects converted to strings and 'values' is 
        a list of corresponding numeric values.
    """
    dates = [item[0].strftime("%Y-%m-%d %H:%M:%S") if isinstance(item[0], datetime) else item[0] for item in data]
    values = [item[1] for item in data]

    return {'dates': dates, 'values': values}


def create_timeseries_object(timeseries):
    
    timeseries_stats = calculate_stats_for_timeseries(timeseries)
    timeseries_values = {
        'dates': list(timeseries['datetime'].values),
        'values': list(timeseries['value'].values)
    }
    
    for suffix in ['full', 'valley', 'mountain']:
        for key in [f'idr_{suffix}', f'iqr_{suffix}', f'minmax_{suffix}']:
            timeseries_stats[key] = {
                'min': list([ast.literal_eval(val)[0] for val in timeseries[key].values]),
                'max': list([ast.literal_eval(val)[1] for val in timeseries[key].values]),
                'mean': np.mean([ast.literal_eval(val)[0] for val in timeseries[key].values])
            }

    obj = {
           'timeseries': timeseries_values,
           'stats': timeseries_stats
    }
    
    return obj

