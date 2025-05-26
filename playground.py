import utils
import pandas as pd
import datetime
import os

from processors import BaseGeoTIFFProcessor


dataset = 'spartacus-v2-1y-1km'
variable = 'TM'
layerDate = datetime.datetime(2025,1,1,0,0,0)
lat = 47
lng = 15
GSA_DATAHUB_ROOT = "/home/shared/CRM/11_gsa_datahub/climate_data"

layerDateCategory = utils.extract_date_category_from_dataset_name(dataset)

# # Extract altitude from DEM
# geotiff_processor = BaseGeoTIFFProcessor(GSA_DATAHUB_ROOT)
# dem_fp = f"/home/shared/CRM/11_gsa_datahub/dem/output_COP90_31287.tif"
# altitude = geotiff_processor.extract_value_from_geotiff((dem_fp, lat, lng))

# # Extract timeseries and statistics
# dataset_fp = 'climate_data/spartacus-v2-1m-1km'
# if layerDateCategory == 'd':
#     timeseries = utils.get_timeseries_from_dataset(dataset_fp, variable, lat, lng, day = 5)
# elif layerDateCategory == 'm':
#     timeseries = utils.get_timeseries_from_dataset(dataset_fp, variable, lat, lng, month = 12)
# else:
#     timeseries = utils.get_timeseries_from_dataset(dataset_fp, variable, lat, lng, climate_period = "1991_2020")

# idr_iqr = pd.read_csv(f"{GSA_DATAHUB_ROOT}/statistics/{dataset}/{variable}/geotiff_metrics_timeseries.csv")
# idr_iqr['datetime'] = pd.to_datetime(idr_iqr['datetime'])

# timeseries_df = pd.DataFrame(timeseries, columns=['datetime', 'value'])

# timeseries = timeseries_df.merge(idr_iqr, how = 'left', on = 'datetime')
# timeseries['datetime'] = timeseries['datetime']  + pd.Timedelta(hours=12)

# timeseries_with_stats = utils.create_timeseries_object(timeseries)

# timeseries_with_stats['stats']['altitude'] = altitude[-1]


dataset = 'spartacus-v2-1y-1km'
layer_name = 'SPARTACUS2-YEARLY_TM_2025_CLIM_20250101T000000_1961_1990'
variable = 'TM'


layer_fp = f"{GSA_DATAHUB_ROOT}/{dataset}/{variable}/{layer_name}.tif"

if os.path.exists(layer_fp): 
    
    # Extract statistics
    raster_stats = utils.get_raster_stats(layer_fp, variable, dataset)
    
    raster_stats['layer'] = layer_name
    raster_stats['dataset'] = dataset
    
    res = utils.convert_float32(raster_stats)