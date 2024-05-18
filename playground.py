import utils
import pandas as pd
import datetime

from processors import BaseGeoTIFFProcessor


dataset = 'spartacus-v2-1m-1km'
variable = 'SA'
layerDate = datetime.datetime(2022,12,12,0,0,0)
lat = 47
lng = 15
GSA_DATAHUB_ROOT = "/home/shared/CRM/11_gsa_datahub/"

layerDateCategory = utils.extract_date_category_from_dataset_name(dataset)

# Extract altitude from DEM
geotiff_processor = BaseGeoTIFFProcessor(GSA_DATAHUB_ROOT)
dem_fp = f"{GSA_DATAHUB_ROOT}/dem/output_COP90_31287.tif"
altitude = geotiff_processor.extract_value_from_geotiff((dem_fp, lat, lng))

# Extract timeseries and statistics
if layerDateCategory == 'd':
    timeseries = utils.get_timeseries_from_dataset(dataset, variable, lat, lng, day = 5)
elif layerDateCategory == 'm':
    timeseries = utils.get_timeseries_from_dataset(dataset, variable, lat, lng, month = 12)
else:
    timeseries = utils.get_timeseries_from_dataset(dataset, variable, lat, lng)

idr_iqr = pd.read_csv(f"{GSA_DATAHUB_ROOT}/statistics/{dataset}/{variable}/geotiff_metrics_timeseries.csv")
idr_iqr['datetime'] = pd.to_datetime(idr_iqr['datetime'])

timeseries_df = pd.DataFrame(timeseries, columns=['datetime', 'value'])

timeseries = timeseries_df.merge(idr_iqr, how = 'left', on = 'datetime')
timeseries['datetime'] = timeseries['datetime']  + pd.Timedelta(hours=12)

timeseries_with_stats = utils.create_timeseries_object(timeseries)

timeseries_with_stats['stats']['altitude'] = altitude[-1]
