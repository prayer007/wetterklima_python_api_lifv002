from flask import jsonify, request, make_response
from werkzeug.security import check_password_hash
import jwt
import datetime
import os

import api_utils
import utils
from app import app
from db_models import Users
import pandas as pd

from processors import BaseGeoTIFFProcessor

import logging
from logging.handlers import RotatingFileHandler

# Improve the logging configuration to ensure proper paths and capture all errors
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'wetterklima_errors.log')

if not app.debug:
    file_handler = RotatingFileHandler(log_file, maxBytes=10240, backupCount=10)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)
    
    # Log application startup
    app.logger.info('Wetterklima API startup')


GSA_DATAHUB_ROOT = "/home/shared/CRM/11_gsa_datahub/"


@app.errorhandler(500)
def internal_error(exception):
    app.logger.error(f"500 Internal Server Error: {str(exception)}")
    return "Internal server error", 500

@app.errorhandler(404)
def not_found_error(exception):
    app.logger.error(f"404 Not Found: {str(exception)}")
    return "Not found", 404

@app.errorhandler(400)
def bad_request_error(exception):
    app.logger.error(f"400 Bad Request: {str(exception)}")
    return "Bad request", 400


@app.route('/login', methods=['GET'])  
def login_user(): 
 

    exp = 60 # expiration time of the token in minutes
 
    auth = request.authorization   
    
    
    if not auth or not auth.username or not auth.password:  
        return make_response(f'could not verify authentification. Please post username and password. Auth: {auth}', 
                            401, {'WWW.Authentication': 'Basic realm: "login required"'})    
    
    user = Users.query.filter_by(name=auth.username).first()
    
    if user is None:
        return make_response('User not found.', 404, {'WWW.Authentication': 'Basic realm: "login required"'})

    try:
        if not check_password_hash(user.password, auth.password):
            return make_response('Password verification failed.', 401, {'WWW.Authentication': 'Basic realm: "login required"'})
    except Exception as e:
        app.logger.error(f"Error during password verification: {e}")
        return make_response('Internal server error.', 500, {'WWW.Authentication': 'Basic realm: "login required"'})

    if check_password_hash(user.password, auth.password):  
        token = jwt.encode({'public_id': user.public_id, 'exp' : datetime.datetime.utcnow() + datetime.timedelta(minutes=exp)}, app.config['SECRET_KEY'])  
        return jsonify({'token' : token, 'expires': f"{exp} minutes"}) 
     
 
    return make_response('could not verify authentification. Please post username and password.',  
                          401, {'WWW.Authentication': 'Basic realm: "login required"'})



@app.route('/test', methods=['GET'])
#@api_utils.token_required
def getTest():
    res = {"Hello": "World"}
    
    return res


@app.route('/gridTimeseries', methods=['POST'])
def getGridTimeseries():
    """
    Get the grid timeseries based on the provided parameters.

    Parameters:
    - dataset (str): The dataset identifier, e.g., 'spartacus-v2-1y-1km'.
    - variable (str): The variable of interest, e.g., 'TM'.
    - layerDate (datetime): The date of the layer, in datetime string.
    - lat (float): Latitude of the point of interest.
    - lng (float): Longitude of the point of interest.

    Returns:
    - JSON response containing timeseries data with statistics and altitude if successful, or HTTP 204 response if no data is found.
    """
    try:
        request_data = request.get_json()

        # Validate required parameters
        required_params = ['dataset', 'variable', 'layerDate', 'lat', 'lng']
        for param in required_params:
            if param not in request_data:
                app.logger.error(f"Missing required parameter: {param}")
                return make_response(f"Missing required parameter: {param}", 400)

        dataset = request_data['dataset']
        variable = request_data['variable']
        layerDate = request_data['layerDate']
        lat = request_data['lat']
        lng = request_data['lng']
        climate = request_data.get('climate', False)
        climatePeriod = request_data.get('climate_period', None)

        # Only prepend '/climate_data/' if climate is True
        if climate == True:
            dataset_path = f"/climate_data/{dataset}" 
        else: 
            dataset_path = dataset
            climatePeriod = None

        layerDateCategory = utils.extract_date_category_from_dataset_name(dataset_path)
        layerDateDt = pd.to_datetime(layerDate) + pd.Timedelta(hours=12)

        geotiff_processor = BaseGeoTIFFProcessor(GSA_DATAHUB_ROOT)
        dem_fp = f"{GSA_DATAHUB_ROOT}/dem/output_COP90_31287.tif"
        altitude = geotiff_processor.extract_value_from_geotiff((dem_fp, lat, lng))

        if layerDateCategory == 'd':
            timeseries = utils.get_timeseries_from_dataset(dataset_path, variable, lat, lng, day=layerDateDt.day, climate_period=climatePeriod)
        elif layerDateCategory == 'm':
            timeseries = utils.get_timeseries_from_dataset(dataset_path, variable, lat, lng, month=layerDateDt.month, climate_period=climatePeriod)
        else:
            timeseries = utils.get_timeseries_from_dataset(dataset_path, variable, lat, lng, climate_period=climatePeriod)

        # Build the correct path for idr_iqr file
        if climate == True:
            stats_fp = f"{GSA_DATAHUB_ROOT}/climate_data/statistics/{dataset}/{variable}/geotiff_metrics_timeseries.csv"
        else:
            stats_fp = f"{GSA_DATAHUB_ROOT}/statistics/{dataset}/{variable}/geotiff_metrics_timeseries.csv"

        idr_iqr = pd.read_csv(stats_fp)
        idr_iqr['datetime'] = pd.to_datetime(idr_iqr['datetime'])

        if climate == True:
            idr_iqr = idr_iqr[idr_iqr['climate_period'] == climatePeriod]

        timeseries_df = pd.DataFrame(timeseries, columns=['datetime', 'value'])
        timeseries = timeseries_df.merge(idr_iqr, how='left', on='datetime')
        timeseries['datetime'] = timeseries['datetime'] + pd.Timedelta(hours=12)

        timeseries_with_stats = utils.create_timeseries_object(timeseries)

        timeseries_with_stats['stats']['altitude'] = altitude[-1]

        if timeseries_with_stats:
            return utils.convert_float32(timeseries_with_stats)
        else:
            return make_response('', 204)
    except Exception as e:
        app.logger.error(f"Error in getGridTimeseries: {str(e)}", exc_info=True)
        return make_response(f"An error occurred: {str(e)}", 500)
    

@app.route('/rasterStats', methods=['POST'])
def getRasterStats():
    """
    Get min, max and mean stats from a raster file
    """
    try:
        request_data = request.get_json()
        
        # Validate required parameters
        required_params = ['dataset', 'variable', 'selectedLayerName']
        for param in required_params:
            if param not in request_data:
                app.logger.error(f"Missing required parameter: {param}")
                return make_response(f"Missing required parameter: {param}", 400)
        
        dataset = request_data['dataset']
        variable = request_data['variable']
        layer_name = request_data['selectedLayerName']
        climate = request_data.get('climate', False)
        climatePeriod = request_data.get('climate_period', None)

        climate_fp = "climate_data" if climate else ''
        layer_fp = f"{GSA_DATAHUB_ROOT}/{climate_fp}/{dataset}/{variable}/{layer_name}.tif"
        
        app.logger.info(f"Looking for raster file: {layer_fp}")
        
        if os.path.exists(layer_fp): 
            # Extract statistics
            raster_stats = utils.get_raster_stats(layer_fp, variable, dataset)
            
            raster_stats['layer'] = layer_name
            raster_stats['dataset'] = dataset
            
            return utils.convert_float32(raster_stats)
        else:
            app.logger.warning(f"Raster file not found: {layer_fp}")
            return make_response(f'Raster file {layer_name} does not exist on the server', 204)
    except Exception as e:
        app.logger.error(f"Error in getRasterStats: {str(e)}", exc_info=True)
        return make_response(f"An error occurred: {str(e)}", 500)