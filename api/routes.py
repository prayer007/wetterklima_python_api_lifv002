from flask import jsonify, request, make_response
from werkzeug.security import check_password_hash
import jwt
import datetime

import api_utils
import utils
from app import app
from db_models import Users
import pandas as pd

from processors import BaseGeoTIFFProcessor

import logging
from logging.handlers import RotatingFileHandler

if not app.debug:
    file_handler = RotatingFileHandler('logs/wetterklima.log', maxBytes=10240, backupCount=10)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)


GSA_DATAHUB_ROOT = "/home/shared/CRM/11_gsa_datahub/"


@app.errorhandler(500)
def internal_error(exception):
    app.logger.error(exception)  # Log the exception
    return "Internal server error", 500


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
    if request.method == 'POST':
        
        request_data = request.get_json()

        dataset = request_data['dataset']
        variable = request_data['variable']
        lat = request_data['lat']
        lng = request_data['lng']
        
        # Extract altitude from DEM
        geotiff_processor = BaseGeoTIFFProcessor(GSA_DATAHUB_ROOT)
        dem_fp = f"{GSA_DATAHUB_ROOT}/dem/output_COP90_31287.tif"
        altitude = geotiff_processor.extract_value_from_geotiff((dem_fp, lat, lng))
        
        # Extract timeseries and statistics
        timeseries = utils.get_timeseries_from_dataset(dataset, variable, lat, lng)
        
        idr_iqr = pd.read_csv(f"{GSA_DATAHUB_ROOT}/statistics/{dataset}/{variable}/geotiff_metrics_timeseries.csv")
        idr_iqr['datetime'] = pd.to_datetime(idr_iqr['datetime'])
        
        timeseries_df = pd.DataFrame(timeseries, columns=['datetime', 'value'])
        timeseries = timeseries_df.merge(idr_iqr, how = 'left', on = 'datetime')
        
        timeseries_with_stats = utils.create_timeseries_object(timeseries)
        
        timeseries_with_stats['stats']['altitude'] = altitude[-1]
        
        if timeseries_with_stats:
            return utils.convert_float32(timeseries_with_stats)
        else:
            return make_response('', 204)
    
    