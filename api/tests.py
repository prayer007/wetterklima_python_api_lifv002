import requests
import json

#%% Host
host = 'https://test.wedda.at'

#%% test login auth

req = requests.get(f'{host}/login', auth=('wetterklima_app', 'dfdjkd739dKJouqb'))
token = req.json()["token"]

headers = {
    "x-access-token": token,
    "Content-type":"application/json; charset=utf-8"
}


#%% get test
x = requests.get(f'{host}/test', headers = headers)

if x.status_code == 200:
    js = x.json()
    print("Get test OK: ",js)


#%% get timeseries of raster values
params={'dataset': 'spartacus-v2-1y-1km',
        'variable': 'TM',
        'lat': 47,
        'lng': 15
        }

x = requests.post(f'{host}/gridTimeseries', data = json.dumps(params), headers = headers)

if x.status_code == 200:
    js = x.json()
    print("Timeseries Raster value test OK: ",js)

