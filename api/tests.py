import requests
import json
import reqs

#%% Host
host = 'http://143.224.185.119:5002'

#%% test login auth

headers = {
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
        'layerDate': '2022-12-12 00:00:00',
        'lat': 47,
        'lng': 15
        }

x = requests.post(f'{host}/gridTimeseries', data = json.dumps(params), headers = headers)

if x.status_code == 200:
    js = x.json()
    print("Timeseries Raster value test OK: ",js)
else:
    print(f"ERROR: {x.status_code}")

#%% get annual comparison
station_id = 11035
variable = "TL"
period = "d"

res = reqs.fetch_annual_comparison_data(station_id, variable, period)[0]

data = res['data']