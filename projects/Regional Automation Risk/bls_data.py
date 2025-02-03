# %%
import requests
import json

url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
headers = {'Content-type': 'application/json'}
data = json.dumps({
    "seriesid": ["OEUM001018000000000000001"], # Example series ID for OEWS
    "startyear": "2023",
    "endyear": "2023",
    "registrationkey": "b88f9f5001c84b3bb69c4771a63a6123"
})

response = requests.post(url, data=data, headers=headers)
json_data = json.loads(response.text)

json_data
# %%
