"""
This one-off script will be fired frequently from CRON jobs,
its intent is to keep the AWS Lambda endpoints 'warm'.
A major issue with using Lambda as an API endpoint is
that the underlying computational resources are garbage
collected if they aren't being used. This will produce 'cold'
starts, where everything in the underlying Lambda must boot
up again. Querying the endpoints frequently reduces this issue
substantially.
"""

import math
import datetime
import ssl
import urllib
import time
import pandas as pd
import numpy as np

from davinci.utils.fileio import force_folder_to_path
from davinci.services.s3 import upload_df

context = ssl._create_unverified_context()


API_ENDPOINTS = { 
    "rb_classifier": 'https://gfyexdkgbj.execute-api.us-east-1.amazonaws.com/dev/rb/recovery/classify/?site=Jefferson',
    "gmi_fte_estimator": 'https://k7d3iztarh.execute-api.us-east-1.amazonaws.com/dev/gmi/fte/total/?p1=30000&p2=1&p3=1&c1=1&c2=1&c3=1&l1=1&l2=1000&l3=1&r1=1&r2=1&r3=5000',
    'avrl': 'https://ynajfpejo0.execute-api.us-east-1.amazonaws.com/mlpricing/avrl-sandbox/?customer_id=walmart&base_rate=1000&orig_city=CARROLLTON&orig_state=TX&dest_city=LITITZ&dest_state=PA&equipment_type=VAN&bid_hazmat_flag=N&stops=1&pickup_date=2023-12-08&delivery_date=2023-12-28&weight=16000&distance=1412&api_key=cllyiuaYNW5arTsIW6SL7HsXX4wR322g', 
 }

DATA_PATH = './.temp_data/api_warming_history_'
DATA_PATHS = {
    key: DATA_PATH + key + '.csv' for key in API_ENDPOINTS
}

def timed_query(url):
    """
    Time a web request.
    :param url: url to ping
    :type url: str
    :return: (Response Status Code (int), Total time (float))
    """
    start = time.time()
    x = urllib.request.urlopen(url, context=context)
    return x.status, time.time() - start

def update_df(code, time, df):
    """
    Update bucket counts in dataframe based on request response.
    :param code: response code
    :type code: int
    :param time: time to recieve response
    :type time: float
    :param df: DataFrame to update
    :type df: pd.DataFrame
    :return: pd.DataFrame
    """
    curr = 0
    i = 0
    while time > curr:
        curr = df.loc[i, 'ResponseLUB']
        i += 1
    if code != 200:
        i = df.shape[0]
    df.loc[i - 1, 'Count'] += 1
    df.loc[i - 1, 'Last'] = datetime.datetime.now()
    return df

def make_df():
    """
    Initialization method if dataframe cache isn't found.
    """
    buckets = [
        0.001,
        0.05,
        0.1,
        0.2,
        0.3,
        0.4,
        0.5,
        1,
        2,
        3,
        5,
        10,
        20,
        60,
        np.inf
    ]
    df = pd.DataFrame({
        'ResponseLUB': buckets,
        'Count': [0] * len(buckets),
        'Last': datetime.datetime.now(),
    })
    return df

def get_data(path):
    """
    Attempt to load our data cache.
    :param path: path to data file
    :type path: str
    :return: pd.DataFrame
    """
    force_folder_to_path(path)
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        df = make_df()
    return df

if __name__ == '__main__':
    # Load our data storage
    curr_date = datetime.datetime.now()
    midnight = curr_date.hour == 0
    first_call = math.floor(curr_date.minute / 5) == 0
    for endpoint in API_ENDPOINTS:
        url = API_ENDPOINTS[endpoint]
        data_path = DATA_PATHS[endpoint]
        df = get_data(data_path)
        try:
            response, t = timed_query(url)
            update_df(response, t, df)
            df.to_csv(data_path, index=False)
            print(f'Success on {endpoint}')
            print(response)
        except Exception as e:
            print(f'Error on {endpoint}')
            print(e)
        if midnight and first_call:
            upload_df(df, 'data/api_warming/api_warming_history_' + endpoint + '.csv')