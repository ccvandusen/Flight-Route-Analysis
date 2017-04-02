import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import boto
import os
import StringIO

'''
This script takes a csv of one year (or less) from the
BTS market segment data and returns a pandas dataframe ready to be
merged with the ontime data and used in the model
'''

Carriers = ['AA', 'AS', 'B6', 'CO', 'DL', 'EV',
            'F9', 'HA', 'HP', 'NK', 'OO', 'UA', 'VX', 'WN', 'US']


def load_and_clean_passenger_data(filename, subset=None):
    Classes = ['A', 'C', 'E', 'F']
    data = pd.read_csv(filename, nrows=subset)
    data = data[data['UNIQUE_CARRIER'].isin(Carriers)]
    data = data[data['CLASS'].isin(Classes)]
    data['route'] = data['UNIQUE_CARRIER'] + \
        ' ' + data['ORIGIN'] + ' ' + data['DEST']
    data.sort_values(by=['ORIGIN', 'UNIQUE_CARRIER', 'DEST'], inplace=True)
    route_dict = defaultdict(list)
    routes = []
    for route in data['route'].unique():
        route_dict[''.join(sorted(route))].append(route)
    for route in data['route']:
        routes.append(route_dict[''.join(sorted(route))][0])
    data['route'] = routes
    data['date'] = pd.to_datetime(
        data.YEAR * 10000 + data.MONTH * 100 + 1, format='%Y%m%d')
    data['avg_fill'] = data['PASSENGERS'] / data['SEATS']
    return data


def save_new_csv(data, filename):
    data.to_csv(filename)


def write_file_to_bucket(bucketname, data, filepath):
    '''
    INPUT: STR, Pandas dataframe, String
    Reads in a pandas dataframe to the specified S3 bucket.
    The filepath is the name given to the file in S3
    '''
    # Get connection to bucket
    access_key, access_secret_key = os.environ[
        'AWS_ACCESS_KEY'], os.environ['AWS_SECRET_ACCESS_KEY']
    conn = boto.connect_s3(access_key, access_secret_key)
    conn = conn.get_bucket(bucketname)
    # Write dataframe to buffer
    csv_buffer = StringIO.StringIO()
    data.to_csv(csv_buffer, index=True)

    # Upload CSV to S3
    file_object = conn.new_key(filepath)
    file_object.set_contents_from_string(csv_buffer.getvalue())


if __name__ == '__main__':
    original_df = load_data('../../../dev/2004.csv')
    routes = get_flight_routes(original_df, 2004)
    new_data = create_closure_column(routes, original_df, 2004)
    new_data = clean_data(new_data)
    save_new_csv(new_data, '2004_indicators.csv')
    write_file_to_bucket('flight-final-project',
                         new_data, '2004_indicators.csv')
