import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import boto
import os
import StringIO

hubs = ['ORD', 'ATL', 'DFW', 'LAX', 'DEN', 'SFO', 'STL', 'EWR', 'PHX', 'PIT']
#'LGA', 'DTW', 'CLT', 'BOS', 'MSP', 'DCA', 'IAH', 'PHL', 'MEM', 'MCO']


def load_hub_data(filepath, subset=None):
    data = pd.read_csv(filepath, nrows=subset)

    # maj_20_hubs = data[data['Origin'].isin(hubs)]
    # maj_20_hubs.reset_index(inplace=True)
    return maj_20_hubs


def get_flight_routes(data, year):
    route_start_dates = defaultdict(lambda: pd.to_datetime(
        year * 10000 + 12 * 100 + 31, format='%Y%m%d'))
    route_end_dates = defaultdict(lambda: pd.to_datetime(
        year * 10000 + 01 * 100 + 01, format='%Y%m%d'))
    data['route'] = data['UniqueCarrier'] + ' ' + \
        data['Origin'] + ' ' + data['Dest']
    data['date'] = pd.to_datetime(
        data.Year * 10000 + data.Month * 100 + data.DayofMonth, format='%Y%m%d')
    for num, date in enumerate(data['date']):
        # import ipdb
        # ipdb.set_trace()
        if route_start_dates[data['route'][num]] >= date:
            route_start_dates[data['route'][num]] = date
        if route_end_dates[data['route'][num]] < date:
            route_end_dates[data['route'][num]] = date
    for route in route_start_dates.keys():
        if route in route_end_dates.keys():
            route_start_dates[route] = (route_start_dates[
                route], route_end_dates[route])
    return route_start_dates


def clean_data(data):
    data = data.drop(['Unnamed: 0', 'TailNum', 'Year', 'Month', 'DayOfWeek',
                      'DayofMonth', 'CancellationCode'], axis=1)
    data['date'] = pd.to_datetime(data['date'])
    return data


def create_closure_indicator(route_dates):
    threshold = pd.to_datetime('2004-12-01')
    for route in route_dates.keys():
        if route_dates[route][1] == route_dates[route][0]:
            route_dates[route] = route_dates[route] + (-1,)
        elif route_dates[route][1] < threshold:
            route_dates[route] = route_dates[route] + (1,)
        else:
            route_dates[route] = route_dates[route] + (0,)
    return route_dates


def create_closure_column(data, closure_dict):
    closure_column = []
    for route in data['route']:
        closure_column.append(closure_dict[route][2])
    data['Closure'] = pd.Series(closure_column)
    for index_num, indicator in enumerate(data['Closure']):
        index_list = []
        if indicator == -1:
            index_list.append(index_num)
    data.drop(data.index[index_list], inplace=True)
    return data


def save_new_csv(data, filename):
    data.to_csv(filename)


def write_file_to_bucket(bucketname, data, filepath):
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
    original_df = load_hub_data('../../../dev/2004.csv')
    routes = get_flight_routes(original_df, 2004)
    closure_dict = create_closure_indicator(routes)
    new_data = create_closure_column(original_df, closure_dict)
    new_data = clean_data(new_data)
    save_new_csv(new_data, '2004_indicators.csv')
    write_file_to_bucket('flight-final-project',
                         new_data, '2004_indicators.csv')
