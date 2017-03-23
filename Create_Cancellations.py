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
    maj_20_hubs = data[data['Origin'].isin(hubs)]
    maj_20_hubs.reset_index(inplace=True)
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


def create_closure_indicator(route_dates):
    threshold = pd.to_datetime('2004-12-24')
    test = []
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
    for index_num, route in enumerate(data['route']):
        if closure_dict[route][2] == -1:
            data.drop(data.index[index_num])
        else:
            closure_column.append(closure_dict[route][2])
    data['Closure'] = pd.Series(closure_column)
    del data['index']
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
    original_df = load_hub_data('data/2004.csv', subset=100000)
    routes = get_flight_routes(original_df, 2004)
    closure_dict = create_closure_indicator(routes)
    new_data = create_closure_column(original_df, closure_dict)
    save_new_csv(new_data, 'test.csv')
    #write_file_to_bucket('flight-final-project', new_data, 'test.csv')


# def efficient_file_read(filepath, hubs):
#     '''
#     INPUT: filepath to csv
#     OUTPUT: dict that contains first date and last date for each flight
#
#     The data is indexed with integers, see README for info on what index represents
#     what
#     '''
#     route_start_dates = defaultdict(lambda: pd.to_datetime(
#         '12/31/1988', infer_datetime_format=True))
#     route_end_dates = defaultdict(lambda: pd.to_datetime(
#         '01/01/1988', infer_datetime_format=True))
#     with open(filepath) as f:
#         for line in f:
#             features = line.split(',')
#             if features[16] in hubs:
#                 if features[17] in hubs:
#                     if features[0] == 'Year':
#                         continue
#                     # + ' ' + features[5] + ' ' + features[7]
#                     route = features[8] + ' ' + \
#                         features[16] + ' ' + features[17]
#                     date = features[1] + "/" + features[2] + "/" + features[0]
#                     date = pd.to_datetime(date, infer_datetime_format=True)
#                     if route_start_dates[route] >= date:
#                         route_start_dates[route] = date
#                     elif route_end_dates[route] < date:
#                         route_end_dates[route] = date
#     for route in route_start_dates.keys():
#         if route in route_end_dates.keys():
#             route_start_dates[route] = (route_start_dates[
#                 route], route_end_dates[route])
#     return route_start_dates

# def route_start_dates
