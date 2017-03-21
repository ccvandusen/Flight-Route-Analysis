import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import random
import multiprocessing as mp
import string
from timeit import Timer

random.seed(123)

hubs = ['ORD', 'ATL', 'DFW', 'LAX', 'DEN', 'SFO', 'STL', 'EWR', 'PHX', 'PIT']
#'LGA', 'DTW', 'CLT', 'BOS', 'MSP', 'DCA', 'IAH', 'PHL', 'MEM', 'MCO']


def load_subset_data(filepath):
    data = pd.read_csv(filepath)
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
    for key in route_dates.keys():
        if route_dates[key][1] == route_dates[key][0]:
            route_dates[key] = route_dates[key] + (-1,)
        elif route_dates[key][1] > threshold:
            route_dates[key] = route_dates[key] + (1,)
        else:
            route_dates[key] = route_dates[key] + (0,)
    return route_dates


def create_closure_column(data, closure_dict):
    closure_column = []
    for index_num, route in enumerate(data['route']):
        if closure_dict[route][2] == -1:
            data.drop(data.index[index_num])
        else:
            closure_column.append(closure_dict[route][2])
    data['Closure'] = pd.Series(closure_column)
    return data


def save_new_csv(data, filename):
    pd.to_csv(path_or_buf=filename)

if __name__ == '__main__':
    please = load_subset_data('data/2004.csv')
    # routes = get_flight_routes(please, 2004)
    # work =
    # test = efficient_file_read('data/2004.csv', hubs)


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
