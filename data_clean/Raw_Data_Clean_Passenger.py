import pandas as pd
import numpy as np
from collections import defaultdict
import Raw_Data_Clean_Ontime as rdco
import datetime
import boto
import os
import StringIO

'''
This script takes a csv of one year (or less) from the
BTS market data and returns a pandas dataframe ready to be
merged with the ontime data and trained by the model
'''

Carriers = ['AA', 'AS', 'B6', 'CO', 'DL', 'EV',
            'F9', 'HA', 'HP', 'NK', 'OO', 'UA', 'VX', 'WN', 'US']


def load_and_clean_passenger_data(filepath, subset=None):
    '''
    INPUT: STR: filepath to csv
           INT: subset of data you want to read in, default None
    OUTPUT: cleaned dataframe with a avg_fill, route, and date column engineered
    '''

    # These are the types of aircraft used, the commercial passenger classes
    Classes = ['A', 'C', 'E', 'F']
    # Reading in file
    data = pd.read_csv(filepath, nrows=subset)
    # filtering out rows that don't have the classes/carriers I want
    data = data[data['UNIQUE_CARRIER'].isin(Carriers)]
    data = data[data['CLASS'].isin(Classes)]

    # Creating a route column
    data['route'] = data['UNIQUE_CARRIER'] + \
        ' ' + data['ORIGIN'] + ' ' + data['DEST']

    # this block resolves route duplicates - so I don't have a route
    # like US LAX JFK and a route like US JFK LAX, as they are the same route
    data.sort_values(by=['ORIGIN', 'UNIQUE_CARRIER', 'DEST'], inplace=True)
    route_dict = defaultdict(list)
    routes = []
    # Putting duplicates under one key
    for route in data['route'].unique():
        route_dict[''.join(sorted(route))].append(route)
    # Making a route list with only one route name
    for route in data['route']:
        routes.append(route_dict[''.join(sorted(route))][0])

    # Creating new columns for the data
    data['route'] = routes
    data['date'] = pd.to_datetime(
        data.YEAR * 10000 + data.MONTH * 100 + 1, format='%Y%m%d')
    data['avg_fill'] = data['PASSENGERS'] / data['SEATS']

    return data


if __name__ == '__main__':
    ontime_data = load_and_clean_passenger_data('2006passenger.csv')
    rdco.save_new_csv(ontime_data, '2006_passengers.csv')
    rdco.write_file_to_bucket('flight-final-project',
                              new_data, '2006_passengers.csv')
