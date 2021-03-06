import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import boto
import os
import StringIO

'''
This script takes in a csv of one year (or less) of data from the
BTS on time performance database and creates a route column,
a route closure label column, and a datetime column.
'''

# These are the carriers I'm tracking
Carriers = ['AA', 'AS', 'B6', 'CO', 'DL', 'EV',
            'F9', 'HA', 'HP', 'NK', 'OO', 'UA', 'VX', 'WN', 'US']


def load_data(filepath, subset=None):
    '''
    INPUT:  STR - string for filepath to the csv filename
            INT - # of rows you want to read in - default None (reads in all)

    I think you can figure out what's going on here
    '''

    data = pd.read_csv(filepath, nrows=subset)
    return data


def get_flight_routes(data, year):
    '''
    INPUT: PANDAS DF from load_data fxn, this fxn is specific to BTS data
    OUTPUT: dictionary of every route in data and the earliest and latest
            date that a carrier flew that route in the df

    This fxn produces a dictionary that is used in create_closure_column below
    and creates the route and date column for the df
    '''
    # These are default dictionaries that have initial values of the first and last
    # day of the year of the df that are used to compare against for finding the
    # first and last day for each route in the df
    route_start_dates = defaultdict(lambda: pd.to_datetime(
        year * 10000 + 12 * 100 + 31, format='%Y%m%d'))

    route_end_dates = defaultdict(lambda: pd.to_datetime(
        year * 10000 + 1 * 100 + 1, format='%Y%m%d'))

    # Creating a new route column in the df
    data['route'] = data['UNIQUE_CARRIER'] + \
        ' ' + data['ORIGIN'] + ' ' + data['DEST']

    # Creating a date column in the df
    data['date'] = pd.to_datetime(
        data.YEAR * 10000 + data.MONTH * 100 + data.DAY_OF_MONTH, format='%Y%m%d')

    # Testing the dates for each route
    for num, date in enumerate(data['date']):

        if route_start_dates[data['route'][num]] >= date:
            route_start_dates[data['route'][num]] = date

        if route_end_dates[data['route'][num]] < date:
            route_end_dates[data['route'][num]] = date

    # Merging the start and end dictionaries
    for route in route_start_dates.keys():
        if route in route_end_dates.keys():
            route_start_dates[route] = (route_start_dates[
                route], route_end_dates[route])

    return route_start_dates


def remove_columns(data, drop_list, convert_date=True):
    '''
    INPUT:  pandas df (can work with any csv, woo!)
            LIST of STR: list of column names from df that you want to drop
            BOOL: whether you want to convert the date column created in
                  get_flight_routes fxn to a datetime object or not.
    OUTPUT: pandas df w/ dropped columns
    '''

    # Dropping extra columns
    data.drop(drop_list, axis=1, inplace=True)

    # Converts date column to datetime object if arg is True
    if convert_date:
        data['date'] = pd.to_datetime(data['date'])
    return data


def create_closure_column(route_dates, data, year):
    '''
    INPUT: DICT: route start and end dates, created from get_flight_routes fxn
           PANDAS DF: data to create indicators for
           INT: year of the df.
    OUTPUT: new pandas df with route closure label column.

    The labels are later filtered and quality controlled in model_data_prep script
    '''

    def create_closure_indicator(route_dates, year):
        '''
        INPUT: DICT: route start and end dates
               INT: year the data describes
        OUTPUT: dictionary with closure indicators for each route
        '''

        # If most recent flight is before this threshold, route is
        # considered closed
        threshold = pd.to_datetime('{}-12-01'.format(str(year)))

        # This loop adds closure indicators : -1 = not a route, 0 = open,
        # 1 = closed
        for route in route_dates.keys():
            first_flight = route_dates[route][0]
            last_flight = route_dates[route][1]

            if last_flight == first_flight:
                route_dates[route] = route_dates[route] + (-1,)

            elif last_flight < threshold:
                route_dates[route] = route_dates[route] + (1,)

            else:
                route_dates[route] = route_dates[route] + (0,)

        return route_dates
    # Using function to create a dictionary of closure indicators
    closure_dict = create_closure_indicator(route_dates, year)

    # Creating a list of closure indicators to add to the pandas df
    closure_column = []
    for route in data['route']:
        closure_column.append(closure_dict[route][2])

    # Creating new columns for pandas df
    data['Closure'] = pd.Series(closure_column)
    data['{}FirstFlight'.format(str(year))]
    data['{}LastFlight'.format(str(year))]

    # Dropping rows with Closure value of -1
    data = data[data.Closure != -1]

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


def clean_raw_data(filepath, filename, year, drop_list, subset=None):
    '''
    INPUT: STR: directory filepath
           STR: name of file you want writted
           INT: the year of the data
           LIST: list of columns from data that you don't want
           INT: subset of rows of csv you want to clean
    OUTPUT: Pandas Df
    run this function to take the csv and return a cleaned, labelled
    df
    '''
    original_df = load_data(filepath, subset)
    routes = get_flight_routes(original_df, year)
    labeled_data = create_closure_column(routes, original_df, year)
    save_new_csv(labeled_data, filename)
    write_file_to_bucket('flight-final-project',
                         labeled_data, filename)

    return labeled_data


if __name__ == '__main__':
    drop_list = ['YEAR', 'MONTH', 'DAY_OF_MONTH']
    clean_raw_data('data/2009.csv', '2009_indicators.csv', 2009, drop_list)
