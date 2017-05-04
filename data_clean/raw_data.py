import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import boto
import os
import StringIO

carriers = ['AA', 'AS', 'B6', 'CO', 'DL', 'EV',
            'F9', 'HA', 'HP', 'NK', 'OO', 'UA', 'VX', 'WN', 'US']
drop_list = []


class RawDataClean(object):

    def __init__(self, filepath, year, drop_list=drop_list, ontime=True, passenger=False, carriers=carriers, nrows=None):
        self.filepath = filepath
        self.year = year
        self.ontime = ontime
        self.passenger = passenger
        self.carriers = carriers
        self.nrows = nrows
        self.drop_list = drop_list

    def load_clean_data(self):
        self.data = pd.read_csv(self.filepath, self.nrows)
        if self.ontime:
            routes = self.get_flight_routes()
            self.

    def get_flight_routes(self, data):
        '''
        OUTPUT: dictionary of every route in data and the earliest and latest
                date that a carrier flew that route in the df

        This fxn produces a dictionary that is used in create_closure_column below
        and creates the route and date column for the df
        '''
        # These are default dictionaries that have initial values of the first and last
        # day of the year of the df that are used to compare against for finding the
        # first and last day for each route in the df
        data = self.data
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

    def write_file_to_bucket(self, bucketname):
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

    def save_new_csv(filename):
        data.to_csv(filename)
