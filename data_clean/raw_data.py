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
        data = pd.read_csv(self.filepath, self.nrows)
        if self.ontime:
            routes = self.get_flight_routes(data)
            data = self.create_closure_column(routes, data)
        if self.passenger:
            data = self.clean_passenger_data(data)
        self.data = data

    def clean_passenger_data(data):
        '''
        INPUT: PANDAS DF: passenger load data from BTS
        OUTPUT: cleaned dataframe with a avg_fill, route, and date column engineered
        '''

        # These are the types of aircraft used, the commercial passenger
        # classes
        Classes = ['A', 'C', 'E', 'F']

        # filtering out rows that don't have the classes/carriers I want
        data = data[data['UNIQUE_CARRIER'].isin(Carriers)]
        data = data[data['CLASS'].isin(Classes)]

        # Creating a route column
        data['route'] = data['UNIQUE_CARRIER'] + \
            ' ' + data['ORIGIN'] + ' ' + data['DEST']

        # this block resolves route duplicates - so I don't have a route
        # like US LAX JFK and a route like US JFK LAX, as they are the same
        # route
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

    def create_closure_column(self, route_dates, data):
        '''
        INPUT: DICT: route start and end dates, created from get_flight_routes fxn
               PANDAS DF: data to create indicators for
        OUTPUT: new pandas df with route closure label column.

        The labels are later filtered and quality controlled in model_data_prep script
        '''
        year = self.year

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
