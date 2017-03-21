import pandas as pd
import numpy as np
'''
FLIGHT EDA STUFF

'''


def read_demand_data(filepath):

    return data


def read_delay_data(filepath):
    '''
    INPUT: String of filepath to CSV filepath
    OUTPUT: pandas dataframe

    Reads in data from the Bureau of Transportation Statistics
    Data can pulled here: https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236
    '''
    flight_data = pd.read_csv(filepath)
    del flight_data['Unnamed: 36']
    return flight_data


def get_delayed_flights(delay_data):

    delayed_flights = delay_data[(delay_data['WEATHER_DELAY'] >= 0)
                                 & (delay_data['CARRIER_DELAY'] >= 0)
                                 & (delay_data['NAS_DELAY'] >= 0)
                                 & (delay_data['SECURITY_DELAY'] >= 0)
                                 & (delay_data['LATE_AIRCRAFT_DELAY'] >= 0)]\
        .dropna(subset=['WEATHER_DELAY', 'SECURITY_DELAY', 'NAS_DELAY', 'CARRIER_DELAY', 'LATE_AIRCRAFT_DELAY'])
    return delayed_flights

def


if __name__ == '__main__':
    full_df = read_delay_data('data/15flights.csv')
    delayed_flights = get_delayed_flights(full_df)
