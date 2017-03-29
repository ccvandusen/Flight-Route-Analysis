import pandas as pd
import numpy as np
import feature_engineer as fe
from collections import defaultdict


def groupby_data(data):
    count = data.groupby('route').count()
    avg = data.groupby('route').mean()
    g_min = data.groupby('route').min()
    g_max = data.groupby('route').max()
    return count, avg, g_min, g_max


def create_model_variables(count, avg, g_min, g_max, year):
    year = year
    variable_dict = defaultdict(list)
    Colnames = ['{0}FlightTotal'.format(str(year)), '{0}ClosureIndicator'.format(str(year)), '{0}AvgDelay'.format(str(year)), '{0}CarrierDelay'.format(str(year)),
                '{0}WeatherDelay'.format(str(year)), '{0}NASDelay'.format(
                    str(year)), '{0}CancelledAvg'.format(str(year)),
                '{0}LateAircraftAvg'.format(str(year)), '{0}Distance'.format(str(year)), '{0}FirstDate'.format(str(year)), '{0}LastDate'.format(str(year))]

    for index_num in range(len(count)):
        # Flight totals
        variable_dict[count.index[index_num]].append(
            count.iloc[index_num]['CRSDepTime'])
    # Getting variables by averaging over all flights for a given route
    for index_num in range(len(avg)):
        # Closures
        variable_dict[avg.index[index_num]].append(
            avg.iloc[index_num]['Closure'])
        # Avg Delays
        variable_dict[avg.index[index_num]].append((avg.iloc[index_num]['ArrDelay']
                                                    + avg.iloc[index_num]['DepDelay']) / 2.)
        # Carrier Delay Average
        variable_dict[avg.index[index_num]].append(
            avg.iloc[index_num]['CarrierDelay'])
        # Weather Delay Average
        variable_dict[avg.index[index_num]].append(
            avg.iloc[index_num]['WeatherDelay'])
        # NAS delay Average
        variable_dict[avg.index[index_num]].append(
            avg.iloc[index_num]['NASDelay'])
        # Average # of cancelled flights
        variable_dict[avg.index[index_num]].append(
            avg.iloc[index_num]['Cancelled'])
        # Late Aircraft delay Average
        variable_dict[avg.index[index_num]].append(
            avg.iloc[index_num]['LateAircraftDelay'])
        # Route Distance
        variable_dict[avg.index[index_num]].append(
            avg.iloc[index_num]['Distance'])
    for index_num in range(len(g_min)):
        # First flight of year
        variable_dict[g_min.index[index_num]].append(
            g_min.iloc[index_num]['date'])
    for index_num in range(len(g_max)):
        # Last flight of year
        variable_dict[g_max.index[index_num]].append(
            g_max.iloc[index_num]['date'])
        # Transforming the dictionary into a dataframe for the model, Assigning
        # column names
    df = pd.DataFrame.from_dict(variable_dict)
    df = df.T
    df.columns = Colnames
    return df


def merge_years(df1, df2):
    merge = pd.concat([df1, df2], axis=1, join='outer')
    return merge

if __name__ == '__main__':
    df1 = fe.load_and_clean_data('../../../dev/data/2004_indicators.csv')
    count, avg, g_min, g_max = groupby_data(df1)
    model_df1 = create_model_variables(count, avg, g_min, g_max, 2004)
    model_df1.dropna(inplace=True)
    df1 = fe.load_and_clean_data('../../../dev/data/2005_indicators.csv')
    count, avg, g_min, g_max = groupby_data(df1)
    model_df2 = create_model_variables(count, avg, g_min, g_max, 2005)
    model_df2.dropna(inplace=True)
    merge_df = merge_years(model_df1, model_df2)
    merge_df.to_csv('0405modeldata.csv')
