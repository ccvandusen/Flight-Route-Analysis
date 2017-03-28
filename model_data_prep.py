import pandas as pd
import numpy as np
import feature_engineer as fe


def groupby_data(data):
    count_groupby = data.groupby('route').count()
    avg_groupby = data.groupby('route').mean()
    min_groupby = data.groupby('route').min()
    return count_groupby, avg_groupby, min_groupby


def create_model_variables(count, avg, min, year):
    variable_dict = defaultdict(list)
    # This key will be used to assign colnames
    variable_dict['Colnames'].extend(('{0}FlightTotal'.format(year), '{0}ClosureIndicator'.format(year), '{0}AvgDelay'.format(year), '{0}CarrierDelay'.format(year),
                                      '{0}WeatherDelay'.format(year), '{0}NASDelay'.format(
                                          year), '{0}CancelledAvg'.format(year),
                                      '{0}LateAircraftAvg'.format(year), '{0}Distance'.format(year)))
    # Getting variables by counting # of instances of given column
    for index_num in range(len(count_groupby)):
        # Flight totals
        variable_dict[count_groupby.index[index_num]].append(
            ('{0}FlightTotal'.format(year), count_groupby.iloc[index_num]['CRSDepTime']))
    # Getting variables by averaging over all flights for a given route
    for index_num in range(len(avg_groupby)):
        # Closures
        variable_dict[avg_groupby.index[index_num]].append(
            ('{0}ClosureIndicator'.format(year), avg_groupby.iloc[index_num]['Closure']))
        # Avg Delays
        variable_dict[avg_groupby.index[index_num]].append(('{0}AvgDelay'.format(year), (avg_groupby.iloc[index_num]['ArrDelay']
                                                                                         + avg_groupby.iloc[index_num]['DepDelay']) / 2.))
        # Carrier Delay Average
        variable_dict[avg_groupby.index[index_num]].append(
            ('{0}CarrierDelay'.format(year), avg_groupby.iloc[index_num]['CarrierDelay']))
        # Weather Delay Average
        variable_dict[avg_groupby.index[index_num]].append(
            ('{0}WeatherDelay'.format(year), avg_groupby.iloc[index_num]['WeatherDelay']))
        # NAS delay Average
        variable_dict[avg_groupby.index[index_num]].append(
            ('{0}NASDelay'.format(year), avg_groupby.iloc[index_num]['NASDelay']))
        # Average # of cancelled flights
        variable_dict[avg_groupby.index[index_num]].append(
            ('{0}CancelledAvg'.format(year), avg_groupby.iloc[index_num]['Cancelled']))
        # Late Aircraft delay Average
        variable_dict[avg_groupby.index[index_num]].append(('{0}LateAircraftAvg'.format(
            year), avg_groupby.iloc[index_num]['LateAircraftDelay']))
    #
    for index_num in range(len(min_groupby)):
        # Route Distance
        variable_dict[min_groupby.index[index_num]].append(
            ('{0}Distance'.format(year), min_groupby.iloc[index_num]['Distance']))
    # Transforming the dictionary into a dataframe for the model, Assigning
    # column names
    df = pd.DataFrame.from_dict(variable_dict)
    df = df.T
    df.columns = df.loc['Colnames']
    return df


def create_df_from_dict(variable_dict):


if __name__ == '__main__':
    df = fe.load_and_clean_data('../../../dev/data/2004_indicators.csv')
