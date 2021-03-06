import pandas as pd
import numpy as np
import feature_engineer as fe
from collections import defaultdict
import Raw_Data_Clean_Passenger as dcp


def groupby_data(data):
    '''
    INPUT: Pandas DF
    OUTPUT: Groupbys for use with data cleaning

    This fxn is used in other fxns in this script
    '''
    count = data.groupby('route').count()
    avg = data.groupby('route').mean()
    g_min = data.groupby('route').min()
    g_max = data.groupby('route').max()
    return count, avg, g_min, g_max


def create_ontime_model_variables(count, avg, g_min, g_max, year):
    '''
    INPUT: PANDAS DF: count, min, max, and average groupbys from the
                      cleaned up on time yearly data from Raw_Data_Clean fxn
           INT: year of the data to be aggregated
    OUTPUT: dataframe to be read in to the model. Still needs to be quality checked
            by the fix_conflicts function

    This function takes the groupby data and creates a dictionary with keys for each
    route and values of the various aggregated data:
    Flight total,
    '''

    # Initializing the dictionary that stores all the values for the new df
    # per route.
    variable_dict = defaultdict(list)

    # These are the column names of the returned dataframe from this function
    Colnames = ['{0}FlightTotal'.format(str(year)), '{0}CRSElapsedTime'.format(str(year)),
                '{0}ActualElapsedTime'.format(
                    str(year)), '{0}ClosureIndicator'.format(str(year)),
                '{0}AvgDelay'.format(
                    str(year)), '{0}CarrierDelay'.format(str(year)),
                '{0}WeatherDelay'.format(str(year)), '{0}NASDelay'.format(
                    str(year)), '{0}CancelledAvg'.format(str(year)),
                '{0}LateAircraftAvg'.format(
                    str(year)), '{0}Distance'.format(str(year)),
                '{0}FirstDate'.format(str(year)), '{0}LastDate'.format(str(year))]

    '''
    Adding all the variables from the various groupbys into
    the dictionary
    '''
    # the data I have from past 2008 has different column names
    if year > 2008:
        for index_num in range(len(count)):
            # Flight totals
            variable_dict[count.index[index_num]].append(
                count.iloc[index_num]['CRS_DEP_TIME'])
        for index_num in range(len(avg)):
            variable_dict[avg.index[index_num]].extend(
                [avg.iloc[index_num]['CRS_ELAPSED_TIME'],
                 avg.iloc[index_num]['ACTUAL_ELAPSED_TIME'],
                 avg.iloc[index_num]['Closure'],
                 ((avg.iloc[index_num]['ARR_DELAY'] +
                   avg.iloc[index_num]['DEP_DELAY']) / 2.),
                 avg.iloc[index_num]['CARRIER_DELAY'],
                 avg.iloc[index_num]['WEATHER_DELAY'],
                 avg.iloc[index_num]['NAS_DELAY'],
                 avg.iloc[index_num]['CANCELLED'],
                 avg.iloc[index_num]['LATE_AIRCRAFT_DELAY'],
                 avg.iloc[index_num]['DISTANCE']])
        for index_num in range(len(g_min)):
            variable_dict[g_min.index[index_num]].append(
                g_min.iloc[index_num]['date'])
        for index_num in range(len(g_max)):
            variable_dict[g_max.index[index_num]].append(
                g_max.iloc[index_num]['date'])

    else:
        for index_num in range(len(count)):
            # Flight totals
            variable_dict[count.index[index_num]].append(
                count.iloc[index_num]['CRSDepTime'])
        for index_num in range(len(avg)):
            variable_dict[avg.index[index_num]].extend(
                [avg.iloc[index_num]['CRSElapsedTime'],
                 avg.iloc[index_num]['ActualElapsedTime'],
                 avg.iloc[index_num]['Closure'],
                 ((avg.iloc[index_num]['ArrDelay'] +
                   avg.iloc[index_num]['DepDelay']) / 2.),
                 avg.iloc[index_num]['CarrierDelay'],
                 avg.iloc[index_num]['WeatherDelay'],
                 avg.iloc[index_num]['NASDelay'],
                 avg.iloc[index_num]['Cancelled'],
                 avg.iloc[index_num]['LateAircraftDelay'],
                 avg.iloc[index_num]['Distance']])
        for index_num in range(len(g_min)):
            variable_dict[g_min.index[index_num]].append(
                g_min.iloc[index_num]['date'])
        for index_num in range(len(g_max)):
            variable_dict[g_max.index[index_num]].append(
                g_max.iloc[index_num]['date'])

    # Transforming the dictionary into a dataframe for the model, Assigning
    # column names
    df = pd.DataFrame.from_dict(variable_dict)
    df = df.T
    df.columns = Colnames
    df['{}BlockTime'.format(year)] = df['{}CRSElapsedTime'.format(
        year)] - df['{}ActualElapsedTime'.format(year)]
    # Lost 3 routes in 2004 this step, is ok (from some 2200)
    df.dropna(inplace=True)

    return df


def create_passenger_model_variables(data, ontime_routes, year):
    data = data[data['route'].isin(ontime_routes)]
    passenger_data = data.groupby('route').sum()

    passenger_variables = defaultdict(list)
    for index_num in range(len(passenger_data)):
        passenger_variables[passenger_data.index[index_num]].extend\
            ((passenger_data.iloc[index_num][0],
              passenger_data.iloc[index_num][1]))

    df = pd.DataFrame.from_dict(passenger_variables)
    df = df.T
    df.columns = ['{}Seats'.format(year), '{}Passengers'.format(year)]
    df['{}fill_pct'.format(year)] = df['{}Passengers'.format(
        year)] / df['{}Seats'.format(year)]

    return df


def merge(merge_list):
    merge = pd.concat(merge_list, axis=1, join='outer')
    return merge


def fix_conflicts(model_data, year_list):
    '''
    INPUT:  PANDAS DF: has aggregated by create_model_variables
            LIST: list of years that is contained within the given dataframe
    OUTPUT: PANDAS DF:  changed many ClosureIndicator values to fix conflicts from
                        year to year that occur when the column was created in Data_Clean

    Note: YEAR LIST MUST BE INTS!!!!!

    Many conflicts arose when assigning whether a route was closed by a Carrier
    or not from Data_Clean create_closure_column fxn. These are things like
    seasonal routes, routes that stopped service after the end of a year,
    routes that didn't run in the December of the previous year
    (yet then ran again the next year), and others. This fxn adresses many of These
    issues. As of now 9 route indicators have conflicts I couldn't figure out why.
    I removed those because time and small data loss.
    '''

# we take out last year in year_list this because most checks occur between
# the given year and the next, so we check year against year + 1
    for year in year_list[:-1]:

        # collecting all routes that are indicated as open for a particular
        # year. We do the same for closed below
        open_routes = model_data[model_data[
            '{}ClosureIndicator'.format(str(year))] == 0]

        # These next two tests work because when we originally made the data we dropped all routes
        # that contained any null values (there weren't very many)
        wrong_open_list = []
        wrong_closed_list = []  # these lists are used later
        for route in open_routes.index:
            # this says if random variable from next year is null (i.e. that route didn't
            # appear in the given year) we change the route indicator to closed from the
            # originally incorrect open indicator
            if np.isnan(model_data.loc[route]['{}CarrierDelay'.format(str(year + 1))]):
                model_data.set_value(
                    route, '{}ClosureIndicator'.format(str(year)), 1)
                wrong_open_list.append(route)
            else:
                continue
        # Creating list of indicated closed routes. We do this after the last for
        # loop because new closed route indicators are added from the loop.
        closed_routes = model_data[model_data[
            '{}ClosureIndicator'.format(str(year))] == 1]

        for route in closed_routes.index:
            # same thing as above but if we indicated that a route is open if it appears
            # the next year and we accidentally indicated it was closed, we perform more
            # checks on these later because there are far fewer closed routes and we really
            # really really want to be sure about these
            if np.isnan(model_data.loc[route]['{}CarrierDelay'.format(str(year + 1))]):
                continue
            else:
                wrong_closed_list.append(route)

        # Creating a threshold to check final flight dates
        threshold = pd.to_datetime('{}-12-01'.format(str(year + 1)))
        # Creating a new seasonal variable to indicate seasonal routes
        model_data['Seasonal'] = 0
        for route in wrong_closed_list:

            delta = (pd.to_datetime(model_data.loc[route][
                '{}LastDate'.format(str(year))]) - pd.to_datetime(model_data.loc[route]['{}LastDate'.format(str(year + 1))]))

            delta2 = (pd.to_datetime(model_data.loc[route]['{}FirstDate'.format(str(
                year))]) - pd.to_datetime(model_data.loc[route]['{}FirstDate'.format(str(year + 1))]))
            # I haven't checked why, but this if/else section doesn't work on exactly one
            # row. Hence try/except. I don't know why and haven't checked, so removed it for now.
            # TODO: check why!!
            try:
                if 30 > abs(abs(delta.days) - 365):
                    # Checking seaonality with this first if clause. For each delta,
                    # if routes start and end within 30 days of the previous years start and end,
                    # then they are seasonal.
                    if 30 > abs(abs(delta2.days) - 365):
                        model_data.set_value(
                            route, 'Seasonal', 1)
                        model_data.set_value(
                            route, '{}ClosureIndicator'.format(str(year)), 0)
                        model_data.set_value(
                            route, '{}ClosureIndicator'.format(str(year + 1)), 0)
                else:
                    # Performing a check to see if indicated closed route
                    if pd.to_datetime(model_data.loc[route]['{}LastDate'.format(str(year + 1))]) < threshold:
                        model_data.set_value(
                            route, '{}ClosureIndicator'.format(str(year)), 0)
                        model_data.set_value(
                            route, '{}ClosureIndicator'.format(str(year + 1)), 0)
                    else:
                        # Don't change value of current year here because it still ran during the following
                        # year, so it didn't actually close on the previous
                        # year.
                        model_data.set_value(
                            route, '{}ClosureIndicator'.format(str(year + 1)), 1)
            except:
                continue

    for year in year_list:
        for route in model_data[(model_data['{}ClosureIndicator'.format(str(year))] > 0)
                                & (model_data['{}ClosureIndicator'.format(str(year))] < 1)].index:
            if model_data.loc[route]['{}ClosureIndicator'.format(str(year))] > 52:
                model_data.set_value(
                    route, '{}ClosureIndicator'.format(str(year)), 0)
            else:
                model_data.set_value(
                    route, '{}ClosureIndicator'.format(str(year)), 1)

    # model_data.set_index('Unnamed: 0', inplace=True)
    return model_data


def year_slice(data, year_list):
    final_list = []
    merge_data_list = []

    for route in data.index:
        final_slice = [route]
        colnames = ['route']

        for year in sorted(year_list)[::-1]:

            if np.isnan(data.loc[route]['{}WeatherDelay'.format(year)]):
                continue

            else:
                for num, column in enumerate(list(data.columns)):

                    if str(year) in column:
                        final_slice.append(column)
                        colnames.append(column[4:])
                    if num > 88:
                        final_slice.append(column)
                        colnames.append(column)

                final_slice.append('Seasonal')
                colnames.append('Seasonal')
                final_list.append(final_slice)
                break

    for column_list in final_list:
        mini_df = data.loc[column_list[0]][column_list]
        mini_df = mini_df.to_frame()
        mini_df = mini_df.T
        mini_df.set_value(mini_df.columns[0], mini_df.columns[
                          0], mini_df.columns[0])
        mini_df.columns = colnames
        merge_data_list.append(mini_df)

    sliced_data = pd.concat(merge_data_list, axis=0, join='outer')
    for route in sliced_data.index:
        if data.loc[route]['UniqueCarrier'] == 'HP':
            if pd.to_datetime(data.loc[route]['2005LastDate']) > pd.to_datetime('2005-12-25'):
                data.set_value(route, '2005ClosureIndicator', 0)
    return sliced_data


def create_model_prepped_data(ontime_filepath_list, passenger_filepath_list, year_list):
    '''
    Read in the data created from the Raw_Data fxns and it will spit out data ready to be
    modelled.
    '''
    ontime_df_list = []
    for index, filename in enumerate(ontime_filepath_list):
        ontime_df_list.append(
            fe.load_and_clean_ontime_data(filename, year_list[index]))
    passenger_df_list = []
    for filename in passenger_filepath_list:
        passenger_df_list.append(dcp.load_and_clean_passenger_data(filename))

    merge_list = []
    if len(ontime_df_list) == 1:
        count, avg, g_min, g_max = groupby_data(ontime_df_list[0])

        ontime_stage_1 = create_ontime_model_variables(
            count, avg, g_min, g_max, year_list[0])

        ontime_routes = ontime_stage_1.index

        passenger_stage_1 = create_passenger_model_variables(
            passenger_df_list[0], ontime_routes, year_list[0])

        total_variable_list = merge([passenger_stage_1, ontime_stage_1])

        return total_variable_list

    elif len(ontime_df_list) > 1:

        for index in range(len(ontime_df_list)):
            count, avg, g_min, g_max = groupby_data(ontime_df_list[index])

            ontime_stage_1 = create_ontime_model_variables(
                count, avg, g_min, g_max, year_list[index])

            ontime_routes = ontime_stage_1.index

            passenger_stage_1 = (create_passenger_model_variables(
                passenger_df_list[index], ontime_routes, year_list[index]))

            total_variable_list = merge([passenger_stage_1, ontime_stage_1])

            merge_list.append(total_variable_list)

        merged_df = merge(merge_list)

        prepped_data = fix_conflicts(merged_df, year_list)

        Carrier = []
        Origin = []
        Dest = []
        for split_route in prepped_data.index.str.split(' '):
            Carrier.append(split_route[0])
            Origin.append(split_route[1])
            Dest.append(split_route[2])
        prepped_data['UniqueCarrier'] = Carrier
        prepped_data['Origin'] = Origin
        prepped_data['Dest'] = Dest

        return prepped_data

    else:
        # print 'U WOT MATE'
        return None

if __name__ == '__main__':
    DATA = create_model_prepped_data(['data/Ontime/2004_indicators.csv',
                                      'data/Ontime/2005_indicators.csv',
                                      'data/Ontime/2006_indicators.csv',
                                      'data/Ontime/2007_indicators.csv',
                                      'data/Ontime/2008_indicators.csv'],
                                     ['data/Passengers/2004passengers.csv',
                                      'data/Passengers/2005passengers.csv',
                                      'data/Passengers/2006passengers.csv',
                                      'data/Passengers/2007passengers.csv',
                                      'data/Passengers/2008passengers.csv'], range(2004, 2009))
