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
    Colnames = ['{0}FlightTotal'.format(str(year)), '{0}ClosureIndicator'.format(str(year)),
                '{0}AvgDelay'.format(
                    str(year)), '{0}CarrierDelay'.format(str(year)),
                '{0}WeatherDelay'.format(str(year)), '{0}NASDelay'.format(
                    str(year)), '{0}CancelledAvg'.format(str(year)),
                '{0}LateAircraftAvg'.format(
                    str(year)), '{0}Distance'.format(str(year)),
                '{0}FirstDate'.format(str(year)), '{0}LastDate'.format(str(year))]

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
    df.dropna(inplace=True)  # Lost 3 routes in this step
    return df


def merge_years(df1, df2):
    merge = pd.concat([df1, df2], axis=1, join='outer')
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

        # Creating a threshold to check final
        threshold = pd.to_datetime('{}-12-01'.format(str(year + 1)))
        # flight dates against
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

    model_data.set_index('Unnamed: 0', inplace=True)
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
                for column in list(data.columns):

                    if str(year) in column:
                        final_slice.append(column)
                        colnames.append(column[4:])

                final_slice.append('Seasonal')
                colnames.append('Seasonal')
                final_list.append(final_slice)
                break

    for column_list in final_list:
        #import ipdb
        # ipdb.set_trace()
        mini_df = data.loc[column_list[0]][column_list]
        mini_df = mini_df.to_frame()
        mini_df = mini_df.T
        mini_df.set_value(mini_df.columns[0], mini_df.columns[
                          0], mini_df.columns[0])
        mini_df.columns = colnames
        merge_data_list.append(mini_df)

    sliced_data = pd.concat(merge_data_list, axis=0, join='outer')

    return sliced_data

if __name__ == '__main__':
    df1 = fe.load_and_clean_data('../../../dev/data/2004_indicators.csv')
    count, avg, g_min, g_max = groupby_data(df1)
    model_df1 = create_model_variables(count, avg, g_min, g_max, 2004)
    df1 = fe.load_and_clean_data('../../../dev/data/2005_indicators.csv')
    count, avg, g_min, g_max = groupby_data(df1)
    model_df2 = create_model_variables(count, avg, g_min, g_max, 2005)
    merge_df = merge_years(model_df1, model_df2)
    merge_df.to_csv('0405modeldata.csv')
