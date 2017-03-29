import pandas as pd
import numpy as np
from collections import defaultdict
Carriers = ['AA', 'AS', 'B6', 'CO', 'DL', 'EV',
            'F9', 'HA', 'HP', 'NK', 'OO', 'UA', 'VX', 'WN', 'US']


def load_and_clean_data(filename, subset=None):
    data = pd.read_csv(filename, nrows=subset)
    data[data['UniqueCarrier'].isin(Carriers)]
    data = data[data['Closure'].isin([0, 1])]
    data = data[data['Diverted'] == 0]
    route_dict = defaultdict(list)
    routes = []
    for route in data['route'].unique():
        route_dict[''.join(sorted(route))].append(route)
    for route in data['route']:
        routes.append(route_dict[''.join(sorted(route))][0])
    data['route'] = routes

    return data


def get_carrier_dummies(data):
    dummies_df = pd.concat(
        [data, pd.get_dummies(data['UniqueCarrier'])], axis=1)
    return dummies_df


def get_gps(data, gps_csv):
    lat_lon = pd.read_csv(gps_csv)
    origin_gps = []
    dest_gps = []
    for code in df['Origin']:
        if code not in airport_lon_lat.keys():
            origin_gps.append((0, 0))
        else:
            origin_gps.append(airport_lon_lat[code])
        for code in df['Dest']:
            if code not in airport_lon_lat.keys():
                dest_gps.append((0, 0))
            else:
                dest_gps.append(airport_lon_lat[code])
    origin_gps, dest_gps = make_airport_keys(gps_csv)
    data['Origin_gps'] = pd.Series(origin_gps)
    data['Dest_gps'] = pd.Series(dest_gps)
    return data


def indicate_hubs(data):
    '''
    INPUT: Pandas DF you want to indicate if you have hubs or not
    OUTPUT: Pandas DF with hubs column: 0 if between two non-hubs, 2 if between
    2 hubs, 1 otherwise.

    '''
    g = data[['UniqueCarrier', 'Origin', 'Dest']].groupby([
        'UniqueCarrier', 'Origin']).count()
    origin_pcts = g.groupby(level=0).apply(lambda x:
                                           100 * x / float(x.sum()))
    sorted_pct = origin_pcts['Dest'].groupby(level=0, group_keys=False)
    top_hubs = sorted_pct.nlargest(10)
    hub_dict = defaultdict(list)
    hub_indicator = []
    for index in xrange(len(top_hubs)):
        if top_hubs[index] > 5.0:
            hub_dict[top_hubs.index[index][0]].append(top_hubs.index[index][1])
    for row in data[['UniqueCarrier', 'Origin', 'Dest']].iterrows():
        if row[1]['Origin'] in hub_dict[row[1]['UniqueCarrier']]:
            if row[1]['Dest'] in hub_dict[row[1]['UniqueCarrier']]:
                hub_indicator.append(2)
                continue  # so it doesn't append twice in this if clause
            hub_indicator.append(1)
        elif row[1]['Dest'] in hub_dict[row[1]['UniqueCarrier']]:
            hub_indicator.append(1)
        else:
            hub_indicator.append(0)
    data['Hubs'] = hub_indicator
    return data


if __name__ == '__main__':
    data = load_data('data/2004_indicators.csv', subset=10000)
    new_data = indicate_hubs(data)
