import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from collections import defaultdict
import model_data_prep as mdp


def plot_closure_times(df, year_list):
    route_dict = defaultdict(list)
    for route in df.index:
        for year in sorted(year_list)[::-1]:
            if pd.isnull(df.loc[route]['{}LastDate'.format(year)]):
                if len(route_dict[route]) > 0:
                    route_dict[route].append(
                        df.loc[route]['{}FirstDate'.format(year + 1)])
            elif len(route_dict) > 0:
                continue
            else:
                route_dict[route].extend([df.loc[route]['{}ClosureIndicator'.format(
                    year)], df.loc[route]['{}LastDate'.format(year)]])
        if len(route_dict[route]) < 3:
            route_dict[route].append(df.loc[route]['2004FirstDate'])

    return route_dict

def


if __name__ == '__main__':
    plot_flight_timeseries('data/0408modeldata.csv')
