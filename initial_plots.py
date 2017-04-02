import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
import model_data_prep as mdp


def plot_flight_timeseries(filename):
    routes = pd.read_csv(filename)
    data = mdp.year_slice(routes, range(2004, 2009))
    X = data['FlightTotal']
    plt.hist(X, bins=50)
    plt.show()

    return  # something should go here? maybe not I just want to plot


if __name__ == '__main__':
    plot_flight_timeseries('data/0408modeldata.csv')
