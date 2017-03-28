import pandas as pd
import numpy as np

if __name__ == '__main__':
    data = pd.read_csv('data/2004_indicators.csv',
                       usecols=['UniqueCarrier', 'Origin', 'Dest', 'Closure', 'route', 'date'], nrows=100000)
    summary = data.groupby('route').mean()
    print len(data), len(summary[(summary['Closure'] > 0.) & (summary['Closure'] < 1.)])
