import pandas as pd
import numpy as np

if __name__ == '__main__':
    data = pd.read_csv('../../../dev/data/0407_indicators.csv',
                       usecols=['UniqueCarrier', 'Origin', 'Dest', 'Closure', 'route', 'date'])
    summary = data.groupby('route').avg()
    print len(data), len(summary[(summary['Closure'] > 0.) & (summary['Closure'] < 1.]))
