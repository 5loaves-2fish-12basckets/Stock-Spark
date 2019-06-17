import os
from alpha_vantage.timeseries import TimeSeries
import csv
from tqdm import tqdm
import time

def collect_data(datadir, targets):
    for symbol in tqdm(targets):
        ts = TimeSeries(key='PCAG643Q0SFCE6WI',output_format='csv')
        data, meta_data = ts.get_daily(symbol, outputsize='full')

        filepath = os.path.join(datadir, symbol+'.csv')

        with open(filepath, 'w') as f:
            writer = csv.writer(f)
            writer.writerows(data)

        time.sleep(14)

        # counter += 1
        # if counter % 5 == 0:
            # time.sleep(70)

