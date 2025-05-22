import requests
import pandas  as pd 
from sqlalchemy import create_engine
from datetime import datetime
import os 

API_KEY= 'ZF6LX9OFTSKWJ4L1'
STOCK_SYMBOL='AAPL'
DB_USER='airflow'
DB_PASSWORD='airflow'
DB_HOST='host.docker.internal'
DB_PORT='5432'
DB_NAME='stocks'
TABLE_NAME='raw_stock_data'


#### DATA EXTRACTION #####

def fetch_stock_data():
    url = f'https://www.alphavantage.co/query'
    params = {
        'function':'TIME_SERIES_INTRADAY',
        'symbol':STOCK_SYMBOL,
        'interval':'60min',
        'apikey': API_KEY,
        'outputsize':'compact'
    }

    print('Fetching data from Alpha Vantage....')

    response = requests.get(url,params=params)
    data= response.json()

    #### PARSE RESPONSE

    time_series =data.get('Time Series (60min)',{})
    records=[]

    for timestamp, values in time_series.items():
        records.append({
            'timestamp': timestamp,
            'open': float(values['1. open']),
            'high': float(values['2. high']),
            'low': float(values['3. low']),
            'close': float(values['4. close']),
            'volume': int(values['5. volume'])
        })

    df =pd.DataFrame(records)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values('timestamp',inplace=True)

    return df
    

    ##### LOAD DATA #####
def load_to_postgres(df):
    engine= create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    print('Loading Data into PostgresSQL..')
    df.to_sql(TABLE_NAME, engine, if_exists='replace',index=False)
    print('Done')

####### Main
if __name__=='__main__':
    df=fetch_stock_data()
    print(df.head())
    load_to_postgres(df)


//////////////////
local

import requests
import pandas  as pd 
from sqlalchemy import create_engine
from datetime import datetime
import os 

API_KEY= 'ZF6LX9OFTSKWJ4L1'
STOCK_SYMBOL='AAPL'
DB_USER='airflow'
DB_PASSWORD='airflow'
DB_HOST='host.docker.internal'
DB_PORT='5432'
DB_NAME='stocks'
TABLE_NAME='raw_stock_data'


#### DATA EXTRACTION #####

def fetch_stock_data():
    url = f'https://www.alphavantage.co/query'
    params = {
        'function':'TIME_SERIES_INTRADAY',
        'symbol':STOCK_SYMBOL,
        'interval':'60min',
        'apikey': API_KEY,
        'outputsize':'compact'
    }

    print('Fetching data from Alpha Vantage....')

    response = requests.get(url,params=params)
    data= response.json()

    #### PARSE RESPONSE

    time_series =data.get('Time Series (60min)',{})
    records=[]

    for timestamp, values in time_series.items():
        records.append({
            'timestamp': timestamp,
            'open': float(values['1. open']),
            'high': float(values['2. high']),
            'low': float(values['3. low']),
            'close': float(values['4. close']),
            'volume': int(values['5. volume'])
        })

    df =pd.DataFrame(records)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values('timestamp',inplace=True)

    return df
    

    ##### LOAD DATA #####
def load_to_postgres(df):
    engine= create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    print('Loading Data into PostgresSQL..')
    df.to_sql(TABLE_NAME, engine, if_exists='replace',index=False)
    print('Done')

####### Main
if __name__=='__main__':
    df=fetch_stock_data()
    print(df.head())
    load_to_postgres(df)