# Imports
import pandas as pd
from pandas.io.json import json_normalize
import datetime as dt
import requests
import json
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR,FLOAT,INTEGER,BIGINT,BLOB,DATETIME,DATE,TIMESTAMP,TIME,BOOLEAN

# load credentials and create db connection
with open('credentials.json') as f:
    db_creds = json.load(f)
connect_str = 'mysql+pymysql://%s:%s@%s:%s/%s' % \
              (db_creds['username'], 
               db_creds['password'],
               db_creds['host'],
               db_creds['port'],
               db_creds['database'])
engine = create_engine(connect_str, echo = False)

# full list of pairs to check
orig_list = ['BALB','CAST','DELN','NCON','UCTY']
orig_dest_pairs = [('BALB', 'ANTC'), ('BALB', 'DUBL'), ('BALB', 'RICH'), ('BALB', 'WARM'), 
                   ('CAST', 'BAYF'), ('CAST', 'DALY'), 
                   ('DELN', 'BAYF'), ('DELN', 'MLBR'), ('DELN', 'WARM'), 
                   ('NCON', 'BAYF'), ('NCON', 'MLBR'), 
                   ('UCTY', 'DALY'), ('UCTY', 'MLBR'), ('UCTY', 'RICH'), ('UCTY', 'SFIA')]

# Empty data frame for the final list of train estimates
df_estimates = pd.DataFrame({
                                'queried_at':  [],
                                'orig':        [],
                                'dest':        [],
                                # 'origin':      [],
                                # 'destination': [],
                                'direction':   [],
                                'minutes':     [],
                                'length':      [],
                                'platform':    [],
                                'color':       [],
                                'hexcolor':    [],
                                'limited':     [],
                                'delay':       [],
                                'bikeflag':    [],
                            })
estimate_time_of_departure_dtypes = {
                                'queried_at':  TIMESTAMP,
                                'orig':        VARCHAR(4),
                                'dest':        VARCHAR(4),
                                # 'origin':      VARCHAR(64),
                                # 'destination': VARCHAR(64),
                                'direction':   VARCHAR(64),
                                'minutes':     INTEGER,
                                'length':      INTEGER,
                                'platform':    INTEGER,
                                'color':       VARCHAR(16),
                                'hexcolor':    VARCHAR(8),
                                'limited':     INTEGER,
                                'delay':       INTEGER,
                                'bikeflag':    INTEGER,
                            }

for orig in orig_list:
    print(orig, end=' ')

    rest_url = 'http://api.bart.gov/api/etd.aspx?cmd=etd&orig=%s&key=MW9S-E7SL-26DU-VV8V&json=y' % (orig)
    response = requests.post(rest_url)
    response_json = json.loads(response.content)
    timestamp_str = (response_json['root']['date'] + ' ' + response_json['root']['time'])
    dt_queried_at = dt.datetime.strptime(timestamp_str,'%m/%d/%Y %I:%M:%S %p %Z')
    print(timestamp_str)
    
    df_orig = json_normalize(response_json['root']['station'])
    df_dest = json_normalize(df_orig['etd'][0])

    # flatten json to append to df_est
    for i,d in df_dest.iterrows():
        df_this_est = json_normalize(d['estimate'])

        df_this_est['queried_at'] = dt_queried_at
        df_this_est['orig'] = df_orig.loc[0,'abbr']
        # df_this_est['origin'] = df_orig.loc[0,'name']
        df_this_est['dest'] = d['abbreviation']
        # df_this_est['destination'] = d['destination']
        df_this_est['limited'] = d['limited']

        # only keep the nearest train
        drop_idx = df_this_est.index[df_this_est['minutes'] > df_this_est['minutes'].min()]

        df_estimates = pd.concat([df_estimates, 
                                  df_this_est.drop(drop_idx)], 
                                 sort=False
                                ).reset_index(drop=True)

df_estimates.to_sql('estimate_time_of_departure',
                    engine,
                    index=False,
                    dtype=estimate_time_of_departure_dtypes,
                    if_exists='append')
