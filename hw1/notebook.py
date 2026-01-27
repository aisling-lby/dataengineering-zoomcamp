#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd
from sqlalchemy import create_engine


# In[2]:

def run()
    taxi_df = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet')
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    print(pd.io.sql.get_schema(taxi_df, name='green_taxi_data', con=engine))

    taxi_df.to_sql(name='green_taxi_data', con=engine, if_exists='replace')

    zone_df = pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')
    print(pd.io.sql.get_schema(zone_df, name='zone', con=engine))
    zone_df.to_sql(name='zone', con=engine, if_exists='replace')

if __name__ == '__main__':
    run()

