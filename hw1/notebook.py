#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd
from sqlalchemy import create_engine


# In[2]:


taxi_df = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet')


# In[3]:


taxi_df.head()


# In[5]:


taxi_df.dtypes


# In[6]:


len(taxi_df)


# In[10]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[11]:


print(pd.io.sql.get_schema(taxi_df, name='green_taxi_data', con=engine))


# In[15]:


taxi_df.to_sql(name='green_taxi_data', con=engine, if_exists='replace')


# In[19]:


zone_df = pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')


# In[20]:


len(zone_df)


# In[21]:


zone_df.head()


# In[22]:


zone_df.dtypes


# In[23]:


print(pd.io.sql.get_schema(zone_df, name='zone', con=engine))


# In[24]:


zone_df.to_sql(name='zone', con=engine, if_exists='replace')

