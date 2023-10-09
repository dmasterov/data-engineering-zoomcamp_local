#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

pd.__version__


# In[2]:


df = pd.read_parquet('/home/den/Downloads/yellow_tripdata_2023-01.parquet', 'auto')


# In[4]:


df.head(1)


# In[5]:


pd.to_datetime(df.tpep_pickup_datetime)


# In[6]:


from sqlalchemy import create_engine


# In[7]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxy')


# In[24]:


engine.connect()


# In[ ]:





# In[8]:


print(pd.io.sql.get_schema(df, name = 'yellow_taxi_data', con=engine))


# In[9]:


df.shape[0]


# In[13]:


df.to_sql('yellow_taxi_data', con = engine, if_exists='append', chunksize=10000)


# In[ ]:




