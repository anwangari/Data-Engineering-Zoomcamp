#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


#Install sqlalchemy
get_ipython().system('pip install sqlalchemy psycopg2-binary')


# In[3]:


from sqlalchemy import create_engine, MetaData


# In[4]:


# Create Engine
engine = create_engine("postgresql://root:root@localhost:5432/ny-taxi")


# In[5]:


# Test Connection
engine.connect()


# In[6]:


query = """
    SELECT 1;
"""

pd.read_sql(query, con=engine)


# ## Read Taxi Dataset Using Pandas

# In[7]:


# INSTALL DEPENDENCIES FOR "read_parquet"
get_ipython().system('pip install pyarrow')
get_ipython().system('pip install fastparquet')


# In[8]:


data = pd.read_parquet("yellow_tripdata_2021-01.parquet")

print(data.shape)
print(data.info())
data.head()


# In[9]:


print(pd.io.sql.get_schema(data, name="yellow_taxi_data", con=engine))


# #### Since the size of the dataframe is big (over 1.3m records), we use iterators to read the entire dataframe in batches into the database. Therefore, we need to convert the dataset to csv to give us iteration capabilities.

# In[10]:


# CONVERT THE DATASET TO CSV

# Write to CSV
data.to_csv('yellow_tripdata_2021-01.csv', index=False)


# In[11]:


df_iter = pd.read_csv("yellow_tripdata_2021-01.csv", iterator=True, chunksize=100_000)
df_iter


# **Note:** 'df_iter' is not a dataframe, it's an iterator.

# In[12]:


df = next(df_iter)
print(df.info())
df.head()


# In[29]:


# Change date columns to datetime type
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[30]:


df.info()


# In[31]:


df.head(0)


# ## Reading The Data Into PostgreSQL

# In[32]:


# Insertin the column names into SQL
df.head(0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")


# In[33]:


# Inserting the records into the "yellow_taxi_data" table (this will read the first batch of 100,000 records)
get_ipython().run_line_magic('time', 'df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")')


# Inserting the rest of the dataset (batches)

# In[13]:


from time import time

while True:
    t_start = time()
    
    #initialize the dataframe
    df = next(df_iter)

    # Change date columns to datetime type
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
 
    # read data into database
    df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")

    t_end = time()

    print(f"Inserted another chunk, took {round(t_end - t_start, 3)}")


# In[ ]:




