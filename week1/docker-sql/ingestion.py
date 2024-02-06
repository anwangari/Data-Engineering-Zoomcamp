#!/usr/bin/env python
# coding: utf-8

import argparse

import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Ingest some CSV data to Postgres.')

# user
# password
# host
# port
# database_name
# table_name 
# url_of_the_csv 

parser.add_argument('user', help='username for postgres')
parser.add_argument('password', help='password for postgres user')
parser.add_argument('host', help='host for postgres')
parser.add_argument('port', help='port for postgres')
parser.add_argument('db', help='database name for postgres')
parser.add_argument('table-name', help='table name [in postgres] where we write results to')
parser.add_argument('url', help='url of the csv file')

args = parser.parse_args()
print(args.accumulate(args.integers))

# Create Engine
engine = create_engine("postgresql://root:root@localhost:5432/ny-taxi")

# Read Taxi Dataset Using Pandas
data = pd.read_parquet("yellow_tripdata_2021-01.parquet")


# CONVERT THE DATASET TO CSV
# Write to CSV
data.to_csv('yellow_tripdata_2021-01.csv', index=False)

# Read data CSV iteratively / in batches
df_iter = pd.read_csv("yellow_tripdata_2021-01.csv", iterator=True, chunksize=100_000)

# **Note:** 'df_iter' IS NOT A DATAFRAME, IT'S AN ITERATOR

df = next(df_iter)

# Change date columns to datetime type
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


#READING THE DATA INTO PostgreSQL

# InsertinG the column names into SQL
df.head(0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")


# Inserting the records into the "yellow_taxi_data" table (this will read the first batch of 100,000 records)
df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")

# Inserting the rest of the dataset (batches)

while True:  
    #initialize the dataframe
    df = next(df_iter)

    # Change date columns to datetime type
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
 
    # read data into database
    df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")