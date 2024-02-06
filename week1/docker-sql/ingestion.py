#!/usr/bin/env python
# coding: utf-8

import os
import argparse

import pandas as pd
from sqlalchemy import create_engine


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = int(params.port)
    db = params.db
    table_name = params.table_name
    url=params.url

    parquet_name = "output.parquet"
    csv_name = "taxi_data.csv"

    # Create Engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # Download file from url
    os.system(f"wget {url} -O {parquet_name}")

    #Initialize dataframe from .parquet file
    data = pd.read_parquet(parquet_name)

    # convert parquet data to CSV format
    data.to_csv(csv_name, index=False)

    # Read data CSV iteratively / in batches
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000, low_memory=False)
    # **Note:** 'df_iter' IS NOT A DATAFRAME, IT'S AN ITERATOR
    
    df = next(df_iter)

    # Change date columns to datetime type
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime, format='%Y-%m-%d %H:%M:%S')
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime, format='%Y-%m-%d %H:%M:%S')

    #READING THE DATA INTO PostgreSQL

    # InsertinG the column names into SQL
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")


    # Inserting the records into the "yellow_taxi_data" table (this will read the first batch of 100,000 records)
    df.to_sql(name=table_name, con=engine, if_exists="append")

    # Inserting the rest of the dataset (batches)

    while True:  
        #initialize the dataframe
        df = next(df_iter)

        # Change date columns to datetime type
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime, format='%Y-%m-%d %H:%M:%S')
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime, format='%Y-%m-%d %H:%M:%S')
    
        # read data into database
        df.to_sql(name=table_name, con=engine, if_exists="append")



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest some CSV data to Postgres.')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres user')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name [in postgres] where we write results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()


main(args)