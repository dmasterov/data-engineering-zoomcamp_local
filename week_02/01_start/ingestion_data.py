#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector



def chunks(l, n):
	for i in range(0, len(l), n):
			yield l.iloc[i:i+n]

@task(log_prints=True)
def download_data(url):
	parquet_name = 'output.parquet'

	os.system(f"wget {url} -O {parquet_name}")

	df = pd.read_parquet(parquet_name, 'auto')
	return df

@task(log_prints=True)
def upsert_data(df, tblname):
	
	pd.to_datetime(df.tpep_pickup_datetime)

	connection_block = SqlAlchemyConnector.load("postgres-connector")

	with connection_block.get_connection(begin=False) as engine:

		df.head(0).to_sql(name=tblname, con = engine, if_exists='replace')

		for idx, chunk in enumerate(chunks(df, 100000)):
			
			chunk.to_sql(con=engine, name=tblname, if_exists='append')
			print(f" chunk number is ", idx)


@task(log_prints=True)
def transform_data(df):

	print(f"before transformation count is: {df['passenger_count'].isin([0]).sum()}")

	print(f"before transformation total count is: {df.shape[0]}")
	
	df = df[df['passenger_count'] != 0]

	print(f"after transformation count is: {df['passenger_count'].isin([0]).sum()}")

	print(f"after transformation total count is: {df.shape[0]}")

	return df


@flow(name="ingest")
def main_flow():
	tblname = "yellow_taxi_data"
	url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

	row_data = download_data(url)

	transfored_data = transform_data(row_data)

	upsert_data(transfored_data, tblname)

if __name__ == '__main__':
	main_flow()