#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def chunks(l, n):
	for i in range(0, len(l), n):
			yield l.iloc[i:i+n]

def ingest_data(user, password, host, port, dbname, tblname, url):

	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

	parquet_name = 'output.parquet'

	os.system(f"wget {url} -O {parquet_name}")

	df = pd.read_parquet(parquet_name, 'auto')

	pd.to_datetime(df.tpep_pickup_datetime)

	engine.connect()

	df.head(0).to_sql(name=tblname, con = engine, if_exists='replace')

	for idx, chunk in enumerate(chunks(df, 100000)):
		
		chunk.to_sql(con=engine, name=tblname, if_exists='append')
		print(f" chunk number is ", idx)

if __name__ == '__main__':
	user = "root"
	password = "root"
	host = "localhost"
	port = "5432"
	dbname = "ny_taxy"
	tblname = "yellow_taxi_data"
	url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"


	ingest_data(user, password, host, port, dbname, tblname, url)