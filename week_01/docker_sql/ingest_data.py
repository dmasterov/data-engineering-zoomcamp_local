#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time 

def chunks(l, n):
	for i in range(0, len(l), n):
			yield l.iloc[i:i+n]

def main(params):

	user = params.user
	password = params.password
	host = params.host
	port = params.port
	dbname = params.dbname
	tblname = params.tblname
	url = params.url

	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

	parquet_name = 'output.parquet'

	os.system(f"wget {url} -O {parquet_name}")

	df = pd.read_parquet(parquet_name, 'auto')

	pd.to_datetime(df.tpep_pickup_datetime)

	engine.connect()

	#print(pd.io.sql.get_schema(df, name = 'yellow_taxi_data', con=engine))

	df.head(0).to_sql(name=tblname, con = engine, if_exists='replace')

	for idx, chunk in enumerate(chunks(df, 100000)):
		
		chunk.to_sql(con=engine, name=tblname, if_exists='append')
		print(f" chunk number is ", idx)

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description='ingesting parquet file to postgre')

	#user, password, host, port, dbname, url

	parser.add_argument('--user', help = 'username for pg')

	parser.add_argument('--password', help = 'password for pg')

	parser.add_argument('--host', help = 'host for pg')

	parser.add_argument('--port', help = 'port for pg')

	parser.add_argument('--dbname', help = 'dbname for pg')

	parser.add_argument('--tblname', help = 'tblname for pg')

	parser.add_argument('--url', help = 'url for parquet file')

	args = parser.parse_args()

	main(args)