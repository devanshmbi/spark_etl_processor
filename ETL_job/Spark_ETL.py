""" Currently the script focuses on reading the CSV/JSON/parquet file format,Create dataframe and apply certain transformations.
 Future endavours will read a txt file / fxwd file and apply business logic"""


""" Importing all the necessary modules from spark libraries """

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os, sys
import numpy as np
import pandas as pd

""" Creating spark session """
spark = SparkSession.builder.appName('spark_etl').getOrCreate()

""" defining main function, it will take the path, parse it and check whether the file is ending with 
 .csv/.parquet/.json for now and will apply the functions accordingly"""
def main():

	path = str(sys.argv[1])
	if path[-3:] == 'csv':
		df = csv_parser(path)
		df.show(4)
		print("Writing successfully to desired location")
		dataframe_writer(df)
		print("Completed writing to desired location")
	elif path[-3:] == 'json':
		df = json_parser(path)
		df.show(4)
		print("Writing successfully to desired location")
		dataframe_writer(df)
		print("Completed writing to desired location")
	elif path[-3:] == 'parquet':
		df = parquet_parser(path)
		df.show(4)
		print("Writing successfully to desired location")
		dataframe_writer(df)
		print("Completed writing to desired location")

def csv_parser(variable):
	csv_reader = spark.read.csv(path = variable, sep = None, header = True,nullValue = 'NULL', nanValue = np.NaN)
	return csv_reader

def json_parser(variable):
	json_reader = spark.read.json(path = variable, multiLine = True, lineSep = '\n')
	return json_parser

def parquet_parser(variable):
	parquet_reader = spark.read.parquet(path = variable)
	return parquet_parser

def dataframe_writer(variable):
	output_directory = '/user/dmodi001/spark_etl_output/' + str(sys.argv[1]).split('/')[-1].split('.')[0]
	df_writer = variable.write.parquet(path= output_directory, mode = 'overwrite', compression = "snappy", partitionBy=None)
	return None

if __name__=='__main__':
	main()
