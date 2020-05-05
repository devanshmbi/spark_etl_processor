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
#spark = SparkSession.builder.appName("HL7 Parser").enableHiveSupport().getOrCreate()
spark = SparkSession.builder.appName('spark_etl').getOrCreate()


""" defining main function, it will take the path from the arguments given during run time, parse it and check whether the file is ending with 
 .csv/.parquet/.json for now and will apply the functions accordingly"""
def main():

	path = str(sys.argv[1])
	""" Checking the file format whether it is csv.json,parquet by parsing the path """
	if path.split('/')[-1].split('.')[-1] == 'csv':
		""" If the path is csv use the csv_parser function to read the file and convert it to dataframe """
		df = csv_parser(path)
		df.show(4)
		print("Writing successfully to desired location")
		""" write the dataframe into desired output directory using dataframe_writer function"""
		dataframe_writer(df)
		print("Completed writing to desired location")
	elif path.split('/')[-1].split('.')[-1] == 'json':
		""" If the path is json use the json_parser function to read the file and convert it to dataframe """
		df = json_parser(path)
		df.show(4)
		print("Writing successfully to desired location")
		""" write the dataframe into desired output directory using dataframe_writer function"""
		dataframe_writer(df)
		print("Completed writing to desired location")
	elif path.split('/')[-1].split('.')[-1] == 'parquet':
		""" If the path is parquet use the parquet_parser function to read the file and convert it to dataframe """
		df = parquet_parser(path)
		df.show(4)
		print("Writing successfully to desired location")
		""" write the dataframe into desired output directory using dataframe_writer function"""
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
