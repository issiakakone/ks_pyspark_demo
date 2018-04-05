
#%--------------------------------------------%#
#% Filename: main.py                          %#
#% Description: Submit the spark job          %#
#% Author: Issiaka Kone                       %#
#% Date: 04/04/2018                           %#
#%--------------------------------------------%#

"""
Main script for data processing
Author: Issiaka Kone
Date: 04/04/2018

Usage: main.py config_file
See also: readme.md
"""

import json
import os
import sys
from pprint import pprint
from pyspark import SparkContext
from pyspark.sql import SQLContext, SaveMode
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

from toolkit import *

APP_NAME = "KoneSoft PySpark Example"

def log(message):
	global sc
	if 'sc' in vars() or 'sc' in globals():
		LOG = sc._jvm.org.apache.log4j.LogManager.getLogger(APP_NAME)
		LOG.warn(message)
	print(message)
	

def main():
	"""
        Usage: main.py config_file
    """
	
	if len(sys.argv) < 1:
		print("Usage: main.py config_file")
		exit(1);
		
	config_file = sys.argv[1];
	
	sc = SparkContext(appName=APP_NAME)
	sqlContext = SQLContext(sc)
	
	# Check and load configuration file and data
	if not os.path.exists (config_file):
		log("Configuration file does not exist: [%s]" % config_file)
		exit(1);
	
	log("Configuration file is: [%s]" % config_file)
	
	with open(config_file) as conf_file:
		config = json.load(conf_file)

	if not ConfigCheck(config):
		log("The configuration data are not valid: [%s]" % config_file)
		exit(1)
	
	# Setting variables
	input_filename = os.path.join(config['environment']['hdfs_namenode'], config['input']['path'], config['input']['filename'])
	exception_filename = os.path.join(config['environment']['hdfs_namenode'], config['parsing']['invalid_path'], config['parsing']['invalid_filename'])
	save_format = config['output']['format']
	output = os.path.join(config['environment']['hdfs_namenode'], config['output']['path'], config['output']['filename']) + "." + save_format
	
	config_types = config['columns']
	
	log("Parameters:\n\tInput: %s, \n\tOutput: %s, \n\tException: %s." % (input_filename, output, exception_filename));
	
	# Reading input source file
	data = sc.textFile(input_filename)
	data = data.zipWithIndex()
	header = data.first()
	sanitized_data = data.filter(lambda d: d != header).filter(lambda d: d[0].strip() != "")

	parsed_data = sanitized_data.map(lambda d: DataSet().parse(d))
	
	# For performance as parsed_data will be used twice
	parsed_data.cache()
	invalid_data = parsed_data.filter(lambda d: not d.valid).map(lambda d: "Line #%d, '%s'" % (d.lineNum, d.line))
	valid_data = parsed_data.filter(lambda d: d.valid)
	
	# Save invalid data according to the spec
	invalid_data.saveAsTextFile(exception_filename)
	
	# About to create DataFrame
	row_data = valid_data.map(lambda d: Row(C1=d.Identifiant, C2=d.Date, C3=d.Montant_1, C4=d.Montant_2, C5=d.Montant_3, C6=d.Telephone, C7=d.Sum_montant, C8=d.Div_sum_montant))
	
	schema = StructType([ \
		StructField("Identifiant", getType(config_types, "Identifiant", StringType()), True), \
		StructField("Date", getType(config_types, "Date", TimestampType()), True), \
		StructField("Montant_1", getType(config_types, "Montant_1", DoubleType()), True), \
		StructField("Montant_2", getType(config_types, "Montant_2", DoubleType()), True), \
		StructField("Montant_3", getType(config_types, "Montant_3", DoubleType()), True), \
		StructField("Telephone", getType(config_types, "Telephone", StringType()), True), \
		StructField("Sum_montant", getType(config_types, "Sum_montant", DoubleType()), True), \
		StructField("Div_sum_montant", getType(config_types, "Div_sum_montant", DoubleType()), True), \
		])
	
	structured_data = sqlContext.createDataFrame(row_data, schema)
	
	# We no longer need to persit the RDD
	parsed_data.unpersist()
	
	structured_data.show()

	# Saving to output file according to the date format in the config file
	date_pattern = getDatePattern(config_types, "Date", "%Y-%m-%d")
	saved_data = structured_data.select("Identifiant", date_format('Date', date_pattern).alias('pDate'), "Montant_1", "Montant_2", "Montant_3", "Telephone", "Sum_montant", "Div_sum_montant")
	
	# Save data with partitionning by year and month
	saved_data.write.partitionBy("Date").mode('overwrite').format(save_format).save(output)
	
	# Stop the context
	sc.stop()
	exit(0)
	

if __name__ == "__main__":
	main()