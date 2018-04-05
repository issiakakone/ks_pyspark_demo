
#%--------------------------------------------%#
#% Filename: toolkit.py                       %#
#% Description: Submit the spark job          %#
#% Author: Issiaka Kone                       %#
#% Date: 04/04/2018                           %#
#%--------------------------------------------%#

"""
Toolkit module contining helper functions for data processing
Author: Issiaka Kone
Date: 04/04/2018
"""

from datetime import datetime
from pyspark.sql.types import *

NAME_FIELD    =    'name'
TYPE_FIELD    =    'type'
PATTERN_FIELD = 'pattern'
LOG_NAME      = 'Toolkit'

def log(message):
	global sc
	if 'sc' in vars() or 'sc' in globals():
		LOG = sc._jvm.org.apache.log4j.LogManager.getLogger(LOG_NAME)
		LOG.warn(message)
	print(message)
	
def ConfigCheck(config):
	""" Check the configuration data validity """
	return True;

def getType(config_types, id, default_type, name_field=NAME_FIELD, type_field=TYPE_FIELD):
	""" Retrieve the SparkSQL type for id key or return default_type if id is not found """ 
	id_types = [t for t in config_types if t[name_field] == id]
	if len(id_types) > 0:
		try:
			id_type = eval(id_types[0][type_field] + "()")
			return id_type
		except:
			return default_type
	else:
		return default_type
		
def getDatePattern(config_types, id, default_pattern, name_field=NAME_FIELD, pattern_field=PATTERN_FIELD):
	""" Retrieve the date pattern for id key or return default_pattern if id is not found """ 
	id_types = [t for t in config_types if t[name_field] == id]
	if len(id_types) > 0:
		return id_types[0][pattern_field]
	else:
		return default_pattern
		

class DataSet:
	"""Parse a line and return a valid data structure with needed values or invalid data structure"""
	
	LOG                      = None
	
	def __init__(self):
						
		""" Data parsing status """
		self.valid           = False
		self.lineNum         = 0
		self.line            = None
		
		""" Columns """
		self.Identifiant     = None
		self.Date            = None
		self.Montant_1       = None
		self.Montant_2       = None
		self.Montant_3       = None
		self.Telephone       = None
		self.Sum_montant     = None
		self.Div_sum_montant = None
		
	
	def _sanitize(self, input):
		stripped = input.strip().strip('"').strip()
		return stripped
	
	def _sanitize_tel_number(self, tel):
		return self._sanitize(tel).replace(".", "").replace(" ", "").replace("-", "")
	
	def _sanitize_date(self, date):
		""" Parse the date time, if the time component is missing, we try to parse the date only """
		try:
			dt = datetime.strptime(self._sanitize(date), "%d/%m/%Y %H:%M:%S")
			return dt
		except:
			try:
				dt = datetime.strptime(self._sanitize(date), "%d/%m/%Y")
				return dt
			except:
				raise
			
		
	def parse(self, data):
		""" 
		Parse the input data and return a valid or invalid object representing the data,
		All exception raised are considered as invalid input data,
		Date without time component are considered valid if it is well formatted.
		"""

		log(".: Start parsing: %d# %s" % (data[1], data[0]))
		
		self.lineNum = data[1]
		self.line    = data[0]
		
		try:
			split_line = self.line.split(';')
			
			if(DataSet.LOG):
				DataSet.LOG.warn(split_line);
			
			self.Identifiant     =            self._sanitize(split_line[0])	
			self.Date            =       self._sanitize_date(split_line[1])
			self.Montant_1       =     float(self._sanitize(split_line[2]))
			self.Montant_2       =     float(self._sanitize(split_line[3]))
			self.Montant_3       =     float(self._sanitize(split_line[4]))
			self.Telephone       = self._sanitize_tel_number(split_line[5])
			self.Sum_montant     =    self.Montant_1 + self.Montant_2 + self.Montant_3
			self.Div_sum_montant =    (self.Montant_1 + self.Montant_2) / self.Montant_3
			
			self.valid = True
		except Exception as e:
			log("Exception occured at line number #%d for %s: %s" % (data[1], data[0], str(e)))
			self.valid = False
		
		return self
		
	def __str__(self):
		return self.__class__.__name__ + "[valid:" + str(self.valid) + ", " + \
			"Identifiant:" + self.Identifiant + ", " + \
			"Date:" + str(self.Date) + ", " + \
			"Montant_1:" + str(self.Montant_1) + ", " + \
			"Montant_2:" + str(self.Montant_2) + ", " + \
			"Montant_3:" + str(self.Montant_3) + ", " + \
			"Telephone:" + str(self.Telephone) + ", " + \
			"Sum_montant:" + str(self.Sum_montant) + ", " + \
			"Div_sum_montant:" + str(self.Div_sum_montant) + ", " + \
			"lineNum:" + str(self.lineNum) + ", " + \
			"line:" + self.line + "]"

	def __repr__(self):
		return self.__str__()
		
	
	