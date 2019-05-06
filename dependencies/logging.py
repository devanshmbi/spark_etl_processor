import logging

#logging.basicConfig(filename= 'test.log', level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(module)s:%(message)s')
#CREATING LOGGER TO GET THE CLASS OF THE OBJECT OR MODULE NAME

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#CREATING FORMAT WHICH CAN BE THEN PASSED TO FILE HANDLER TO WRITE OUTPUT TO FILE
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(module)s:%(message)s')

#CREATING OUTPUT FILE TO STORE ALL THE LOGS
file_handler = logging.FileHandler('spark_etl.log')
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)

#printing out the logs or error part to console
#stream_handler = logging.StreamHandler()
#stream_handler.setLevel(logging.DEBUG)
#file_handler.setFormatter(formatter)

#passing the output parameter file to logger to save only error output to sample.log file
logger.addHandler(file_handler)

#passing the output parameter file to logger to print output to console
#logger.addHandler(stream_handler)
