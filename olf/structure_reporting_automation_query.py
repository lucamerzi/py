from google.cloud import bigquery
import datetime
import random
import time
import numpy as np

# project = "datamining-1184" #INSERT DATAMINING ID HERE
# dataset_name = 'AXA_ES'
# bgclient = bigquery.Client(project=project)
# dataset = bgclient.dataset(dataset_name)

#GET THE START DATE
start_date = (datetime.date.today() - datetime.timedelta(float(7))) # CORRECTION: pd.Timedelta cannot accept np.int64 on python 3.3 or 3.4, however np.float64 does work.
start_date_str = str(start_date)
start_date_bq = start_date_str.replace('-','')
print("I am calculating from the date: " + start_date_bq)

#GET THE END DATE
end_date = (datetime.date.today() - datetime.timedelta(float(1))) # CORRECTION: pd.Timedelta cannot accept np.int64 on python 3.3 or 3.4, however np.float64 does work.
end_date_str = str(end_date)
end_date_bq = end_date_str.replace('-','')
print("I am calculating up to the date: " + end_date_bq)



################################################ TABLE 1: ENTER YOUR SQL QUERY HERE ################################################
query = """


""" %()

#print(query)