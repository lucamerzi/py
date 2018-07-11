import numpy as np
import pandas as pd
from google.cloud import bigquery
import random
import time
import datetime


def fetch_data_local(query):

	"""
	Fetch the results of a query in a pandas DataFrame
	"""

	client = bigquery.Client(project='datamining-1184')
	
	query_results = client.run_sync_query(query)

	# Use standard SQL syntax for queries.
	# See: https://cloud.google.com/bigquery/sql-reference/
	query_results.use_legacy_sql = True
	query_results.timeout_ms=10*60*1000

	query_results.run()

	job = query_results.job
	job.reload()                          # API rquest
	retry_count = 0

	while retry_count < 15 and job.state != 'DONE':
		time.sleep(1.5**retry_count)      # exponential backoff
		retry_count += 1
		job.reload()                      # API request

	assert job.state == 'DONE'

	final_rows=[]
	rows=query_results.fetch_data()
	
	for row in rows:
		final_rows.append(row)

	print(str(len(final_rows))+" rows fetched")
	colnames=[a.name for a in query_results.schema]
	return pd.DataFrame(final_rows,columns=colnames)


query_str = """
				select
				campain_id,
				case 
				when campain_id = "28" then "Jarama_HH"
				when campain_id = "29" then "Madrid"
				when campain_id = "30" then "Barcelona"
				when campain_id = "32" then "Mexico"
				when campain_id = "33" then "Paris"
				when campain_id = "34" then "Pamplona"
				when campain_id = "36" then "Jarama_Auto"
				when campain_id = "37" then "Detroit"
				END AS Audience,
				count(user_id) Value,
				FROM      
				    (
				    SELECT user_id, campain_id
				    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp("20180604"), timestamp("20180610"))
				    where campain_id = "28" OR campain_id = "29" OR campain_id = "30" OR campain_id = "32" OR campain_id = "33" OR campain_id = "34" OR campain_id = "36" OR campain_id = "37"
				    group by user_id, campain_id
				    )
				group by campain_id, Audience
				order by campain_id, Audience
			"""


# executes the query and DL the result as a pandas dataframe
df = fetch_data_local(query_str)
#filepath where to write the excel
filepath = "test.xlsx"

print("Writing to file: "+str(filepath))
writer=pd.ExcelWriter(filepath, engine='xlsxwriter')

df.to_excel(writer, sheet_name="MyAwesomeQueryResult",index=False)

#important to be executed at the end
writer.close()

