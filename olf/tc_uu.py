from google.cloud import bigquery
import datetime
import random
import time
import numpy as np

project = "datamining-1184"
dataset_name = 'AXA_ES'

bgclient = bigquery.Client(project=project)
dataset = bgclient.dataset(dataset_name)

#Liste de rattrapage
jours = np.arange(2, 149, 1)
jours_ord=sorted(jours, reverse=True)

for i in jours_ord:
    target_date = (datetime.date.today()- datetime.timedelta(float(i))) # retrieve date target_date
    target_date_str = str(datetime.date.today()- datetime.timedelta(float(i))) # retrieve date target_date
    target_date_BQ = target_date_str.replace('-','')
    print(target_date)
    
    
    query = """
    
    ############################## SQL QUERY HERE
    

    select jour, conversion_page_name, count(*) daily_uu
    from
        (SELECT conversion_page_name, date(dh_serv) as jour, user_id
        FROM [datamining-1184:AXA_ES.wcm_conversion_%s]
        where 1 = 1 
        --and user_id = "DEUNgUeCBWMM"
        and conversion_page_id in ('24', '31', '36')
        and action_type = "conversion"
        group by conversion_page_name, jour, user_id
        )
    group by conversion_page_name, jour
    order by jour, conversion_page_name


    ############################## END QUERY
    
    """ %(target_date_BQ)
    
    table_destination_name = 'tc_uu'
    print(query)
    
    try:
        table=dataset.table(name=table_destination_name)
        r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
        job = bgclient.run_async_query('%s_%s'% (table_destination_name,r), query) 
        job.destination = table 
        job.write_disposition = 'WRITE_APPEND'
        job.allow_large_results = True
        job.begin()
    except Exception as e:
        print('%s'%e)
      
    time.sleep(10)