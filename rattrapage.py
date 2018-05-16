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
    print(target_date)
    
    target_date = (datetime.date.today()- datetime.timedelta(float(i)))
    target_date_str = str(datetime.date.today()- datetime.timedelta(float(i)))
    target_date_BQ = target_date_str.replace('-','')
    
    target_date_93 = str((target_date - datetime.timedelta(93)))
    target_date_93_BQ = target_date_93.replace('-','')
    
    query = """
    
    ############################## SQL QUERY HERE

    select A.Jour Jour,A.Conversion_page_name Conversion_page_name, Audience, count(*) cnt
    from
        (select
        CASE WHEN Audience in (1) THEN 'G0'
        WHEN Audience in (2) THEN 'PEKIN'
        WHEN Audience in (3) THEN 'MEXICO'
        WHEN Audience in (4,5) THEN 'PARIS'
        WHEN Audience in (6) THEN 'JARAMA'
        WHEN Audience in (7,8) THEN 'DETROIT'
        WHEN Audience in (9,10) THEN 'PAMPLONA'
        WHEN Audience in (11) THEN 'MADRID'
        WHEN Audience in (12,13) THEN 'BARCELONA'
        END AS Audience,
        *
        from
            (SELECT *, row_number() over (partition by B.weboid, A.Conversion_page_name order by B.dDataSource desc) Rang
            FROM 
                  (Select *
                  from  (select weboid, Audience,dDataSource
                        from table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_],timestamp('%s'),timestamp('%s'))
                        -- where Audience <> 1
                        )B

                  inner join

                        (select USER_ID, date(dh_serv) AS Jour, Conversion_page_name
                        FROM [datamining-1184:AXA_ES.wcm_conversion_%s] #WCM pour le jour courant
                        WHERE action_type='conversion' AND Conversion_page_ID IN ('24', '31', '36')
                        group by user_id, Jour, Conversion_page_name
                        )A on B.weboid=A.user_id
                 )
            )
        where rang = 1)
    group by Jour, Conversion_page_name, audience
    order by Jour, Conversion_page_name, audience

    ############################## END QUERY
    
    """ %(target_date_93_BQ, target_date_BQ, target_date_BQ)
    
    table_destination_name = 'traffic_composition_visits_20180509'
    print(query)
    
    # try:
    #     table=dataset.table(name=table_destination_name)
    #     r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
    #     job = bgclient.run_async_query('%s_%s'% (table_destination_name,r), query) 
    #     job.destination = table 
    #     job.write_disposition = 'WRITE_APPEND'
    #     job.allow_large_results = True
    #     job.begin()
    # except Exception as e:
    #     print('%s'%e)
      
    # time.sleep(15)