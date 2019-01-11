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
jours = np.arange(2, 100, 1)
jours_ord=sorted(jours, reverse=True)

for i in jours_ord:
    target_date = (datetime.date.today()- datetime.timedelta(float(i))) # retrieve date target_date
    target_date_str = str(datetime.date.today()- datetime.timedelta(float(i))) # retrieve date target_date
    target_date_BQ = target_date_str.replace('-','')
    print(target_date)

    # target_date_93 = str((target_date - datetime.timedelta(93)))
    # target_date_93_BQ = target_date_93.replace('-','')
    
    query = """
    
    ############################## SQL QUERY HERE


    select jour, conversion_page_name, 
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
    count(user_id) daily_audience
    from
        (select *, ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY diff asc) row_num
        from
            (select x.user_id user_id, x.conversion_page_name conversion_page_name,    date(x.jour) jour,    x.campain_id campain_id,    y.Audience audience,    date(y.dDataSource) dDataSource, abs(datediff(jour, dDataSource)) diff
            from

                (select user_id, conversion_page_name, date(visite) jour, campain_id
                from
                    (select a.conversion_page_name conversion_page_name, a.user_id user_id, a.visite visite, b.user_id userid, b.campain_id campain_id, b.expo expo
                    from
                        -- website visit
                        (
                        SELECT conversion_page_name, user_id, min(dh_serv) as visite
                        FROM [datamining-1184:AXA_ES.wcm_conversion_%s]
                        where 1 = 1 
                        --and user_id = "DEUNgUeCBWMM"
                        and conversion_page_id in ('24', '31', '36')
                        and action_type = "conversion"
                        group by conversion_page_name, user_id
                        )a

                    inner join
                        -- exposure to campaign
                        (
                        SELECT user_id, campain_id, dh_serv as expo
                        FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp("20180128"), timestamp("%s"))
                        where campain_id = "28" OR campain_id = "29" OR campain_id = "34"
                        group by user_id, campain_id, expo
                        )b

                    on 
                    a.user_id = b.user_id
                    having visite > expo)
                group by user_id, conversion_page_name, jour, campain_id)x

            inner join

                (
                select weboid, Audience,dDataSource
                from table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_],timestamp('20180101'),timestamp('%s'))
                -- where Audience <> 1
                )y
            on x.user_id = y.weboid
            --where x.user_id = "g3v3fhppn1SD"
            ))
    where row_num = 1
    group by jour, conversion_page_name, audience
    order by jour, conversion_page_name, audience


    ############################## END QUERY
    
    """ %(target_date_BQ, target_date_BQ, target_date_BQ)
    
    table_destination_name = 'traffic_composition_visits_exposed_20180509'
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
      
    # time.sleep(30)