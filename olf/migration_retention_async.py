# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 18:02:04 2017

@author: absaibes
"""
from gcloud import bigquery
import json
import time
import random

project = 'datamining-1184'
bgclient = bigquery.Client(project=project)
dataset_name = 'AXA_ES'
dataset = bgclient.dataset(dataset_name)



# Defining function

def Migration (step):
   if step==30 or step==60 or step==90 or step==120 or step==150:
      #laisse le tps a BQ car trop de jobs en même temps ne passe pas (limit 53)
      print(step,'DODO')
      time.sleep(600) 
      print('WAKE UP')
   
   else:
      query = """\

            Select %s as Rang, A1,A2, float(count(*)) as NbCookies,
            CASE WHEN A1 in (1) THEN 'G0'
            WHEN A1 in (2) THEN 'PEKIN'
            WHEN A1 in (3) THEN 'MEJICO'
            WHEN A1 in (4,5) THEN 'PARIS'
            WHEN A1 in (6) THEN 'JARAMA'
            WHEN A1 in (7,8) THEN 'DETROIT'
            WHEN A1 in (9,10) THEN 'PAMPLONA'
            WHEN A1 in (11) THEN 'MADRID'
            WHEN A1 in (12,13) THEN 'BARCELONA'
            END AS Audience_name_A1,
            CASE WHEN A2 in (1) THEN 'G0'
            WHEN A2 in (2) THEN 'PEKIN'
            WHEN A2 in (3) THEN 'MEJICO'
            WHEN A2 in (4,5) THEN 'PARIS'
            WHEN A2 in (6) THEN 'JARAMA'
            WHEN A2 in (7,8) THEN 'DETROIT'
            WHEN A2 in (9,10) THEN 'PAMPLONA'
            WHEN A2 in (11) THEN 'MADRID'
            WHEN A2 in (12,13) THEN 'BARCELONA'
            END AS Audience_name_A2
            from (Select weboid,audience as A1, dDataSource as D1
                     from (Select weboid,audience,dDataSource,row_number()over(partition by weboid order by dDataSource) rang
                           from (SELECT weboid,audience,dDataSource
                                 from table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_],timestamp('20180101'),timestamp('20180516'))
                                 where audience != 1
                                 group by weboid,audience,dDataSource
                                 )
                           )
                     where rang=%s
                     ) a
               inner join (Select weboid,audience as A2, dDataSource as D2
                           from (Select weboid,audience,dDataSource,row_number()over(partition by weboid order by dDataSource) rang
                                 from (SELECT weboid,audience,dDataSource
                                       from table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_],timestamp('20180101'),timestamp('20180516'))
                                       where audience != 1
                                       group by weboid,audience,dDataSource
                                       )
                                 )
                           where rang=%s
                           ) b on a.weboid=b.weboid
               group by A1,A2, Audience_name_A1, Audience_name_A2
               order by A1,A2

               """ % (step,step, step+1)

      #print(query)

      table=dataset.table(name='') #update table name
      r_key=str(random.randint(1,1000000)) 
      job = bgclient.run_async_query('Axa_Migration_Job_%s_%s'% (step,r_key), query) 
      job.destination = table 
      job.write_disposition= 'WRITE_APPEND'
      job.begin()



# Defining function

def Retention (step):
   if step==30 or step==60 or step==90 or step==120 or step==150:
      #laisse le tps a BQ car trop de jobs en même temps ne passe pas (limit 53)
      print(step,'DODO')
      time.sleep(600) 
      print('WAKE UP')
   
   else:
      query = """\

               Select %s as Rang,A1, A2, avg(diff) EcartJour,
                  CASE WHEN A1 in (1) THEN 'G0'
                  WHEN A1 in (2) THEN 'PEKIN'
                  WHEN A1 in (3) THEN 'MEJICO'
                  WHEN A1 in (4,5) THEN 'PARIS'
                  WHEN A1 in (6) THEN 'JARAMA'
                  WHEN A1 in (7,8) THEN 'DETROIT'
                  WHEN A1 in (9,10) THEN 'PAMPLONA'
                  WHEN A1 in (11) THEN 'MADRID'
                  WHEN A1 in (12,13) THEN 'BARCELONA'
                  END AS Audience_name_A1,
                  CASE WHEN A2 in (1) THEN 'G0'
                  WHEN A2 in (2) THEN 'PEKIN'
                  WHEN A2 in (3) THEN 'MEJICO'
                  WHEN A2 in (4,5) THEN 'PARIS'
                  WHEN A2 in (6) THEN 'JARAMA'
                  WHEN A2 in (7,8) THEN 'DETROIT'
                  WHEN A2 in (9,10) THEN 'PAMPLONA'
                  WHEN A2 in (11) THEN 'MADRID'
                  WHEN A2 in (12,13) THEN 'BARCELONA'
                  END AS Audience_name_A2
               from (Select A1, A2, datediff(D2,D1) diff
                     from (Select weboid,audience as A1, dDataSource as D1
                           from (Select weboid,audience,dDataSource,row_number()over(partition by weboid order by dDataSource) rang
                                 from (SELECT weboid,audience,dDataSource
                                       from table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_],timestamp('20180101'),timestamp('20180516'))
                                       where audience != 1
                                       group by weboid,audience,dDataSource
                                       )
                                 )
                           where rang=%s
                           ) a
                     inner join (Select weboid,audience as A2, dDataSource as D2
                                 from (Select weboid,audience,dDataSource,row_number()over(partition by weboid order by dDataSource) rang
                                       from (SELECT weboid,audience,dDataSource
                                             from table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_],timestamp('20180101'),timestamp('20180516'))
                                             where audience != 1
                                             group by weboid,audience,dDataSource
                                             )
                                       )
                                 where rang=%s
                                 ) b on a.weboid=b.weboid
                   )
               group by A1, A2, Audience_name_A1, Audience_name_A2

               """ % (step,step, step+1)

      #print(query)

      table=dataset.table(name='') #update table name
      r_key=str(random.randint(1,1000000)) 
      job = bgclient.run_async_query('Axa_Retention_Job_%s_%s'% (step,r_key), query) 
      job.destination = table 
      job.write_disposition= 'WRITE_APPEND'
      job.begin()



# Run function(s)

steps=list(range(1,30-1,1))

#map(Migration,steps)
#map(Retention,steps)

for s in steps:
   print(s)
   Migration(s)
   #Retention(s)