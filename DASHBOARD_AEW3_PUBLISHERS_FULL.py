from gcloud import bigquery
import datetime
import random
import time
import numpy as np

project = "datamining-1184" #INSERT DATAMINING ID HERE
dataset_name = 'AXA_ES'

bgclient = bigquery.Client(project=project)
dataset = bgclient.dataset(dataset_name)

#GET THE TARGET DAY
targetDate = (datetime.date.today() - datetime.timedelta(float(2))) # CORRECTION: pd.Timedelta cannot accept np.int64 on python 3.3 or 3.4, however np.float64 does work.
targetDateStr = str(targetDate)
targetDateBQ = targetDateStr.replace('-','')
print("I am calculating the for the target date: " + targetDateBQ)

#GET THE TARGET DAY MINUS 30 DAYS
targetDate93Str = str(targetDate - datetime.timedelta(93))
targetDate93BQ = targetDate93Str.replace('-','')
print("I will start retrieving profiles on : " + targetDate93BQ)










################################################ TABLE 1: DASHBOARD_AEW3_PUBLISHERS_AUDIENCES ################################################
query = """
        SELECT timestamp(Date) as Date, site_name, Audience, UU, UUprofiles, UU_Tot, Imp
        FROM
        (select a.Date as Date, a.site_name as site_name, a.Audience as Audience, a.UU as UU, b.UUAud as UUAud, C.UUprofiles as UUprofiles, d.UU_Tot as UU_Tot, d.Imp as Imp
        from

        ########################## Volumes par audience par jour par publisher != audience 1

            (Select string(integer(%s)) as Date, site_name,Audience, count(*) as UU
            from (Select weboid,Audience, site_name, row_number() over (partition by weboid,site_name/*,Audience*/ order by diff asc) as Rang
                from (Select weboid,Audience, site_name, datediff(DH_SERV,ddatasource) diff
                from (SELECT user_id, site_name, date(DH_SERV) as DH_SERV
                    FROM [datamining-1184:AXA_ES.wcm_impressionvisibility_%s]
                    where campain_id = "19"
                    ) as a
                inner join (select weboid,
                            CASE WHEN Audience in (1) THEN 'G0'
                            WHEN Audience in (2) THEN 'PEKIN'
                            WHEN Audience in (3) THEN 'MEJICO'
                            WHEN Audience in (4,5) THEN 'PARIS'
                            WHEN Audience in (6) THEN 'JARAMA'
                            WHEN Audience in (7,8) THEN 'DETROIT'
                            WHEN Audience in (9,10) THEN 'PAMPLONA'
                            WHEN Audience in (11) THEN 'MADRID'
                            WHEN Audience in (12,13) THEN 'BARCELONA'
                            END AS Audience,
                            date(ddatasource) as ddatasource
                        FROM table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_], timestamp('%s'), timestamp('%s'))
                        where AUDIENCE <>1 ########################## FILTRE AUDIENCE 1
                        ) as b on a.user_id=b.weboid
                )
                )
            where Rang=1
            group by site_name,Audience
            ORDER BY site_name,Audience) as a

        inner join

        ########################## Volumes par audience par jour != audience 1

            (SELECT string(integer('%s')) as Date, 
            CASE WHEN Audience in (1) THEN 'G0'
                WHEN Audience in (2) THEN 'PEKIN'
                WHEN Audience in (3) THEN 'MEJICO'
                WHEN Audience in (4,5) THEN 'PARIS'
                WHEN Audience in (6) THEN 'JARAMA'
                WHEN Audience in (7,8) THEN 'DETROIT'
                WHEN Audience in (9,10) THEN 'PAMPLONA'
                WHEN Audience in (11) THEN 'MADRID'
                WHEN Audience in (12,13) THEN 'BARCELONA'
                END AS Audience, 
                            count(weboid) UUAud
                    from (select weboid, audience, row_number() over (partition by weboid order by ddatasource desc) as Rang
                        FROM table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_], timestamp('%s'), timestamp('%s'))
                where AUDIENCE <>1 ########################## FILTRE AUDIENCE 1
                        )
                    where rang=1
                    group by Audience) as b
            
        on a.Date = b.Date and a.audience = b.audience 

        inner join

        ########################## Volumes profilés par jour par publisher != audience 1

            (Select string(integer('%s')) as Date,site_name, count(*) UUProfiles
            from (Select weboid, site_name
                from (SELECT user_id, site_name
                    FROM [datamining-1184:AXA_ES.wcm_impressionvisibility_%s]
                    where campain_id = "19") as a
                inner join (select weboid
                    FROM table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_], timestamp('%s'), timestamp('%s'))
                    where AUDIENCE <>1 ########################## FILTRE AUDIENCE 1

                    ) as b on a.user_id=b.weboid
                GROUP BY weboid,site_name
            )
            group by site_name
            order by site_name) as c

        on a.Date = c.Date and a.site_name = c.site_name

        inner join

        ########################## Volumes d'impressions et UU incluant toutes les audiences et individus non profilés

            (SELECT replace(date(dh_serv),'-','') as Date, site_name,count(unique(USER_id)) as UU_Tot, count(user_id) as Imp
            FROM [datamining-1184:AXA_ES.wcm_impressionvisibility_%s] 
            where campain_id ="19"
            group by Date,site_name) as d

        on a.Date = d.Date and a.site_name = d.site_name)


""" %(targetDateBQ, targetDateBQ, targetDate93BQ, targetDateBQ, targetDateBQ, targetDate93BQ, targetDateBQ, targetDateBQ, targetDateBQ, targetDate93BQ, targetDateBQ, targetDateBQ)

#print(query)

table_destination_name = 'DASHBOARD_AEW3_PUBLISHERS_AUDIENCES'
try:
    table=dataset.table(name=table_destination_name)
    r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
    job = bgclient.run_async_query('%s_%s'% (table_destination_name,r), query) 
    job.destination = table 
    job.write_disposition = 'WRITE_APPEND'
    job.allow_large_results = True
    job.begin()
    cont=0
    while cont<600:
        cont+=1
        job.reload()
        if job.ended:
            if not job.errors:
                print('job done, table created')
                break
            else:
                print('error in job')
                break
        time.sleep(5)
        print('job running, elapsed time:'+str(cont*5)+'s')

        if not job.ended:
            print('Job not finished ... The script has to be restarted')
except Exception as e:
    print("ERROR TABLE CREATION 1: %s"%e)









################################################ TABLE 2: DASHBOARD_AEW3_PUBLISHERS_ImpUU ################################################
query = """
        Select a.site_name AS site_name, Imp as Imp, UU_Exp as UU_Exp, Imp/UU_Exp as Pression, '%s' as DSource
        from (SELECT site_name,count(*) as Imp
              FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_] , timestamp('20171121'), timestamp('%s'))
              where campain_id='19'
              group by site_name
              ) a
        inner join (Select site_name, count(*) UU_Exp
                    from (SELECT site_name , user_id
                          FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_] , timestamp('20171121'), timestamp('%s'))
                          where campain_id='19'
                          group by site_name, user_id
                          )
                    group by site_name
                    )b on a.site_name=b.site_name

         """ % (targetDateBQ,targetDateBQ,targetDateBQ)



target_table='DASHBOARD_AEW3_PUBLISHERS_ImpUU'
try:
    table=dataset.table(name=target_table)
    r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
    job = bgclient.run_async_query('%s_%s'% (target_table,r), query) 
    job.destination = table 
    job.write_disposition = 'WRITE_TRUNCATE'
    job.allow_large_results = True
    job.begin()
    cont=0
    while cont<600:
        cont+=1
        job.reload()
        if job.ended:
            if not job.errors:
                print('job done, table created')
                break
            else:
                print('error in job')
            break
        time.sleep(5)
        print('job running, elapsed time:'+str(cont*5)+'s')

        if not job.ended:
            print('Job not finished ... The script has to be restarted')
except Exception as e:
    print("ERROR TABLE CREATION 2: %s"%e)



target_table='DASHBOARD_AEW3_PUBLISHERS_ImpUU_historique'
try:
    table=dataset.table(name=target_table)
    r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
    job = bgclient.run_async_query('%s_%s'% (target_table,r), query) 
    job.destination = table 
    job.write_disposition = 'WRITE_APPEND'
    job.allow_large_results = True
    job.begin()
    cont=0
    while cont<600:
        cont+=1
        job.reload()
        if job.ended:
            if not job.errors:
                print('job done, table created')
                break
            else:
                print('error in job')
            break
        time.sleep(5)
        print('job running, elapsed time:'+str(cont*5)+'s')

        if not job.ended:
            print('Job not finished ... The script has to be restarted')
except Exception as e:
    print("ERROR TABLE CREATION 2: %s"%e)











################################################ TABLE 3: DASHBOARD_AEW3_PUBLISHERS_UUfullPub_UUAud_WAMPart ################################################
query = """


############################## SQL QUERY HERE
Select site_name,a.Audience as Audience, UUfullPub, UUAud, UUfullPub/UUAud as WAMPart, '%s' as dinsert
from (Select  *
      from (Select site_name,Audience, count(*) as UUfullPub
            from (Select weboid,Audience, site_name, row_number() over (partition by weboid,site_name/*,Audience*/ order by diff asc) as Rang
                  from (Select weboid,Audience, site_name, datediff(DH_SERV,ddatasource) diff
                        from (SELECT user_id, site_name, date(DH_SERV) as DH_SERV
                              FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_],timestamp('%s'),timestamp('%s'))
                              where campain_id = "19"
                              group by user_id,site_name,DH_SERV
                              ) as a
                        inner join (select weboid,
                                           CASE WHEN Audience in (1) THEN 'G0'
                                           WHEN Audience in (2) THEN 'PEKIN'
                                           WHEN Audience in (3) THEN 'MEJICO'
                                           WHEN Audience in (4,5) THEN 'PARIS'
                                           WHEN Audience in (6) THEN 'JARAMA'
                                           WHEN Audience in (7,8) THEN 'DETROIT'
                                           WHEN Audience in (9,10) THEN 'PAMPLONA'
                                           WHEN Audience in (11) THEN 'MADRID'
                                           WHEN Audience in (12,13) THEN 'BARCELONA'
                                           END AS Audience,
                                           date(ddatasource) as ddatasource
                                    FROM table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_], timestamp('%s'), timestamp('%s'))
                                    where AUDIENCE <>1 ########################## FILTRE AUDIENCE 1
                                    ) as b on a.user_id=b.weboid
                        )
                 )
            where Rang=1
            group by site_name,Audience
            )
            ,
            (Select site_name,Audience, count(*) as UUfullPub
             from (select site_name,weboid,Audience
                  from(Select 'AllPub' site_name,weboid,Audience
                        from (Select weboid,Audience, site_name, row_number() over (partition by weboid,site_name/*,Audience*/ order by diff asc) as Rang
                              from (Select weboid,Audience, site_name, datediff(DH_SERV,ddatasource) diff
                                    from (SELECT user_id, site_name, date(DH_SERV) as DH_SERV
                                          FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_],timestamp('%s'),timestamp('%s'))
                                          where campain_id = "19"
                                          group by user_id,site_name,DH_SERV
                                          ) as a
                                    inner join (select weboid,
                                                       CASE WHEN Audience in (1) THEN 'G0'
                                                       WHEN Audience in (2) THEN 'PEKIN'
                                                       WHEN Audience in (3) THEN 'MEJICO'
                                                       WHEN Audience in (4,5) THEN 'PARIS'
                                                       WHEN Audience in (6) THEN 'JARAMA'
                                                       WHEN Audience in (7,8) THEN 'DETROIT'
                                                       WHEN Audience in (9,10) THEN 'PAMPLONA'
                                                       WHEN Audience in (11) THEN 'MADRID'
                                                       WHEN Audience in (12,13) THEN 'BARCELONA'
                                                       END AS Audience,
                                                       date(ddatasource) as ddatasource
                                                FROM table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_], timestamp('%s'), timestamp('%s'))
                                                where AUDIENCE <>1 ########################## FILTRE AUDIENCE 1
                                                ) as b on a.user_id=b.weboid
                                    )
                             )
                        where Rang=1
                        group by site_name,Audience,weboid
                        )
                   group by site_name,weboid,Audience
                   )
          group by site_name,Audience
          )
      )a
inner join (SELECT CASE WHEN Audience in (1) THEN 'G0'
                        WHEN Audience in (2) THEN 'PEKIN'
                        WHEN Audience in (3) THEN 'MEJICO'
                        WHEN Audience in (4,5) THEN 'PARIS'
                        WHEN Audience in (6) THEN 'JARAMA'
                        WHEN Audience in (7,8) THEN 'DETROIT'
                        WHEN Audience in (9,10) THEN 'PAMPLONA'
                        WHEN Audience in (11) THEN 'MADRID'
                        WHEN Audience in (12,13) THEN 'BARCELONA'
                        END AS Audience, 
                              count(weboid) UUAud
                          from (select weboid, audience, row_number() over (partition by weboid order by ddatasource desc) as Rang
                            FROM table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_], timestamp('%s'), timestamp('%s'))
                        where AUDIENCE <>1 ########################## FILTRE AUDIENCE 1
                            )
                          where rang=1
                          group by Audience)b on a.Audience=b.audience



############################## END QUERY



""" %(targetDateBQ, targetDate93BQ, targetDateBQ, targetDate93BQ, targetDateBQ, targetDate93BQ, targetDateBQ, targetDate93BQ, targetDateBQ, targetDate93BQ, targetDateBQ)

#print(query) #CHECK SCRIPT IS WORKING

### TABLE 3 TRUNCATE
table_destination_name = 'DASHBOARD_AEW3_PUBLISHERS_UUfullPub_UUAud_WAMPart'
try:
    table=dataset.table(name=table_destination_name)
    r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
    job = bgclient.run_async_query('%s_%s'% (table_destination_name,r), query) 
    job.destination = table 
    job.write_disposition = 'WRITE_TRUNCATE'
    job.allow_large_results = True
    job.begin()
    cont=0
    while cont<60:
        cont+=1
        job.reload()
        if job.ended:
            if not job.errors:
                print('job done, table created')
                break
            else:
                print('error in job')
                break
        time.sleep(5)
        print('job running, elapsed time:'+str(cont*5)+'s')

    if not job.ended:
        print('Job not finished ... The script has to be restarted')
except Exception as e:
    print("ERROR TABLE CREATION 3: %s"%e)

### TABLE 3 HISTORIQUE APPEND
table_destination_name = 'DASHBOARD_AEW3_PUBLISHERS_UUfullPub_UUAud_WAMPart_historique'
try:
    table=dataset.table(name=table_destination_name)
    r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
    job = bgclient.run_async_query('%s_%s'% (table_destination_name,r), query) 
    job.destination = table 
    job.write_disposition = 'WRITE_APPEND'
    job.allow_large_results = True
    job.begin()
    cont=0
    while cont<60:
        cont+=1
        job.reload()
        if job.ended:
            if not job.errors:
                print('job done, table created')
                break
            else:
                print('error in job')
                break
        time.sleep(5)
        print('job running, elapsed time:'+str(cont*5)+'s')

    if not job.ended:
        print('Job not finished ... The script has to be restarted')
except Exception as e:
    print("ERROR TABLE CREATION 3: %s"%e)










################################################ TABLE 4: DASHBOARD_AEW3_PUBLISHERS_MATCH_RATE ################################################
query = """



############################## SQL QUERY HERE


select a.site_name site_name, a.UUfullPub as UUfullPub, 
a.dinsert as dinsert_UUfullPub, 
b.UU_Exp as UU_Exp, 
timestamp(b.DSource) as dinstert_UUExp,
UUfullPub/ UU_Exp as match_rate_whole_period

from
    (SELECT site_name, dinsert, sum(UUfullPub) UUfullPub
    FROM [datamining-1184:AXA_ES.DASHBOARD_AEW3_PUBLISHERS_UUfullPub_UUAud_WAMPart]
    group by site_name, dinsert)a
inner join
    (SELECT site_name, UU_Exp, DSource
    FROM [datamining-1184:AXA_ES.DASHBOARD_AEW3_PUBLISHERS_ImpUU])b
on a.site_name = b.site_name


############################## END QUERY
"""



### TABLE 4 TRUNCATE
table_destination_name = 'DASHBOARD_AEW3_PUBLISHERS_MATCH_RATE'
try:
    table=dataset.table(name=table_destination_name)
    r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
    job = bgclient.run_async_query('%s_%s'% (table_destination_name,r), query) 
    job.destination = table 
    job.write_disposition = 'WRITE_TRUNCATE'
    job.allow_large_results = True
    job.begin()
    cont=0
    while cont<60:
        cont+=1
        job.reload()
        if job.ended:
            if not job.errors:
                print('job done, table created')
                break
            else:
                print('error in job')
                break
        time.sleep(5)
        print('job running, elapsed time:'+str(cont*5)+'s')

    if not job.ended:
        print('Job not finished ... The script has to be restarted')
except Exception as e:
    print("ERROR TABLE CREATION 3: %s"%e)

### TABLE 4 HISTORIQUE APPEND
table_destination_name = 'DASHBOARD_AEW3_PUBLISHERS_MATCH_RATE_HISTORIQUE'
try:
    table=dataset.table(name=table_destination_name)
    r=str(random.randint(1,1000000)) # unique JOB ID in BigQuery
    job = bgclient.run_async_query('%s_%s'% (table_destination_name,r), query) 
    job.destination = table 
    job.write_disposition = 'WRITE_APPEND'
    job.allow_large_results = True
    job.begin()
    cont=0
    while cont<60:
        cont+=1
        job.reload()
        if job.ended:
            if not job.errors:
                print('job done, table created')
                break
            else:
                print('error in job')
                break
        time.sleep(5)
        print('job running, elapsed time:'+str(cont*5)+'s')

    if not job.ended:
        print('Job not finished ... The script has to be restarted')
except Exception as e:
    print("ERROR TABLE CREATION 3: %s"%e)