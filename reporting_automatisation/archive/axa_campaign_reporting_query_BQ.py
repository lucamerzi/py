
# coding: utf-8

# In[6]:


# coding: utf-8

# In[34]:


import numpy as np
import pandas as pd

import random
import time
import datetime
import os

from gcloud import datastore, bigquery, storage


# In[35]:


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


# In[36]:


#cd Desktop/


# In[37]:

bgclient = bigquery.Client(project=project)
stclient = storage.Client(project=project)
project= 'datamining-1184'
def query_BQ (query):
    query_results = bgclient.run_sync_query(query)
    query_results.use_legacy_sql = True
    query_results.timeout_ms=300000
    query_results.run()
    final_rows=[]
    page_token=None
    while True:
        rows, total_rows, page_token = query_results.fetch_data(max_results=10000,page_token=page_token)
        for row in rows:
            final_rows.append(row)
        if not page_token:
            break
    return pd.DataFrame(final_rows,columns=[a.name for a in query_results.schema])


    
    df=query_BQ(query_string)   
    print(len(df))
    df.head()


# In[38]:


dmp_volume = """
			SELECT
			'"""+start_date_bq+"""' as date_begin, 
			'"""+end_date_bq+"""' as date_end,
			"ALL" as campaign_id,
			audience as audience,
			"DMP_volume" AS metric, 
			"" as split, 
			volwebo as value
			FROM
				(SELECT 
				CASE WHEN Audience in (1) THEN 'G0'
				WHEN Audience in (2) THEN 'PEKIN'
				WHEN Audience in (3) THEN 'MEXICO'
				WHEN Audience in (4,5) THEN 'PARIS'
				WHEN Audience in (6) THEN 'JARAMA'
				WHEN Audience in (7,8) THEN 'DETROIT'
				WHEN Audience in (9,10) THEN 'PAMPLONA'
				WHEN Audience in (11) THEN 'MADRID'
				WHEN Audience in (12,13) THEN 'BARCELONA'
				END AS audience, 
				count(*) VolWebo
				FROM
					(SELECT weboid, audience, ddatasource, row_number() over (partition by weboid order by ddatasource desc) rang
					FROM
						(
						select weboid, audience, ddatasource
						FROM table_date_range([datamining-1184:AXA_ES.AEW3_ProjRemade8_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
						where audience <>1 
						--AND weboid = "---1rRfyah3-"
						group by weboid, audience, ddatasource
						)
					)
				WHERE rang = 1
				GROUP BY Audience
				order by Audience)
			"""


# In[39]:


# UU
UU = """
select 
'"""+start_date_bq+"""' as date_begin, 
'"""+end_date_bq+"""' as date_end,
campain_id as campaign_id,
case 
when campain_id = "28" then "Jarama_HH"
when campain_id = "29" then "Madrid_HH"
when campain_id = "30" then "Barcelona_PE"
when campain_id = "32" then "Mexico_CO"
when campain_id = "33" then "Paris_CO"
when campain_id = "34" then "Pamplona_HE"
when campain_id = "36" then "Jarama_AUTO"
when campain_id = "37" then "Detroit_AUTO"
END AS audience, 
"UU" AS metric, 
"" as split, 
count(user_id) value,
FROM      
    (
    SELECT user_id, campain_id
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37")
    group by user_id, campain_id
    )
group by campaign_id, audience
order by campaign_id, audience
"""


# In[40]:


# UU_publisher
UU_publisher = """
select 
'"""+start_date_bq+"""' as date_begin, 
'"""+end_date_bq+"""' as date_end,
campain_id as campaign_id,
case 
when campain_id = "28" then "Jarama_HH"
when campain_id = "29" then "Madrid_HH"
when campain_id = "30" then "Barcelona_PE"
when campain_id = "32" then "Mexico_CO"
when campain_id = "33" then "Paris_CO"
when campain_id = "34" then "Pamplona_HE"
when campain_id = "36" then "Jarama_AUTO"
when campain_id = "37" then "Detroit_AUTO"
END AS audience, 
"UU_publisher" AS metric, 
Publisher as split, 
uu as value
from
    (SELECT campain_id, 'A3Media' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%A3Media%"
    group by  campain_id,Publisher
    ),
    (SELECT campain_id, 'Adman' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Adman%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'AOL' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%AOL%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Bluemedia' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Bluemedia%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Eltiempo' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Eltiempo.es%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Godo' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Godo%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Mediaset' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Mediaset%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Yume' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Yume%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Prisa' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Prisa%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Schibsted' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Schibsted%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Seedtag' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Seedtag%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Smartclip' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Smartclip%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Sunmedia' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Sunmedia%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Unidad_Editorial' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Unidad Editorial%"
    group by campain_id,Publisher
    ),    
    (SELECT campain_id, 'Vocento' as Publisher,count(unique(user_id)) uu
    FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
    where campain_id IN ("28","29","30","32","33","34","36","37") and Site_name LIKE "%Vocento%"
    group by campain_id,Publisher
    )
group by campaign_id, audience, split, value
order by campaign_id, split
"""


# In[41]:


# UU_DSP
UU_DSP = """
select 
'"""+start_date_bq+"""' as date_begin, 
'"""+end_date_bq+"""' as date_end,
campain_id as campaign_id,
case 
when campain_id = "28" then "Jarama_HH"
when campain_id = "29" then "Madrid_HH"
when campain_id = "30" then "Barcelona_PE"
when campain_id = "32" then "Mexico_CO"
when campain_id = "33" then "Paris_CO"
when campain_id = "34" then "Pamplona_HE"
when campain_id = "36" then "Jarama_AUTO"
when campain_id = "37" then "Detroit_AUTO"
END AS audience, 
"UU_DSP" AS metric, 
Publisher as split, 
uu as value
from
	(SELECT campain_id, 'MM' as Publisher,count(unique(user_id)) uu
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37") 
	and insertion_name like "%_MM%"
	group by  campain_id,Publisher
	),
	(SELECT campain_id, 'DBM' as Publisher,count(unique(user_id)) uu
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37") 
	and insertion_name like "%_DBM%"
	group by campain_id,Publisher
	),    
	(SELECT campain_id, 'APX' as Publisher,count(unique(user_id)) uu
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37") 
	and insertion_name like "%_Appn%"
	group by campain_id,Publisher
	)
group by campaign_id, audience, split, value
order by campaign_id
"""


# In[42]:


# freq_format
freq_format = """
select 
'"""+start_date_bq+"""' as date_begin, 
'"""+end_date_bq+"""' as date_end,
a.campain_id campaign_id,
case 
when a.campain_id = "28" then "Jarama_HH"
when a.campain_id = "29" then "Madrid_HH"
when a.campain_id = "30" then "Barcelona_PE"
when a.campain_id = "32" then "Mexico_CO"
when a.campain_id = "33" then "Paris_CO"
when a.campain_id = "34" then "Pamplona_HE"
when a.campain_id = "36" then "Jarama_AUTO"
when a.campain_id = "37" then "Detroit_AUTO"
END AS audience, 
"pression_format" AS metric, 
b.imp / a.UU value, 
b.format split
from
	(select campain_id, Format, count(user_id) UU
	FROM      
		(
		SELECT user_id, campain_id,
		CASE 
		WHEN Insertion_Name like "%Display%" or Insertion_Name like "%120x600%" OR Insertion_Name LIKE "%160x600%" OR Insertion_Name LIKE "%300x250%" OR Insertion_Name LIKE "%300x600%" OR Insertion_Name LIKE "%728x90%" OR Insertion_Name LIKE "%970x250%" OR Insertion_Name LIKE "%980x90%" then "Display"
		WHEN Insertion_Name like "%Video%" or Insertion_Name like "%NativeAd%" OR Insertion_Name LIKE "%PreRoll%" OR Insertion_Name LIKE "%Inread%" Then "Video" 
		end as Format,
		FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
		where campain_id IN ("28","29","30","32","33","34","36","37")
		group by user_id, campain_id, Format
		)
	group by campain_id, Format)a
inner join
	(
	SELECT campain_id, count(*) imp,
	CASE 
	WHEN Insertion_Name like "%Display%" or Insertion_Name like "%120x600%" OR Insertion_Name LIKE "%160x600%" OR Insertion_Name LIKE "%300x250%" OR Insertion_Name LIKE "%300x600%" OR Insertion_Name LIKE "%728x90%" OR Insertion_Name LIKE "%970x250%" OR Insertion_Name LIKE "%980x90%" then "Display"
	WHEN Insertion_Name like "%Video%" or Insertion_Name like "%NativeAd%" OR Insertion_Name LIKE "%PreRoll%" OR Insertion_Name LIKE "%Inread%" Then "Video"
	end as Format,
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37")
	group by campain_id, Format
	)b
on a.campain_id = b.campain_id and a.Format = b.format 

group by campaign_id, audience, value, split
order by campaign_id, split
"""


# In[43]:


# exc_DSP
exc_DSP = """
select 
'"""+start_date_bq+"""' as date_begin, 
'"""+end_date_bq+"""' as date_end,
campain_id as campaign_id,
case 
when campain_id = "28" then "Jarama_HH"
when campain_id = "29" then "Madrid_HH"
when campain_id = "30" then "Barcelona_PE"
when campain_id = "32" then "Mexico_CO"
when campain_id = "33" then "Paris_CO"
when campain_id = "34" then "Pamplona_HE"
when campain_id = "36" then "Jarama_AUTO"
when campain_id = "37" then "Detroit_AUTO"
END AS audience, 
"UU_DSP" AS metric, 
Publisher as split, 
uu as value
from
	(SELECT campain_id, 'MM' as Publisher,count(unique(user_id)) uu
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37") 
	and insertion_name like "%_MM%"
	group by  campain_id,Publisher
	),
	(SELECT campain_id, 'DBM' as Publisher,count(unique(user_id)) uu
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37") 
	and insertion_name like "%_DBM%"
	group by campain_id,Publisher
	),    
	(SELECT campain_id, 'APX' as Publisher,count(unique(user_id)) uu
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37") 
	and insertion_name like "%_Appn%"
	group by campain_id,Publisher
	)
group by campaign_id, audience, split, value
order by campaign_id
"""


# In[44]:


# exc_format
exc_format = """
select
'"""+start_date_bq+"""' as date_begin,
'"""+end_date_bq+"""' as date_end,
campain_id as campaign_id,
case 
when campain_id = "28" then "Jarama_HH"
when campain_id = "29" then "Madrid_HH"
when campain_id = "30" then "Barcelona_PE"
when campain_id = "32" then "Mexico_CO"
when campain_id = "33" then "Paris_CO"
when campain_id = "34" then "Pamplona_HE"
when campain_id = "36" then "Jarama_AUTO"
when campain_id = "37" then "Detroit_AUTO"
END AS audience,
"UU_exc_format" as metric,
sum(video) video, sum(display) display
from
    (select user_id, video, display, video + display as sum, campain_id
    from
        (
        select campain_id, user_id, sum(Video) Video, sum(display) Display
        from
            (
            SELECT user_id, campain_id,
            CASE WHEN Insertion_Name like "%Display%" or Insertion_Name like "%120x600%" OR Insertion_Name LIKE "%160x600%" OR Insertion_Name LIKE "%300x250%" OR Insertion_Name LIKE "%300x600%" OR Insertion_Name LIKE "%728x90%" OR Insertion_Name LIKE "%970x250%" OR Insertion_Name LIKE "%980x90%" Then 1 else 0 end as Display,
            CASE WHEN Insertion_Name like "%Video%" or Insertion_Name like "%NativeAd%" OR Insertion_Name LIKE "%PreRoll%" OR Insertion_Name LIKE "%Inread%" Then 1 else 0 end as Video
            FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+start_date_bq+"""'))
            where campain_id IN ("28","29","30","32","33","34","36","37")
            group by user_id, display, video, campain_id
            )
        group by user_id, campain_id
        )
    )
where sum = 1
group by campaign_id, audience
order by campaign_id, audience
"""


# In[45]:


# exc_publisher
exc_publisher = """
select 
'"""+start_date_bq+"""' as date_begin, 
'"""+end_date_bq+"""' as date_end,
campain_id as campaign_id,
case 
when campain_id = "28" then "Jarama_HH"
when campain_id = "29" then "Madrid_HH"
when campain_id = "30" then "Barcelona_PE"
when campain_id = "32" then "Mexico_CO"
when campain_id = "33" then "Paris_CO"
when campain_id = "34" then "Pamplona_HE"
when campain_id = "36" then "Jarama_AUTO"
when campain_id = "37" then "Detroit_AUTO"
END AS audience, 
"UU_exc_publisher" AS metric, 
Publisher as split, 
uu as value
FROM
	(SELECT CASE
	WHEN A3Media =1 THEN "A3Media" 
	WHEN Adman =1 THEN "Adman"
	WHEN AOL =1 THEN "AOL"
	WHEN Bluemedia =1 THEN "Bluemedia"
	WHEN Eltiempo =1 THEN "Eltiempo"
	WHEN Godo =1 THEN "Godo"
	WHEN Mediaset =1 THEN "Mediaset"
	WHEN Prisa =1 THEN "Prisa"
	WHEN Schibsted =1 THEN "Schibsted"
	WHEN Seedtag =1 THEN "Seedtag"
	WHEN Smartclip =1 THEN "Smartclip"
	WHEN Sunmedia =1 THEN "Sunmedia"
	WHEN Unidad_Editorial =1 THEN "Unidad_Editorial"
	WHEN Vocento =1 THEN "Vocento"
	WHEN Yume =1 THEN "Yume"
	END AS Publisher,
	UU,
	campain_id
	FROM    
		(SELECT
		campain_id,
		A3Media,Adman,AOL,Bluemedia,Eltiempo,Godo,Mediaset,Prisa,Schibsted,Seedtag,Smartclip,Sunmedia,Unidad_Editorial,Vocento,Yume,
		A3Media + Adman + AOL + Bluemedia + Eltiempo + Godo + Mediaset + Prisa + Schibsted + Seedtag + Smartclip + Sunmedia + Unidad_Editorial + Vocento + Yume AS TOT,
		UU
		FROM    
			(SELECT campain_id, A3Media,Adman,AOL,Bluemedia,Eltiempo,Godo,Mediaset,Prisa,Schibsted,Seedtag,Smartclip,Sunmedia,Unidad_Editorial,Vocento,Yume,count(*)UU
			FROM
				(SELECT
				user_id,
				campain_id,
				SUM(A3Media)A3Media,SUM(Adman)Adman,SUM(AOL)AOL,SUM(Bluemedia)Bluemedia,SUM(Eltiempo)Eltiempo,SUM(Godo)Godo,SUM(Mediaset)Mediaset,SUM(Prisa)Prisa,SUM(Schibsted)Schibsted,SUM(Seedtag)Seedtag,SUM(Smartclip)Smartclip,SUM(Sunmedia)Sunmedia,SUM(Unidad_Editorial)Unidad_Editorial,SUM(Vocento)Vocento,SUM(Yume)Yume
				FROM
					(
					SELECT user_id,
					campain_id,
					CASE WHEN Site_name LIKE "%A3Media%" THEN 1 ELSE 0 END AS A3Media,
					CASE WHEN Site_name LIKE "%Adman%" THEN 1 ELSE 0 END AS Adman,
					CASE WHEN Site_name LIKE "%AOL%" THEN 1 ELSE 0 END AS AOL,
					CASE WHEN Site_name LIKE "%Bluemedia%" THEN 1 ELSE 0 END AS Bluemedia,
					CASE WHEN Site_name LIKE "%Eltiempo.es%" THEN 1 ELSE 0 END AS Eltiempo,
					CASE WHEN Site_name LIKE "%Godo%" THEN 1 ELSE 0 END AS Godo,
					CASE WHEN Site_name LIKE "%Mediaset%" THEN 1 ELSE 0 END AS Mediaset,
					CASE WHEN Site_name LIKE "%Prisa%" THEN 1 ELSE 0 END AS Prisa,
					CASE WHEN Site_name LIKE "%Schibsted%" THEN 1 ELSE 0 END AS Schibsted,
					CASE WHEN Site_name LIKE "%Seedtag%" THEN 1 ELSE 0 END AS Seedtag,
					CASE WHEN Site_name LIKE "%Smartclip%" THEN 1 ELSE 0 END AS Smartclip,
					CASE WHEN Site_name LIKE "%Sunmedia%" THEN 1 ELSE 0 END AS Sunmedia,
					CASE WHEN Site_name LIKE "%Unidad Editorial%" THEN 1 ELSE 0 END AS Unidad_Editorial,
					CASE WHEN Site_name LIKE "%Vocento%" THEN 1 ELSE 0 END AS Vocento,
					CASE WHEN Site_name LIKE "%Yume%" THEN 1 ELSE 0 END AS Yume
					FROM (
						  SELECT user_id, Site_name, campain_id
						  FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
						  where campain_id IN ("28","29","30","32","33","34","36","37")
						  group by user_id, Site_name, campain_id
						 )
					GROUP BY user_id,campain_id,A3Media,Adman,AOL,Bluemedia,Eltiempo,Godo,Mediaset,Prisa,Schibsted,Seedtag,Smartclip,Sunmedia,Unidad_Editorial,Vocento,Yume  
					)
				GROUP BY user_id, campain_id)
			GROUP BY A3Media,Adman,AOL,Bluemedia,Eltiempo,Godo,Mediaset,Prisa,Schibsted,Seedtag,Smartclip,Sunmedia,Unidad_Editorial,Vocento,Yume, campain_id)
		HAVING TOT = 1))
order by campaign_id, split
"""


# In[46]:


# UU_format
UU_format = """
select 
'"""+start_date_bq+"""' as date_begin, 
'"""+end_date_bq+"""' as date_end,
campain_id as campaign_id,
case 
when campain_id = "28" then "Jarama_HH"
when campain_id = "29" then "Madrid_HH"
when campain_id = "30" then "Barcelona_PE"
when campain_id = "32" then "Mexico_CO"
when campain_id = "33" then "Paris_CO"
when campain_id = "34" then "Pamplona_HE"
when campain_id = "36" then "Jarama_AUTO"
when campain_id = "37" then "Detroit_AUTO"
END AS audience, 
"UU_format" AS metric, 
Publisher as split, 
uu as value
from
	(SELECT campain_id, 'Video' as Publisher,count(unique(user_id)) uu
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37") 
	and (Insertion_Name like "%Video%" or Insertion_Name like "%NativeAd%" OR Insertion_Name LIKE "%PreRoll%" OR Insertion_Name LIKE "%Inread%")
	group by  campain_id,Publisher
	),
	(SELECT campain_id, 'Display' as Publisher,count(unique(user_id)) uu
	FROM table_date_range([datamining-1184:AXA_ES.wcm_impressionvisibility_], timestamp('"""+start_date_bq+"""'), timestamp('"""+end_date_bq+"""'))
	where campain_id IN ("28","29","30","32","33","34","36","37") 
	and (Insertion_Name like "%Display%" or Insertion_Name like "%120x600%" OR Insertion_Name LIKE "%160x600%" OR Insertion_Name LIKE "%300x250%" OR Insertion_Name LIKE "%300x600%" OR Insertion_Name LIKE "%728x90%" OR Insertion_Name LIKE "%970x250%" OR Insertion_Name LIKE "%980x90%")
	group by campain_id,Publisher
	)
group by campaign_id, audience, split, value
order by campaign_id, split
"""


# In[47]:


# executes the query and DL the result as a pandas dataframe
df_dmp_volume = query_BQ(dmp_volume)
df_UU = query_BQ(UU)
df_UU_publisher = query_BQ(UU_publisher)
df_UU_DSP = query_BQ(UU_DSP)
df_freq_format = query_BQ(freq_format)
df_exc_DSP = query_BQ(exc_DSP)
df_exc_format = query_BQ(exc_format)
df_exc_publisher = query_BQ(exc_publisher)
df_UU_format = query_BQ(UU_format)


# In[52]:


#filepath where to write the excel
filepath = "report_axa_" + start_date_bq + "_" + end_date_bq + ".xlsx"


# In[53]:


print("Writing to file: "+str(filepath))
writer=pd.ExcelWriter(filepath, engine='xlsxwriter')


# In[54]:


df_dmp_volume.to_excel(writer, sheet_name="dmp_volume",index=False)
df_UU.to_excel(writer, sheet_name="UU",index=False)
df_UU_publisher.to_excel(writer, sheet_name="UU_publisher",index=False)

df_UU_DSP.to_excel(writer, sheet_name="UU_DSP",index=False)
df_freq_format.to_excel(writer, sheet_name="freq_format",index=False)
df_exc_DSP.to_excel(writer, sheet_name="exc_DSP",index=False)

df_exc_format.to_excel(writer, sheet_name="exc_format",index=False)
df_exc_publisher.to_excel(writer, sheet_name="exc_publisher",index=False)
df_UU_format.to_excel(writer, sheet_name="UU_format",index=False)


# In[55]:


#important to be executed at the end
writer.close()

