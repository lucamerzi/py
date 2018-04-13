import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os


def date_converter(date_as_string):
    """ INPUT FORMAT: Month( .@)Year as a string. ex: 'Jan.@2010' 'Mars @2010' 
    """

    dic_corres_month={'Jan.':'01',
                        'Fév.':'02',
                        'Mars':'03',
                        'Avr.':'04',
                        'Mai ':'05', 
                        'Juin':'06', 
                        'Jul.':'07', 
                        'Août':'08',
                        'Sep.':'09',
                        'Oct.':'10',
                        'Nov.':'11',
                        'Déc.':'12'}

    month=dic_corres_month[date_as_string[:4]]

    year=date_as_string.split('@')[1]

    date=str(year+'-'+month+'-01 00:00:00 UTC')

    return date



########################################################################################################################################################################
#########################################################                     SOCIODEMO                     ############################################################
########################################################################################################################################################################

######################################################### STEP 1: PROCESS FILE & EXPORT TO CSV

FileName=''

data_acheteurs=pd.read_csv('G:/My Drive/BU DATA/Clients - Prospects/AAA/Dashboard_Automotive_Insights/AAA-Data-Processing/Raw Data/'+FileName+'.csv',encoding='latin-1',sep=';',index_col=False)
data_acheteurs['DATE']=data_acheteurs.DATE.apply(lambda x: date_converter(x))
data_acheteurs['TYPE_LOCAT_PRF']=data_acheteurs['TYPE_LOCAT_PRF'].replace(' ', 'TRADITIONNEL')
data_acheteurs['SEXE']=data_acheteurs['SEXE'].replace('F', 'Femme')
data_acheteurs['SEXE']=data_acheteurs['SEXE'].replace('M', 'Homme')
data_acheteurs['YEAR']=data_acheteurs.DATE.apply(lambda x: x[:4])

data_clean=data_acheteurs[(data_acheteurs.AGE_PROPRIETAIRE>=18) &  (data_acheteurs.AGE_PROPRIETAIRE<=90) & (data_acheteurs.SEXE!=' ')]

data_clean.to_csv('G:/My Drive/BU DATA/Clients - Prospects/AAA/Dashboard_Automotive_Insights/AAA-Data-Processing/Processed Data/SD_'+FileName+'.csv',index=False)

######################################################### STEP 2: PUSH IN BIGQUERY TABLE

osquery='bq load --noreplace --skip_leading_rows 1 "AAADataPool.OBSA_data_age_volume_" "G:/My Drive/BU DATA/Clients - Prospects/AAA/Dashboard_Automotive_Insights/AAA-Data-Processing/Processed Data/SD_'+FileName+'.csv" "G:/My Drive/BU DATA/Clients - Prospects/AAA/Dashboard_Automotive_Insights/AAA-Data-Processing/Schema/SD.json'

os.system(osquery)



#########################################################################################################################################################################
#########################################################                CARS                      #######################################################################
#########################################################################################################################################################################

######################################################### STEP 1: PROCESS FILE & EXPORT TO CSV

FileName=''
data_modeles=pd.read_csv('G:/My Drive/BU DATA/Clients - Prospects/AAA/Dashboard_Automotive_Insights/AAA-Data-Processing/Raw Data/'+FileName+'.csv',encoding='latin-1',sep=';',index_col=False)

data_clean=data_modeles[~data_modeles.DATE.isin(['Ann.@2001', 'Ann.@2002'])].copy()

data_clean['DATE']=data_clean.DATE.apply(lambda x: date_converter(x))

data_clean['YEAR']=data_clean.DATE.apply(lambda x: x[:4])

data_clean.to_csv('G:/My Drive/BU DATA/Clients - Prospects/AAA/Dashboard_Automotive_Insights/AAA-Data-Processing/Processed Data/Cars_'+FileName+'.csv',index=False)

######################################################### STEP 2: PUSH IN BIGQUERY TABLE

osquery='bq load --noreplace --skip_leading_rows 1 "AAADataPool.OBSA_data_modele_volume_" "G:/My Drive/BU DATA/Clients - Prospects/AAA/Dashboard_Automotive_Insights/AAA-Data-Processing/Processed Data/Cars_'+FileName+'.csv" "G:/My Drive/BU DATA/Clients - Prospects/AAA/Dashboard_Automotive_Insights/AAA-Data-Processing/Schema/Car.json'

os.system(osquery)