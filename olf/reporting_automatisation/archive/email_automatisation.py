
# coding: utf-8

# In[18]:


# coding: utf-8
from google.cloud import bigquery
import smtplib
import subprocess
import time
import datetime
import os



os.chdir("C:\\Users\\lmerzetti.WEBORAMA")
os.chdir("Desktop")



#GET THE START DATE
start_date = (datetime.date.today() - datetime.timedelta(float(7))) # CORRECTION: pd.Timedelta cannot accept np.int64 on python 3.3 or 3.4, however np.float64 does work.
start_date_str = str(start_date)
start_date_bq = start_date_str.replace('-','')
# print("I am calculating from the date: " + start_date_bq)

#GET THE END DATE
end_date = (datetime.date.today() - datetime.timedelta(float(1))) # CORRECTION: pd.Timedelta cannot accept np.int64 on python 3.3 or 3.4, however np.float64 does work.
end_date_str = str(end_date)
end_date_bq = end_date_str.replace('-','')
# print("I am calculating up to the date: " + end_date_bq)



# SEND REPORT TO STORAGE
os.system("gsutil cp report_axa_%s_%s.xlsx gs://luca_merzetti/report_axa_%s_%s.xlsx"%(start_date_bq, end_date_bq, start_date_bq, end_date_bq))



# GET FILES NAMES IN STORAGE
def get_filenames(bucket_name):
        cmd = "gs://%s" %(bucket_name)

        filenames = subprocess.check_output('gsutil ls '+cmd, shell=True).decode("utf-8").split('\n')
        filenames_list = []

        for i in range(len(filenames)):
            filenames_list.append(filenames[i].replace(cmd,''))

        filenames_list = [x for x in filenames_list if x]

        return filenames_list



def monitoring_alert (task):
    sender = os.environ['EMAIL_USER']
    receivers = [os.environ['EMAIL_USER']]

    for receiver in receivers:
        message = 'From: From Person <' + sender + '>\nTo: To Person <' + receiver + '>\nSubject: TEST \n\n' + task
        pwd = os.environ['EMAIL_PASSWORD']

        smtpObj = smtplib.SMTP('smtp.googlemail.com', 587)
        smtpObj.starttls()
        smtpObj.login(os.environ['EMAIL_USER'], pwd)
        smtpObj.sendmail(sender, receiver, message)
        smtpObj.quit()



def check_and_send(date_start, date_end, files_list):
    # print("/report_axa_%s_%s.xlsx\\r"%(date_start,date_end))
    if "/report_axa_%s_%s.xlsx\r"%(date_start, date_end) in files_list:
        monitoring_alert('You will find the report here:  \n\nEnjoy your day!')
    else:
        monitoring_alert('The report is not yet there:  \n\nEnjoy your day!')



# TEST
# monitoring_alert ('This is a test email \n\nEnjoy your day!')



fl = get_filenames("luca_merzetti")
check_and_send(start_date_bq, end_date_bq, fl)

