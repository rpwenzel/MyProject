import pandas as pd
from pandas import DataFrame
import numpy as np
import MyData
import os.path

global debug,path

debug = False #True or  #False
path =''      #'c:/temp/'

def Ingest(e, df):

    if debug: print('Inside Ingest')

#   PROCESS CUSTOMER NEW AND UPDATE
    if e[0]['type'] == 'CUSTOMER':
        compare_key = e[0]['key']
        i = 0
        compare_key = e[0]['key']
        if debug and e[0]['type'] == 'CUSTOMER':print (e[0]['type'],e[0]['verb'],e[0]['key'],e[0]['event_time'],e[0]['last_name'],e[0]['adr_city'],e[0]['adr_state'])
        if e[0]['type'] == 'CUSTOMER' and e[0]['verb'] == "NEW" and compare_key not in (df['keyc']).to_string(index=False):

            typec            = e[0]['type']
            verbc            = e[0]['verb']
            keyc             = e[0]['key']
            event_timec      = e[0]['event_time']
            last_name        = e[0]['last_name']
            adr_city         = e[0]['adr_city']
            adr_state        = e[0]['adr_state']
            types            = e[1]['type']
            verbs            = e[1]['verb']
            keys             = e[1]['key']
            event_times      = e[1]['event_time']
            customer_ids     = e[1]['customer_id']
            tags             = str(e[1]['tags'])
            typei            = e[2]['type']
            verbi            = e[2]['verb']
            keyi             = e[2]['key']
            event_timei      = e[2]['event_time']
            customer_idi     = e[2]['customer_id']
            camera_make      = e[2]['camera_make']
            camera_model     = e[2]['camera_model']
            typeo            = e[3]['type']
            verbo            = e[3]['verb']
            keyo             = e[3]['key']
            event_timeo      = e[3]['event_time']
            customer_ido     = e[3]['customer_id']
            total_amount     = e[3]['total_amount']

            df2 = df.append(pd.DataFrame([[typec,verbc,keyc,event_timec,last_name,adr_city,adr_state,types,verbs,keys,event_times,customer_ids,tags,typei,verbi,keyi,event_timei,customer_idi,camera_make,camera_model,typeo,verbo,keyo,event_timeo,customer_ido
,total_amount]], columns=df.columns))
            df = df2

        elif  e[0]['type'] == 'CUSTOMER' and e[0]['verb'] == "UPDATE"  and compare_key in (df['keyc']).to_string(index=False):
            df.loc[df['keyc'] == e[0]['key'], ['event_timec', 'last_name', 'adr_city', 'adr_state']] = [e[0]['event_time'], e[0]['last_name'], e[0]['adr_city'], e[0]['adr_state']]

        else:
            print ('Customer Already Exists:compare_key=',compare_key,'found in=',(df['keyc']).to_string(index=False))


#   PROCESS ORDER UPDATE
    elif  e[0]['type'] == 'ORDER' and e[0]['verb'] == "UPDATE":
        compare_key = e[0]['customer_id']
        if compare_key in (df['customer_ido']).to_string(index=False):
            df.loc[df['keyc'] == e[0]['customer_id'], ['keyo','event_timeo', 'total_amount']] = [e[0]['key'], e[0]['event_time'], e[0]['total_amount']]
        if debug and e[0]['type'] == 'ORDER':print (e[0]['type'],e[0]['verb'],e[0]['key'],e[0]['event_time'],e[0]['customer_id'],e[0]['total_amount'])

#   PROCESS ORDER NEW
    elif  e[0]['type'] == 'ORDER' and e[0]['verb'] == "NEW":
        compare_key = e[0]['customer_id']
        if debug and e[0]['type'] == 'ORDER':print (e[0]['type'],e[0]['verb'],e[0]['key'],e[0]['event_time'],e[0]['customer_id'],e[0]['total_amount'])
        print('INCOMLETE - RETURNING')


#   PROCESS SITE_VISIT NEW
    elif e[0]['type'] == 'SITE_VISIT':
        i = 0
        compare_key = e[0]['customer_id']
        df1 = df.loc[df['customer_ids'] == compare_key]
        df2 = df1.loc[df1['customer_ids'] == e[0]['customer_id'], ['keys','event_times', 'tags']] =  [e[0]['key'], e[0]['event_time'], str(e[0]['tags'])]
        df1.append(df2)
        if debug and e[0]['type'] == 'SITE_VISIT':print (i,e[0]['type'],e[0]['verb'],e[0]['key'],e[0]['event_time'],e[0]['customer_id'],e[0]['tags'])


#   PROCESS IMAGE UPLOAD
    else:
        if e[0]['type'] == 'IMAGE':
            i = 0
            compare_key = e[0]['customer_id']
            df1 = df.loc[df['customer_idi'] == compare_key]
            df2 = df1.loc[df1['customer_idi'] == e[0]['customer_id'], ['keyi','event_timei','camera_make','camera_model']] =  [e[0]['key'], e[0]['event_time'], e[0]['camera_make'], e[0]['camera_model']]
            df1.append(df2)
            if debug and e[0]['type'] == 'IMAGE':print (i,e[0]['type'],e[0]['verb'],e[0]['key'],e[0]['event_time'],e[0]['customer_id'],e[0]['camera_make'],e[0]['camera_model'])

    location = path + 'MyOutput_ingest_pandas.csv'
    df.to_csv(location, index = False)
    return(df)

#    INITIALIZE PANDAS USING SAMPLE_INPUT
def Init_pandas():
    if debug: print('inside Init_pandas')

    details = [{"typec": "CUSTOMER", "verbc": "NEW", "keyc": "96f55c7d8f42", "event_timec": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK",
"types": "SITE_VISIT", "verbs": "NEW", "keys": "ac05e815502f", "event_times": "2017-01-06T12:45:52.041Z", "customer_ids": "96f55c7d8f42", "tags": [{"some key": "some value"}],
"typei": "IMAGE", "verbi": "UPLOAD", "keyi": "d8ede43b1d9f", "event_timei": "2017-01-06T12:47:12.344Z", "customer_idi": "96f55c7d8f42", "camera_make": "Canon", "camera_model": "EOS 80D",
"typeo": "ORDER", "verbo": "NEW", "keyo": "68d84e5d1a43", "event_timeo": "2017-01-06T12:55:55.555Z", "customer_ido": "96f55c7d8f42", "total_amount": "12.34 USD"}]

# Initializing DataFrame: df
    df = pd.DataFrame(details)

    location = path + 'MyOutput_init_pandas.csv'
    df.to_csv(location, index = False)
    return(df)

###########################################################

if __name__ == '__main__':
# For simplicity, this program can be rerun, processing all data

    print('Beginning')

#   INITIALIZE Pandas
    df = Init_pandas()

#   GET INPUT RECORDS FROM MyData.py
    filea = MyData.a
    fileb = MyData.b
    filec = MyData.c
    filed = MyData.d
    filee = MyData.e
    filef = MyData.f
    fileg = MyData.g

#   INGEST RECORDS (comment out with records to skip if you so desire)
    df = Ingest(filea,df)  # NEW    CUSTOMER 2
    df = Ingest(fileb,df)  # NEW    CUSTOMER 3
    df = Ingest(filec,df)  # UPDATE CUSTOMER 1
    df = Ingest(filed,df)  # UPDATE ORDER, CUSTOMER 3
    df = Ingest(filee,df)  # NEW SITE_VISIT, CUSTOMER 2
    df = Ingest(filef,df)  # UPLOAD IMAGE, CUSTOMER 3
    df = Ingest(fileg,df)  # NEW ORDER, CUSTOMER 2  -  NOT CODED; Intentionally Returns:   'INCOMPLETE - RETURNING'

    location = path + 'MyOutput_ingest_pandas.csv'
print('Output location: ' + location)

print('Finished')
