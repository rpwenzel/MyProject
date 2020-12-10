import json
import os.path
from os import path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import exc

global debug,exists,engine
debug = False
exists = 'append'
engine = create_engine('postgresql://bob:password@localhost:5432/bobtest') # Database and credentials

'''Since pd.read_json will only read a single json statement,
I created 8 files, which includes the initial sample_input
name format is:  sfi_#.json  where # is 1...8
'''

def main():
    print('Beginning')

    drop_tables = 'DROP TABLE IF EXISTS public."CUSTOMER", public."SITE_VISIT", public."IMAGE", public."ORDER"'
    runsqlcmds(drop_tables)

# Process sfi_#.json files
    i = 1
    loop = True
    while loop:
        fileName = 'sfi_' + str(i) + '.json'
        
# If file is found, process it        
        if path.isfile(fileName):  # does file exists?
            Ingest(fileName,engine)
            if i == 1:sqlKeysConstraints
            i += 1
        else:
            loop = False

    print('Finished')
    return

def sqlKeysConstraints():
    #TBD#
    return

def runsqlcmds(command):      #process drop tables and update statements
    try:
        with engine.connect() as con:
           rs = con.execute(command)

    except exe.SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        return error
    return

def Ingest(e,D):
    if debug: print('inside CUSTOMER_NEW ')

    df = pd.read_json(e)
    df.reset_index(drop=True)

    if debug: print(df)

    df1=df.iloc[[0] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()

# Determine What file types and verbs are found, then process each file found 
    if 'CUSTOMER'   == type and 'NEW' == verb:    CUSTOMER_NEW(df,D)
    if 'SITE_VISIT' == type and 'NEW' == verb:    SITE_VISIT_NEW(df,D)
    if 'IMAGE'      == type and 'UPLOAD' == verb: IMAGE_NEW(df,D)
    if 'ORDER'      == type and 'NEW' == verb:    ORDER_NEW(df,D)

    if 'CUSTOMER'   == type and 'UPDATE' == verb: CUSTOMER_UPDATE(df,D)   #[4,5,6,7]
    if 'ORDER'      == type and 'UPDATE' == verb: ORDER_UPDATE(df,D)      #[3,4,6]

    return

def CUSTOMER_NEW(df,D):
#   CUSTOMER, NEW
    if debug: print('inside CUSTOMER_NEW ')

    df1=df.iloc[[0] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    df2=df.iloc[[0], 2:7]
    df2.to_sql('CUSTOMER', D, if_exists=exists, index=False)

    df1=df.iloc[[1] , : ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    tags_str = str(df1['tags'].squeeze())
    df2=df.iloc[[1], np.r_[2,3,7]]
    df2['tags'] = tags_str.replace('1 ','') # adding string to 'tags' (since df2=df.iloc[[1], np.r_[2,3,7,8]] fails)
    if 'SITE_VISIT' == type and 'NEW' == verb:
        df2.to_sql('SITE_VISIT', D, if_exists=exists, index=False)

    df1=df.iloc[[2] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    df2=df.iloc[[2], np.r_[2,3,7,9,10]]
    if 'IMAGE' == type and 'UPLOAD' == verb:
        df2.to_sql('IMAGE', D, if_exists=exists, index=False)

    df1=df.iloc[[3] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    df2=df.iloc[[3], np.r_[2,3,7,11]]
    if 'ORDER' == type and 'NEW' == verb:
        df2.to_sql('ORDER', D, if_exists=exists, index=False)
    return

def SITE_VISIT_NEW(df,D):
#   SITE_VISIT, NEW
    if debug: print('inside SITE_VISIT_NEW ')

    df1=df.iloc[[0] , : ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    tags_str = str(df1['tags'].squeeze())
    df2=df.iloc[[0], np.r_[2,3,4]]
    df2['tags'] = tags_str.replace('0 ','') # adding string to 'tags' (since df2=df.iloc[[1], np.r_[2,3,7,8]] fails)
    df2.to_sql('SITE_VISIT', D, if_exists=exists, index=False)
    return

def IMAGE_NEW(df,D):
#   IMAGE, NEW
    if debug: print('inside IMAGE_NEW ')

    df1=df.iloc[[0] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    df2=df.iloc[[0], np.r_[2,3,4,5,6]]
    df2.to_sql('IMAGE', D, if_exists=exists, index=False)
    return

def ORDER_NEW(df,D):
#   ORDER, NEW
    if debug: print('inside ORDER_NEW ')

    df1=df.iloc[[0] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    df2=df.iloc[[0], np.r_[2,3,4,5]]
    df2.to_sql('ORDER', D, if_exists=exists, index=False)
    return


def CUSTOMER_UPDATE(df,D):
#   CUSTOMER, UPDATE            Update these relative fields:  [4,5,6,7]
    if debug: print('inside CUSTOMER_UPDATE 1 ')

    df1=df.iloc[[0] , : ]

    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    key  = df1['key'].squeeze()
    event_time = df1['event_time'].squeeze()
    last_name =df1['last_name'].squeeze()
    adr_city =df1['adr_city'].squeeze()
    adr_state =df1['adr_state'].squeeze()
    
    sql_update1 = 'UPDATE public."CUSTOMER" SET event_time = '
    sql_update2 = "'" + str(event_time) + "' ,last_name = '" + last_name
    sql_update3 = "' ,adr_city = '" + adr_city
    sql_update4 = "' ,adr_state = '" + adr_state + "' WHERE key = '" + key + "';"
    sql_update  = sql_update1 + sql_update2 +  sql_update3 + sql_update4

    runsqlcmds(sql_update)

    return

def ORDER_UPDATE(df,D):
#   ORDER, UPDATE               Update these relative fields:  [3,4,6]
    if debug: print('inside ORDER_UPDATE ')

    df1=df.iloc[[0] , : ]

    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    key  = df1['key'].squeeze()
    event_time = df1['event_time'].squeeze()
    customer_id = df1['customer_id'].squeeze()
    total_amount = df1['total_amount'].squeeze()

    sql_update1 = 'UPDATE public."ORDER" SET event_time= '
    sql_update2 = "'" + str(event_time)
    sql_update3 = "' ,total_amount = '" + total_amount
    sql_update4 = "' WHERE key ='" + key + "' AND  customer_id = '" + customer_id + "'"
    sql_update  = sql_update1 + sql_update2 +  sql_update3 + sql_update4

    runsqlcmds(sql_update)

    return

if __name__== "__main__":
    main()
