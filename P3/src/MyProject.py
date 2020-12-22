import datetime
from datetime import datetime
from datetime import timedelta
import json
import numpy as np
import os.path
from os import path
import pandas as pd
import pytz
from sqlalchemy import create_engine
from sqlalchemy import exc

'''
I used postgresql for windows for my Data warehouse: postgresql-13.0-1-windows-x64.exe

After installing postgress with: password    for user postgress password:

1.
createdb -h localhost -p 5432 -U postgres bobtest
password: password  (was specified when prompted)

2.
createuser -P -U postgres bob

3.
C:\Program Files\PostgreSQL\13\bin>psql bobtest postgres
Password for user postgres: password  --(entered)

4.
bobtest=# \du                         --(\du shows users)
results:
 Role name |                         Attributes                         | Member of
-----------+------------------------------------------------------------+-----------
 bob       |                                                            | {}
 postgres  | Superuser, Create role, Create DB, Replication, Bypass RLS | {}
5.
bobtest=# exit

Please contact me if you need any help or have any questions re postgres install

'''

global debug,exists,engine
debug = False #True
exists = 'append'
engine = create_engine('postgresql://bob:password@localhost:5432/bobtest') # Database and credentials

def CUSTOMER_NEW(df,D):
#   CUSTOMER, NEW
    if debug: print('Inside CUSTOMER_NEW ')

    df1=df.iloc[[0] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    df2=df.iloc[[0], 2:7]
    df2.to_sql('CUSTOMER', D, if_exists=exists, index=False)
    if len(df.index) == 1: return

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


def CUSTOMER_UPDATE(df,D):
#   CUSTOMER, UPDATE            Update these relative fields:  [4,5,6,7]
    if debug: print('Inside CUSTOMER_UPDATE 1 ')

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

    sqlcmd(sql_update)
    return


def IMAGE_NEW(df,D):
#   IMAGE, NEW
    if debug: print('Inside IMAGE_NEW ')

    df1=df.iloc[[0] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    df2=df.iloc[[0], np.r_[2,3,4,5,6]]
    df2.to_sql('IMAGE', D, if_exists=exists, index=False)
    return


def ORDER_NEW(df,D):
#   ORDER, NEW
    if debug: print('Inside ORDER_NEW ')

    df1=df.iloc[[0] , :2 ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    df2=df.iloc[[0], np.r_[2,3,4,5]]
    df2.to_sql('ORDER', D, if_exists=exists, index=False)
    return


def ORDER_UPDATE(df,D):
#   ORDER, UPDATE               Update these relative fields:  [3,4,6]
    if debug: print('Inside ORDER_UPDATE ')

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

    sqlcmd(sql_update)
    return


def SITE_VISIT_NEW(df,D):
#   SITE_VISIT, NEW
    if debug: print('Inside SITE_VISIT_NEW ')

    df1=df.iloc[[0] , : ]
    type = df1['type'].squeeze()
    verb = df1['verb'].squeeze()
    tags_str = str(df1['tags'].squeeze())
    df2=df.iloc[[0], np.r_[2,3,4]]
    df2['tags'] = tags_str.replace('0 ','') # adding string to 'tags' (since df2=df.iloc[[1], np.r_[2,3,7,8]] fails)
    df2.to_sql('SITE_VISIT', D, if_exists=exists, index=False)
    return

# Ingest record e, D is Database connection
def Ingest(e,D):
    if debug: print('Inside Ingest Records ')

    df = pd.read_json(e)
    df.reset_index(drop=True)

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

    return df1


def sqlcmd(input):      #process update statements
    if debug:print('Inside sqlcmd')
    SQLAlchemyError = 'Dummy'
    try:
        with engine.connect() as con:
           rs = con.execute(input)
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        return error

    return


# Find Top Simple LTV Customers (x), D is Database connection
def TopXSimpleLTVCustomers(x, D):
    if debug: print('Inside TopXSimpleLTVCCustomers')
    utc=pytz.UTC

    query = '''
    select * from public."ORDER"
    '''
    df = pd.read_sql_query(query, engine)

#  Split total_amount int amt and cur()
    df[['amt','cur']] = df.total_amount.str.split(" ",expand=True)
    df['amt'] = pd.to_numeric(df['amt'])

#   Find the week of year
    year_week = []
    for t in df['event_time']:
        week = str(t.isocalendar()[1])
        if len(week) == 1:  week = '0' + str(week)
        year_week_row_entry = str(t.year) + '-' + week
        year_week.append(year_week_row_entry)

    df['year_week'] = year_week
    df['amt'] = pd.to_numeric(df['amt'])


# Find the Sum of the values found in a week
# identify the columns we want to aggregate by; this could
    group_cols = ['customer_id','year_week']
# identify the columns which we want to average; this could
    metric_cols = [ 'amt']
# create a new DataFrame with a MultiIndex consisting of the group_cols
# and a column for the mean of each column in metric_cols
    aggs = df.groupby(group_cols)[metric_cols].sum()
# remove the metric_cols from df because we are going to replace them
# with the means in aggs
    df.drop(metric_cols, axis=1, inplace=True)
    df.drop(['total_amount','cur'], axis=1, inplace=True)
# dedupe to leave only one row with each combination of group_cols in df
    df.drop_duplicates(subset=group_cols, keep='last', inplace=True)
# add the mean column from aggs into df
    df = df.merge(right=aggs, right_index=True, left_on=group_cols, how='right')


#   Arbitrarily based on my data using last 10 years,   could have calculated it if needed to from the data
    mindate = datetime.strptime('2011-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    maxdate = datetime.strptime('2021-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    mask = (df['event_time'] > mindate.replace(tzinfo=utc)) & (df['event_time'] <= maxdate.replace(tzinfo=utc))
    df_filtered=df.loc[mask]


#   Find the means for each week   Similar to Find the Sum above
    group_cols = ['customer_id']
    metric_cols = [ 'amt']
    aggs = df.groupby(group_cols)[metric_cols].mean()
    df.drop(metric_cols, axis=1, inplace=True)
    df.drop_duplicates(subset=group_cols, keep='last', inplace=True)
#   Add the mean column from aggs into df
    df = df.merge(right=aggs, right_index=True, left_on=group_cols, how='right')

#   Calculate Simple LTV per customer
    df['Simple LTV'] = 52*df['amt']*10

#   Sort the result with Simple LTV in descending order with customer_id and amt
    df_sort = df.sort_values(['Simple LTV','customer_id'], ascending=[False,True]).reset_index()
    df_simpleLTV =df_sort[['Simple LTV','customer_id', 'amt']]

    return df_simpleLTV



def main():
    print('Beginning\n')

#   used to drop all of the tables every time; so job can be run any time
    drop_tables = 'DROP TABLE IF EXISTS public."CUSTOMER", public."SITE_VISIT", public."IMAGE", public."ORDER"'

#   used to set up primary and foreign keys
    alter_table =  ['ALTER TABLE public."CUSTOMER" ADD PRIMARY KEY ("key")',
                   'ALTER TABLE public."SITE_VISIT" ADD PRIMARY KEY ("key")',
                   'ALTER TABLE public."IMAGE" ADD PRIMARY KEY ("key")',
                   'ALTER TABLE public."ORDER" ADD PRIMARY KEY ("key")',
                   'ALTER TABLE public."SITE_VISIT" ADD CONSTRAINT IMAGE_FK FOREIGN KEY(customer_id) REFERENCES public."CUSTOMER"(key)',
                   'ALTER TABLE public."IMAGE" ADD CONSTRAINT IMAGE_FK FOREIGN KEY(customer_id) REFERENCES public."CUSTOMER"(key)',
                   'ALTER TABLE public."ORDER" ADD CONSTRAINT IMAGE_FK FOREIGN KEY(customer_id) REFERENCES public."CUSTOMER"(key)']


# Cleanup: Drop tables if they exists
    sqlcmd(drop_tables)

# Process json file with top_x, NEW, UPLOAD AND UPDATE Records
    fileName = '1file_input.json'
    first = True
    count=0
    with open(fileName) as jsonfile:
        for line in jsonfile:
            df = pd.read_json(line)
            count += 1
#           read value for x (top_x variable in json file) for function: TopXSimpleLTVCustomers(x,D)
            if first:
                df.reset_index(drop=True)
                df1=df.iloc[[0] , :1 ]
                x = int(float(df1['top_x'].squeeze()))
                first = False
                continue

            e = line
            D = engine
            Ingest(e, D)
# Close json file
    jsonfile.close()

# Add primary and foreign keys to tables
    for i in alter_table:
        sqlcmd (i)


#   find Top x customers,  x from top_x variable in json file
    D = engine
    df_top_x_results = TopXSimpleLTVCustomers(x, D)

    y = x    #only used for the following print statement
    print('Top ' + str(y) + ' Simple LTV Customers:')
    df_topx = df_top_x_results.head(x)
    print(df_topx.to_string(index = False))

    print('\nFinished')
    return


if __name__== "__main__":
    main()
