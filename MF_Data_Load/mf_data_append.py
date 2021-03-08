from mftool import Mftool
import nsepy
from nsepy import get_history
from datetime import date,datetime as dt
import datetime as dtm
import sqlalchemy as sa
import urllib.parse
import pandas as pd

mf = Mftool()

#Use this for windows authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=DESKTOP-MAK81E6\SQLEXPRESS;"
                                 "DATABASE=MFDATA;"
                                 "Trusted_Connection=yes")

'''
#Use this for SQL server authentication
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=dagger;"
                                 "DATABASE=test;"
                                 "UID=user;"
                                 "PWD=password")
'''

#Connection String
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

# Connect to the required SQL Server
conn=engine.connect()

"""
# Code to extract and load the scheme name and codes of all MFs
df = pd.DataFrame.from_dict(mf.get_scheme_codes(as_json=False),orient='index')
df.reset_index(inplace=True)
df.columns = ['Code','Scheme_Name']
df.to_sql(name='MF_SCHEME_CODES',con=conn,if_exists='replace',index=False)
"""

query = "SELECT * FROM dbo.MF_SCHEME_DETAILS "
mf_data = conn.execute(query)
mfunds = mf_data.fetchall()
df = pd.DataFrame(mfunds,columns=['FundHouse','sch_type','sch_categoty','sch_code','sch_name','sch_start','sch_nav'])

end_year = 2021

for data in df.itertuples():
    code = data[4]
    print("Extract data for the Fund : {}".format(data[5]))
    # if code != 119550:
    #     continue
    fund_house = data[1].split(' ')[0]
    fund_name = data[5].split('-')[0].split(' ')
    fund_name = fund_name[0] + '_' + '_'.join(fund_name[-4:-1])
    fund_name = fund_name.replace('_&_','_') if '&' in fund_name else fund_name
    tbl_name = fund_name.upper()
    query = 'select max(date) from dbo.'+tbl_name
    max_date = conn.execute(query)
    max_date = max_date.fetchall()[0][0]
    ext_date = max_date - dtm.timedelta(30)

    get_data_query = "select * from dbo."+tbl_name+" where date > '{}'".format(ext_date)
    get_data = pd.read_sql(get_data_query,con=conn,parse_dates=True)
    df1 = pd.DataFrame()
    try:
        ext_data = mf.get_scheme_historical_nav_year(code, end_year, as_json=False)
        mf_data = ext_data['data']

        for d in mf_data:
            df = pd.DataFrame.from_dict(d,orient='index')
            df =df.T
            df1 = df1.append(df,ignore_index=True)
    except:
        continue

    df1['date'] = pd.to_datetime(df1['date'], format='%d-%m-%Y')
    df1 = df1[df1['date']>max_date]
    if len(df1.index)==0:
        print('Latest data already present in the database')
        continue
    df1 = df1.append(get_data[['date','nav']])
    df1['nav'] = df1['nav'].astype(dtype=float)
    df1['nav'].replace(0, method='pad', inplace=True)
    df1.sort_values(by='date', ascending=True, inplace=True)
    df1['nav_chg_daily'] = round(df1['nav'].pct_change() * 100, 2)
    df1['nav_chg_wkly'] = round(df1['nav'].pct_change(periods=5) * 100, 2)
    df1['nav_chg_mth'] = round(df1['nav'].pct_change(periods=20) * 100, 2)
    try:
        df1.to_sql(name=tbl_name, con=conn, if_exists='replace', index=False)
    except Exception as e:
        print('error is : {}'.format(e))
        print("Data Load not done for the Fund : {}".format(data[5]))
        continue
    print("Data Load done for the Fund : {}".format(data[5]))

