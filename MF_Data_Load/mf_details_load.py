from mftool import Mftool
import nsepy
from nsepy import get_history
from datetime import date
import sqlalchemy as sa
import urllib.parse
import pandas as pd
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

mf = Mftool()

# df = pd.DataFrame(mf.get_all_amc_profiles(as_json=False))
"""
# Code to extract and load the scheme name and codes of all MFs
df = pd.DataFrame.from_dict(mf.get_scheme_codes(as_json=False),orient='index')
df.reset_index(inplace=True)
df.columns = ['Code','Scheme_Name']
df.to_sql(name='MF_SCHEME_CODES',con=conn,if_exists='replace',index=False)
"""

query = "SELECT code FROM dbo.MF_SCHEME_CODES where CheckIt='Y' "
mf_data = conn.execute(query)
mfunds = mf_data.fetchall()
df_all = pd.DataFrame()

for fund in mfunds:
    try:
        print(fund[0])
        df = pd.DataFrame.from_dict(mf.get_scheme_details(fund[0],as_json=False),orient='index')
        df = df.T
        scheme_dt_nav = df['scheme_start_date'].values[0]
        df['scheme_start_date'] = scheme_dt_nav['date']
        df['scheme_nav'] = scheme_dt_nav['nav']
        df_all = df.append(df_all,ignore_index=True)
    except:
        continue

#scheme_nav = df_all['scheme_start_date'].values[0]['nav']
df_all.to_sql(name='MF_SCHEME_DETAILS',con=conn,if_exists='replace',index=False)
print(df_all.tail())