from mftool import Mftool
import nsepy
from nsepy import get_history
from datetime import date,datetime as dt

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

query = "SELECT top 1* FROM dbo.Aditya119533 order by date desc"
df1 = pd.read_sql_query(query,con=conn,parse_dates=True)
print(df1)

data = pd.DataFrame.from_dict(mf.get_scheme_quote('119533',as_json=False),orient='index')

data = data.T
print(data)
print(data.columns)