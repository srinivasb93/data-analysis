import pandas as pd
import sqlalchemy as sa
import urllib.parse

#Connect to SQL SERVER
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                 "SERVER=DESKTOP-MAK81E6\SQLEXPRESS;"
                                 "DATABASE=MFDATA;"
                                 "Trusted_Connection=yes")
dbEngine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
conn = dbEngine.connect()

#stock_list = ['TATAMOTORS','MARUTI','ZEEL','BRITANNIA','SBIN','BAJAJFINSV','BAJFINANCE','HDFCBANK','ASIANPAINT','ICICIBANK','KOTAKBANK','RELIANCE','TITAN','HINDUNILVR','INDUSINDBK','ITC','NESTLEIND','INFY','TCS']
#stock_list = ['TATAMOTORS']

query = "SELECT mf_name FROM dbo.MUTUAL_FUNDS"
mfs_data = conn.execute(query)
mf_names = mfs_data.fetchall()

for mf in mf_names:
    mf = mf[0]
    print(mf)
    # query = "SELECT * FROM dbo." + stock + " WHERE DATE >='2020-10-20 00:00:00.000' AND DATE <'2021-01-05 00:00:00.000' ORDER BY DATE ASC "
    # query = "SELECT * FROM dbo." + stock + " WHERE DATE >='2020-01-25 00:00:00.000' ORDER BY DATE ASC "
    query = "SELECT date,nav FROM dbo." + mf + " ORDER BY DATE ASC "
    data = pd.read_sql_query(query, con=conn, parse_dates=True)
    #data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%m/%d/%Y')
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date',inplace=True)
    data.sort_index(inplace=True)

    logic = {'nav':'last'}

    #logic to calculate weekly fund data from daily data
    wk_data = data.resample('W').agg(logic)
    wk_data.index = wk_data.index - pd.DateOffset(days=6)
    wk_data.reset_index(inplace=True)
    wk_data['Wk_nav_Chg'] = round(wk_data['nav'].pct_change()*100,2)
    wk_data['Bi_Wk_nav_Chg'] = round(wk_data['nav'].pct_change(periods=2)*100,2)
    wk_data['High_52_Wk'] = wk_data['nav'].rolling(52).max()
    wk_data['Low_52_Wk'] = wk_data['nav'].rolling(52).min()
    wk_data['ATH'] = wk_data['nav'].expanding().max()
    wk_data['ATL'] = wk_data['nav'].expanding().min()

    print("Start Weekly Data Load for the fund : {}".format(mf))
    # Write data read from .csv to SQL Server table
    wk_data.to_sql(name=mf+'_W', con=conn, if_exists='replace', index=False)
    # stock.to_sql(name=stock + '_W', con=conn, if_exists='append', index=False)
    print("Weekly Data Load done for the fund : {}".format(mf))

    #logic to calculate Monthly fund data from daily data
    mth_data = data.resample('M').agg(logic)
    mth_data.reset_index(inplace=True)
    mth_data['Mth_nav_Chg'] = round(mth_data['nav'].pct_change() * 100, 2)
    mth_data['Bi_Mth_nav_Chg'] = round(mth_data['nav'].pct_change(periods=2) * 100, 2)
    mth_data['High_12_Mth'] = mth_data['nav'].rolling(12).max()
    mth_data['Low_12_Mth'] = mth_data['nav'].rolling(12).min()
    mth_data['ATH'] = mth_data['nav'].expanding().max()
    mth_data['ATL'] = mth_data['nav'].expanding().min()

    print("Start Monthly Data Load for the fund : {}".format(mf))
    # Write data read from .csv to SQL Server table
    mth_data.to_sql(name=mf+'_M', con=conn, if_exists='replace', index=False)
    # stock.to_sql(name=stock + '_W', con=conn, if_exists='append', index=False)
    print("Monthly Data Load done for the fund : {}".format(mf))

print('Weekly and Monthly Data Load successful for all the funds')
