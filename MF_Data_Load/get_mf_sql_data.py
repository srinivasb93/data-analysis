import pandas as pd
import sqlalchemy as sa
import datetime as dt
import urllib

# mf = 'ADITYA_PSU_DEBT'
# mf = 'MIRAE_EMERGING_BLUECHIP_FUND'


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

#Query to fetch all mutual fund table names
query = "SELECT mf_name FROM dbo.MUTUAL_FUNDS"
mfs_data = conn.execute(query)
mf_funds = mfs_data.fetchall()

#Function to obtain returns from SIP via investing fixed amount every month
def calculate_returns_sip(in_data):
    temp_df = in_data.copy()
    temp_df['sip_month']=temp_df['date'].dt.month
    temp_df['sip_day'] = temp_df['date'].dt.day
    temp_df['sip_year'] = temp_df['date'].dt.year
    temp_df.dropna(inplace=True)
    temp_df.reset_index(inplace=True,drop=True)
    total_investment = 100000
    add_investment = 0
    add_units = 0
    total_units = round(total_investment/temp_df.loc[0,'nav'],2)
    k = 2
    len_di = len(temp_df.index)
    for mf_data in temp_df.itertuples():
        if (k < len_di):
            current_month = temp_df.loc[k,'sip_month']
            prev_month = temp_df.loc[k-1,'sip_month']
            if current_month!=prev_month:
                sip_amount = 2050
                add_investment +=sip_amount
                sip_units = round(sip_amount/temp_df.loc[k,'nav'],2)
                add_units += sip_units
                total_units = total_units + sip_units
                total_investment = total_investment + sip_amount
                # print(' SIP executed on Date : {}'.format(temp_df.loc[k,'date']))
                # print(' SIP Amount invested : {}'.format(sip_amount))
        k+=1
    present_nav = temp_df['nav'].tail().values[0]
    present_val = total_units*present_nav
    abs_returns = round(((present_val-total_investment)/total_investment)*100,2)
    print('Total Investment Value : {}'.format(total_investment))
    print('Additional Investment made : {}'.format(add_investment))
    print('Present Investment Value : {}'.format(present_val))
    print('Absolute Returns : {0:.2f}'.format(abs_returns))
    print('Available units at present : {}'.format(total_units))

#Function to obtain returns from SIP via adhoc logic
def calculate_returns(in_data):
    temp_df = in_data.copy()
    temp_df.dropna(inplace=True)
    temp_df.reset_index(inplace=True,drop=True)
    total_investment = 100000
    add_investment = 0
    add_units = 0
    total_units = round(total_investment/temp_df.loc[0,'nav'],2)
    for mf_data in temp_df.itertuples():
        mult_factor = int(mf_data[3]*10)
        if mult_factor <0:
            sip_amount = abs(mult_factor)*1000
            add_investment +=sip_amount
            sip_units = round(sip_amount/temp_df.loc[mf_data[0],'nav'],2)
            add_units += sip_units
            total_units = total_units + sip_units
            total_investment = total_investment + sip_amount
            # print(' SIP executed on Date : {}'.format(temp_df.loc[mf_data[0],'date']))
            # print(' SIP Amount invested : {}'.format(sip_amount))
    present_nav = temp_df['nav'].tail().values[0]
    present_val = total_units*present_nav
    abs_returns = round(((present_val-total_investment)/total_investment)*100,2)
    print('Total Investment Value : {}'.format(total_investment))
    print('Additional Investment made : {}'.format(add_investment))
    print('Present Investment Value : {}'.format(present_val))
    print('Absolute Returns : {0:.2f}'.format(abs_returns))
    print('Available units at present : {}'.format(total_units))

#function to combine monthly SIP and adhoc method
def calculate_returns_both(in_data):
    temp_df = in_data.copy()
    temp_df['sip_month']=temp_df['date'].dt.month
    temp_df['sip_day'] = temp_df['date'].dt.day
    temp_df['sip_year'] = temp_df['date'].dt.year
    temp_df.dropna(inplace=True)
    temp_df.reset_index(inplace=True,drop=True)
    total_investment = 100000
    add_investment = 0
    add_units = 0
    total_units = round(total_investment/temp_df.loc[0,'nav'],2)
    k = 2
    len_di = len(temp_df.index)
    negative_chg = temp_df['nav_chg_daily'][temp_df['nav_chg_daily']<=-.1].count()
    for mf_data in temp_df.itertuples():
        if (k < len_di):
            current_month = temp_df.loc[k,'sip_month']
            prev_month = temp_df.loc[k-1,'sip_month']
            mult_factor = int(mf_data[3] * 10)

            if current_month!=prev_month:
                sip_amount = 2050
                add_investment +=sip_amount
                sip_units = round(sip_amount/temp_df.loc[k,'nav'],2)
                add_units += sip_units
                total_units = total_units + sip_units
                total_investment = total_investment + sip_amount
                # print(' SIP executed on Date : {}'.format(temp_df.loc[k,'date']))
                # print(' SIP Amount invested : {}'.format(sip_amount))
            elif mult_factor <0:
                sip_amount = int(199000/negative_chg)
                add_investment +=sip_amount
                sip_units = round(sip_amount/temp_df.loc[mf_data[0],'nav'],2)
                add_units += sip_units
                total_units = total_units + sip_units
                total_investment = total_investment + sip_amount
                # print(' SIP executed on Date : {}'.format(temp_df.loc[mf_data[0],'date']))
                # print(' SIP Amount invested : {}'.format(sip_amount))
        k+=1
    present_nav = temp_df['nav'].tail().values[0]
    present_val = total_units*present_nav
    abs_returns = round(((present_val-total_investment)/total_investment)*100,2)
    print('Total Investment Value : {}'.format(total_investment))
    print('Additional Investment made : {}'.format(add_investment))
    print('Present Investment Value : {}'.format(present_val))
    print('Absolute Returns : {0:.2f}'.format(abs_returns))
    print('Available units at present : {}'.format(total_units))


#Function to calcluate best day for SIP returns every month
def calculate_returns_daywise(in_data,day):
    temp_df = in_data.copy()
    temp_df['sip_month']=temp_df['date'].dt.month
    temp_df['sip_day'] = temp_df['date'].dt.day
    temp_df['sip_year'] = temp_df['date'].dt.year
    # temp_df['daysinmonth'] = temp_df['date'].dt.daysinmonth
    # temp_df['isleapyear'] = temp_df['date'].is_leap_year
    temp_df.dropna(inplace=True)
    temp_df.reset_index(inplace=True,drop=True)
    total_investment = 100000
    add_investment = 0
    add_units = 0
    total_units = round(total_investment/temp_df.loc[0,'nav'],2)

    for mf_data in temp_df.itertuples():
        day_today = temp_df.loc[mf_data[0],'sip_day']
        if day==day_today:
            sip_amount = 2000 if day!=31 else 3000
            sip_amount = 2220 if day in [2,25] else sip_amount
            add_investment +=sip_amount
            sip_units = round(sip_amount/temp_df.loc[mf_data[0],'nav'],2)
            add_units += sip_units
            total_units = total_units + sip_units
            total_investment = total_investment + sip_amount
            # print('Total Investment : {}'.format(total_investment))
            # print(' SIP executed on Date : {}'.format(temp_df.loc[mf_data[0],'date']))
            # print(' SIP Amount invested : {}'.format(sip_amount))

    present_nav = temp_df['nav'].tail().values[0]
    present_val = total_units*present_nav
    abs_returns = round(((present_val-total_investment)/total_investment)*100,2)
    # print('Below is the analysis when invested on day : {}'.format(day))
    # print('Total Investment Value : {}'.format(total_investment))
    # print('Additional Investment made : {}'.format(add_investment))
    # print('Present Investment Value : {}'.format(present_val))
    # print('Absolute Returns : {0:.2f}'.format(abs_returns))
    # print('Available units at present : {}'.format(total_units))
    # print('=====================================================')
    return day,total_investment,add_investment,present_val,abs_returns,total_units

#Program to implement the logic to identify returns of Mutual Funds
for mf in mf_funds:
    # if mf[0]!='ADITYA_PSU_DEBT':
    #     continue
    query = "SELECT * FROM dbo." + mf[0] + " ORDER BY DATE ASC "
    data = pd.read_sql_query(query, con=conn, parse_dates=True)
    data['date'] = pd.to_datetime(data['date'])
    # data['date'] = pd.to_datetime(data['date']).dt.strftime('%m/%d/%Y')
    # data['Symbol'] = mf
    # print(data.dtypes)
    print('Returns as per invest when NAV low logic')
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++')
    calculate_returns(data[['date','nav','nav_chg_daily']])
    print('====================================================')

    print('Returns when invested every month on Day 1')
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++')
    calculate_returns_sip(data[['date','nav','nav_chg_daily']])
    print('====================================================')

    print('Returns when above 2 methods are combined')
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++')
    calculate_returns_both(data[['date','nav','nav_chg_daily']])
    print('====================================================')

    # print('Call the function to check for returns obatined day wise')
    # print('++++++++++++++++++++++++++++++++++++++++++++++++++++')
    # days_count = [i for i in range(1,32)]
    # mf_analysis = pd.DataFrame()
    # for day in days_count:
    #     returns_data = calculate_returns_daywise(data[['date','nav','nav_chg_daily']],day)
    #     returns_df = pd.DataFrame([returns_data],columns=['day','total_investment','add_investment','present_val','abs_returns','total_units'])
    #     returns_df['Mutual_Fund'] = mf[0]
    #     mf_analysis = mf_analysis.append(returns_df,ignore_index=True)
    # mf_analysis.to_sql(name='MUTUAL_FUND_RETURNS', con=conn, if_exists='append', index=False)
    print('Data Analysis is complete for the Fund : {}'.format(mf[0]))
print('Data Analysis is complete for all Mutual Funds')
