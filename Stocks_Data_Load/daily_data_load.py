import nsepy
import pandas as pd
from nsepy import get_history
from datetime import date
import sqlalchemy as sa
import urllib.parse
import re


class Dataload:
    # Class to enable bhav copy data extraction and load into corresponding tables in SQL for each stock
    # Use this for windows authentication
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=DESKTOP-MAK81E6\SQLEXPRESS;"
                                     "DATABASE=NSEDATA;"
                                     "Trusted_Connection=yes")

    '''
    #Use this for SQL server authentication
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=dagger;"
                                     "DATABASE=test;"
                                     "UID=user;"
                                     "PWD=password")
    '''
    # Connection String
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    # Connect to the required SQL Server
    try:
        conn = engine.connect()
    except ConnectionError:
        print('Job aborted due to SQL Server connection issue')
    ###########################################################################################################
    def extract_bhav_copy(self, extract_date):
        # Method to extract bhav copy for the passed date
        try:
            # Extract stock specific bhav copy into a DataFrame
            bcopy = nsepy.history.get_price_list(dt=extract_date)

            # bcopy.to_csv('bhavcopy_'+today_dt.strftime('%d%m%Y')+'.csv',index=False)
            # Load entire bhav copy contents on stock specific data into SQL table
            bcopy.to_sql(name='BHAVCOPY', con=self.conn, if_exists='replace', index=False)

            # Extract Index specific bhav copy into a DataFrame
            bcopy_indices = nsepy.history.get_indices_price_list(dt=extract_date)

            # bcopy.to_csv('bhavcopy_'+today_dt.strftime('%d%m%Y')+'.csv',index=False)
            # Load entire bhav copy contents on stock specific data into SQL table
            bcopy_indices.to_sql(name='BHAVCOPY_INDICES', con=self.conn, if_exists='replace', index=False)
            return 'Bhav Copy extraction and data load is complete'
        except Exception as e:
            print('Bhav copy is not avaialble for Date {} : Error Msg :  {}'.format(extract_date, e.__str__()))
            return 'File not available'
    ############################################################################################################
    def read_sql_data(self, data_type='Stock'):
        """ Method to read bhav data for both stocks and indices """
        if data_type == 'Stock':
            query = "SELECT NAME FROM dbo.STOCKS"
            bhav_table = 'BHAVCOPY'
        else:
            query = "SELECT NAME FROM dbo.STOCK_INDICES UNION SELECT NAME FROM dbo.STOCK_SECTORS"
            bhav_table = 'BHAVCOPY_INDICES'

        stocks_data = self.conn.execute(query)
        stocks = stocks_data.fetchall()
        # Extract stock/index bhavcopy data into a dataframe
        bhav_query = "SELECT * FROM dbo." + bhav_table
        data = pd.read_sql_query(bhav_query, con=self.conn, parse_dates=True)
        # Extract data of required columns from BHAVCOPY data as in the SQL tables format
        if data_type == 'Stock':
            self.df_today = data.loc[:, ['SYMBOL', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']]
            self.df_today.rename(columns={'TIMESTAMP': 'Date', 'OPEN': 'Open', 'HIGH': 'High', 'LOW': 'Low',
                                          'CLOSE': 'Close', 'TOTTRDQTY': 'Volume'}, inplace=True)
            self.df_today.set_index('SYMBOL', inplace=True)
        else:
            self.df_today = data.loc[:, ['NAME', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']]
            self.df_today.rename(columns={'TIMESTAMP': 'Date', 'OPEN': 'Open', 'HIGH': 'High', 'LOW': 'Low',
                                          'CLOSE': 'Close', 'TOTTRDQTY': 'Volume'}, inplace=True)
            self.df_today.set_index('NAME', inplace=True)
            self.df_today['Date'] = pd.to_datetime(self.df_today['Date'], format='%d-%m-%Y')
        return stocks
    ##############################################################################################################
    def load_sql_stock_data(self, stock):
        """ Method to load data for each stock """
        if stock == 'BAJAJAUTO':
            stock = 'BAJAJ-AUTO'
        if stock == 'MM':
            stock = 'M&M'
        if stock == 'MCDOWELL':
            stock = 'MCDOWELL-N'
        if stock == 'LTFH':
            stock = 'L&TFH'
        if stock == 'MMFIN':
            stock = 'M&MFIN'

        data_to_add = self.df_today[self.df_today.index == stock]

        print("Start Data Load for the stock : {}".format(stock))
        if stock == 'BAJAJ-AUTO':
            stock = 'BAJAJAUTO'
        if stock == 'M&M':
            stock = 'MM'
        if stock == 'MCDOWELL-N':
            stock = 'MCDOWELL'
        if stock == 'L&TFH':
            stock = 'LTFH'
        if stock == 'M&MFIN':
            stock = 'MMFIN'
        # Write data read from bhav table to SQL Server table
        data_to_add.to_sql(name=stock, con=self.conn, if_exists='append', index=False)
        print("Data Load done for the stock : {}".format(stock))
    ################################################################################################################
    def load_index_data(self, stock):
        """ Method to append index data from bhav table to Index table for the required date """
        self.df_today.index = self.df_today.index.str.upper()
        # Converting index table names to be in sync with the index names as in the BHAVCOPY INDICES table
        if stock == 'NIFTY_SMLCAP_100':
            stock = 'NIFTY Smallcap 100'
        if stock == 'NIFTY_SMLCAP_250':
            stock = 'Nifty Smallcap 250'
        if stock == 'NIFTY_SMLCAP_50':
            stock = 'Nifty Smallcap 50'
        if stock == 'NIFTY_INFRA':
            stock = 'Nifty Infrastructure'
        if stock == 'NIFTY_SERV_SECTOR':
            stock = 'Nifty Services Sector'
        if stock == 'NIFTY_CONSUMPTION':
            stock = 'Nifty India Consumption'
        if stock == 'NIFTY_FIN_SERVICE':
            stock = 'Nifty Financial Services'
        # Converting index table names to be in sync with the index names as in the BHAVCOPY INDICES table
        data_to_add = self.df_today[self.df_today.index == ' '.join(stock.upper().split('_'))]
        # Converting index names in the BHAVCOPY INDICES table to be in sync with the index table names in the database
        if stock == 'NIFTY Smallcap 100':
            stock = 'NIFTY_SMLCAP_100'
        if stock == 'Nifty Smallcap 250':
            stock = 'NIFTY_SMLCAP_250'
        if stock == 'Nifty Smallcap 50':
            stock = 'NIFTY_SMLCAP_50'
        if stock == 'Nifty Infrastructure':
            stock = 'NIFTY_INFRA'
        if stock == 'Nifty Services Sector':
            stock = 'NIFTY_SERV_SECTOR'
        if stock == 'Nifty India Consumption':
            stock = 'NIFTY_CONSUMPTION'
        if stock == 'Nifty Financial Services':
            stock = 'NIFTY_FIN_SERVICE'
        print("Start Data Load for the Index : {}".format(stock))
        # Write data read from bhav table to SQL Server table
        data_to_add.to_sql(name=stock, con=self.conn, if_exists='append', index=False)
        print("Data Load done for the Index : {}".format(stock))
    #################################################################################################################
    @staticmethod
    def get_max_date(stock='NIFTY_50'):
        """ Method to extract max date to identify the data to be loaded for days until current date """
        try:
            # Extract maximum date for until which data is present in SQL for a stock
            query_max_date = "SELECT max(DATE) FROM dbo." + stock
            startdate = Dataload.conn.execute(query_max_date)
            start_dt = startdate.fetchall()
            start_dt = start_dt[0][0]
            start_dt = pd.to_datetime(start_dt).date()
            return start_dt
            # Exception handling for no data in SQL for the stock
        except:
            print('No data for the stock {}'.format(stock))

# Specify stock or index type
stock_index = ['Stock', 'Index']
# Create dataload object to start with the data load
dataload = Dataload()
# Capture stocks/indices for which data load is not complete
missing_data_load = {}

if __name__ == '__main__':
    """ Call required methods in this module for data load """
    # Get max date of a base stock like 'SBIN'
    max_date = dataload.get_max_date()
    # Increment max date to identify the start date for the data load
    extract_start_date = max_date + pd.to_timedelta('1 day')
    # Create a date range series for all the dates pending data load
    extract_date_range = pd.Series(pd.date_range(start=extract_start_date, end=date.today(), freq='D'))
    # Loop through the date range to call methods for the data load
    for extract_date in extract_date_range:
        extract_date = pd.to_datetime(extract_date).date()
        # Skip load if the date is falling on a weekend
        day_is = extract_date.strftime('%A')
        if day_is in ['Saturday', 'Sunday']:
            print('{} - {} is a weekend Holiday'.format(extract_date, day_is))
            continue
        # Call method to extract bhav copy for stocks and indices
        print('Extracting bhav data for the Date : {}'.format(extract_date))
        bhav_copy_load = dataload.extract_bhav_copy(extract_date)
        # If bhav copy is available, proceed with the data load for each stock and index
        if re.search(r"complete", bhav_copy_load):
            print(f'Bhav Data extraction is complete for the Date : {extract_date}')
            for stock_type in stock_index:
                # Get list of stocks/indices
                stocks_list = dataload.read_sql_data(stock_type)
                if stock_type == 'Stock':
                    # Call data load method for each stock
                    for stock in stocks_list:
                        stock = stock[0]
                        date_diff = extract_date - dataload.get_max_date(stock)

                        if date_diff.days <= 0:
                            print('skipped load for Stock : {}'.format(stock))
                            continue
                        try:
                            dataload.load_sql_stock_data(stock)
                        except:
                            print('Data Load is not complete for {}'.format(stock))
                            missing_data_load.update('stock', (stock, extract_date))
                else:
                    # Call data load method for each index
                    for nse_index in stocks_list:
                        nse_index = nse_index[0]
                        date_diff = extract_date - dataload.get_max_date(nse_index)

                        if date_diff.days <= 0:
                            print('skipped load for Index : {}'.format(nse_index))
                            continue
                        try:
                            dataload.load_index_data(nse_index)
                        except:
                            print('Data Load is not complete for {}'.format(nse_index))
                            missing_data_load.update('index', (nse_index, extract_date))
        else:
            continue
