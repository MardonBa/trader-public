import requests as r
import finnhub
#import json        Not needed for now
import wget
from datetime import date, datetime, timedelta
import pandas as pd
import time
#from tabulate import tabulate     Not needed for now
import traceback
import os

from warnings import simplefilter 
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
## Ignores the warnings raised by pandas when adding financial data columns to the data dataframe

pd.set_option('display.max_columns', None)



"""
api calls should be in the following format for aggregates (data over a specified time period)
API_AGG_BASE_URL/ticker/:ticker/range/:range/:timespan/:start-date/:end-date?adujsted=bool&sort=asc&limit=int&apiKey=API_KEY
if timespan = minute and range = 5 then 5-minute bars will be returned.

api calls should be in the following format for grouped daily 
API_AGG_BASE_URL/grouped/locale/us/market/stocks/:date?adjusted=bool&apiKey=API_KEY

api calls should be in the following format for daily open/close
API_DAILY_BASE_URL/:ticker/:date?adjusted=bool&apiKey=API_KEY

asc sorts in ascending order (oldest on top), and desc sorts in descending order (newest on top)
limit is an integer (default 5000, max 50000), number of instances


for other questions relating to the polygon api, visit https://polygon.io/docs/stocks/getting-started
"""

class GetData:
    def __init__(self, polygon_api_key, finnhub_api_key):
        self.POLYGON_API_KEY = polygon_api_key
        self.POLYGON_API_AGG_BASE_URL = 'https://api.polygon.io/v2/aggs'
        self.POLYGON_API_DAILY_BASE_URL = 'https://api.polygon.io/v1/open-close'

        self.FINNHUB_API_KEY = finnhub_api_key
        self.finnhub_client = finnhub.Client(api_key=self.FINNHUB_API_KEY)

    def _create_api_call(self, query, params, call_type): ## type is daily, daily_aggregate, or time_period_aggregate
        for key in query.keys(): ## Makes sure that the query has all the necessary parameters
            if key not in params:
                print(params)
                raise ValueError('Query should have the specified params for the type of api call')
            
        if call_type == 'daily':
            api_call = f'{self.POLYGON_API_DAILY_BASE_URL}/{query["ticker"]}/{query["date"]}?adjusted={query["adjusted"]}&apiKey={self.POLYGON_API_KEY}'
        elif call_type == 'daily_aggregate':
            api_call = f'{self.POLYGON_API_AGG_BASE_URL}/grouped/locale/us/market/stocks/{query["date"]}?adjusted={query["adjusted"]}&apiKey={self.POLYGON_API_KEY}'
        elif call_type == 'time_period_aggregate':
            api_call = f'{self.POLYGON_API_AGG_BASE_URL}/ticker/{query["ticker"]}/range/{query["range"]}/{query["timespan"]}/{query["start_date"]}/{query["end_date"]}?adjusted={query["adjusted"]}&limit={query["limit"]}&apiKey={self.POLYGON_API_KEY}'

        return api_call

    def get_polygon_data(self, query, agg): 
        ## Query should be an object as input with what should be passed to the query string
        ## Agg should be false if calling for daily, otherwise it should be a string, either daily or time_period
        if type(query) != dict:
            print(type(query))
            raise ValueError("Query must be a dictionary")
        else:
            if agg != False: 
                if agg == 'time_period': ## Not really necessary, since daily_agg gets the same data. I don't want to get rid of it in case I find some use for it
                    params = ['ticker', 'range', 'timespan', 'start_date', 'end_date', 'adjusted', 'sort', 'limit']
                    call_type = 'time_period_aggregate'
                elif agg == 'daily_agg': ## Can only get data 2 years from present date, start getting and savind data sooner rather than later
                    params = ['date', 'adjusted']
                    call_type = 'daily_aggregate'
                else:
                    raise ValueError("If agg isn't false, it should either be time_period of daily_agg")
            else:
                params = ['ticker', 'date', 'adjusted']
                call_type = 'daily'

            api_call = self._create_api_call(query, params, call_type)
            response = r.get(api_call).json() ## remove .json() when errors come up to see status code
            return response
        
    def get_financials(self, ticker):
        
        financials = self.finnhub_client.company_basic_financials(ticker, 'all')
        ## Gets basic financial data
        ## The data not stored in the dictionary mapped to metric won't be used due to inconsistency in the time-frames represented

        return financials
    
    def get_vix_history(self):
        wget.download("https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv", "VIX_History.csv")



    
    ## Maybe for some feature engineering, engineer a % change feature (not given by api)



class SortData: ## Used to get the historical data necessary
    def __init__(self, data_class):
        self.data_class = data_class
    
    def _get_earliest_possible_date(self, date): 
        digits_to_change = int(date[2:4])
        digits_to_change -= 2
        new_date = date[:2] + str(digits_to_change) + date[4:]
        return new_date
    
    def _extract_date_info(self, date): 
        date = datetime.strptime(date, '%Y-%m-%d')
        year = int(date.year)
        month = int(date.month)
        day = int(date.day)
        return [year, month, day]
    
    def _daterange(self, start_date, end_date): ## Adapted from https://stackoverflow.com/questions/1060279/iterating-through-a-range-of-dates-in-python
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        for n in range(int((end_date - start_date).days + 1)):
            yield str(start_date + timedelta(n))[:10]
    
    def _build_daily_agg_query(self): ## Builds out a list of all query dictionaries possible, starting 2 years from today
        queries = []
        todays_date = str(date.today())
        earliest_possible_date = self._get_earliest_possible_date(todays_date)

        for single_date in self._daterange(earliest_possible_date, todays_date): ## Iterate over all possible dates
            queries.append({ 
                'date': single_date, 
                'adjusted': 'true'  ## String for the future api call
                })
        
        return queries
    

    def _build_daily_open_close_query(self, ticker):
        queries = []
        todays_date = str(date.today())
        earliest_possible_date = self._get_earliest_possible_date(todays_date)

        for single_date in self._daterange(earliest_possible_date, todays_date): ## Iterate over all possible dates
            queries.append({ 
                'ticker': ticker,
                'date': single_date, 
                'adjusted': 'true'  ## String for the future api call
                })
            
        return queries
    

    def _build_time_period_agg_query(self): pass 
    ## Not really necessary right now so I'm not going to build it
    ## Mainly just here in case I find a need for it, since there's already the code for the steps before building queries

    def _get_time_diff(self, time1, format1, time2, format2):
        time_diff = int((datetime.strptime(time1, format1) - datetime.strptime(time2, format2)).days)
        return time_diff

    def _add_time_diff(self, original, time):
        new_str = f'{original}_{time}_days_before'
        return new_str
    
    def _rename_cols(self, df, time_diff): ## Calls the above function in order to properly rename the columns
        df.rename(columns={
            'T': 'ticker', ## To be removed 
            'c': self._add_time_diff('close_price', time_diff), 
            'h': self._add_time_diff('highest_price', time_diff),
            'l': self._add_time_diff('lowest_price', time_diff),
            'n': self._add_time_diff('num_transactions', time_diff),
            'o': self._add_time_diff('open_price', time_diff),
            't': self._add_time_diff('unix_timestamp', time_diff), ## To be removed
            'v': self._add_time_diff('trading_volume', time_diff),
            'vw': self._add_time_diff('trading_volume_weighted', time_diff)
        }, inplace=True)

        return df
    
    def _merge_on_tickers(self, df1, df2): 
        common_tickers = set.union(set(df1['ticker']), set(df2['ticker']))

        for ticker in df1['ticker']:
            if ticker not in common_tickers:
                df1.drop(ticker, axis=0, inplace=True)

        for ticker in df2['ticker']:
            if ticker not in common_tickers:
                df2.drop(ticker, axis=0, inplace=True)

        for col in df2.columns:
            if col == 'ticker': continue ## Surely there's a better solution to skip the ticker column, but it's late
            else:
                df1[col] = df2[col].copy()

        return df1
                
    def _check_union_of_data(self, l1, l2):
        common_data = set.union(set(l1), set(l2))
        return common_data


        
    def get_and_sort_initial_data(self):
        data_df = pd.DataFrame()
        first_call = True


        ###############################################
        ## Get the polygon api data
        ###############################################

        daily_agg_queries = self._build_daily_agg_query()
        for i, query in enumerate(daily_agg_queries):
            time_diff = self._get_time_diff(daily_agg_queries[-1]['date'], '%Y-%m-%d', daily_agg_queries[i]['date'], '%Y-%m-%d' )
            print(f'time difference: {time_diff}')
            ##if i == 4: break ## For testing purposes

            print(f"i = {i}")
            if i % 5 == 4: 
                print('sleeping')
                cont = input('would you like to continue? ')
                if cont == 'no': break
                time.sleep(60) ## Can only make 5 api calls per minute, so this stops the code from running for 1 minute until it can again
            data = self.data_class.get_polygon_data(query, 'daily_agg')
            if data['queryCount'] == 0: continue ## Skips over weekends/holidays
            if data_df.empty: ## Checks if the df is empty, using not and XOR
                data_df = data_df.from_dict(data['results']) ## Base case (first successful API call)
                data_df = self._rename_cols(data_df, time_diff) ## Renames the column accordingly
            else:
                temp_df = pd.DataFrame()
                temp_df = temp_df.from_dict(data['results']) ## Base case (first successful API call)
                temp_df = self._rename_cols(temp_df, time_diff) ## Renames the column accordingly
                data_df = self._merge_on_tickers(data_df, temp_df) ## Merges on same date

            



        ###############################################
        ## Get the financials data from Finnhub API
        ###############################################


        ## Get the financials data for each ticker there is data for
        ## If there is no available data for the ticker, remove from the dataframe
        
        tickers = list(data_df['ticker'])
        og_num_cols = len(data_df.columns)
        df_cols = list(data_df.columns)[og_num_cols:] ## df_cols is sliced so that it only has the columns that come from the financial data currently being fetched
        i_offset = 0
        for i, ticker in enumerate(tickers):
            if i % 60 == 59: 
                print('sleeping')
                cont = input('would you like to continue? ')
                if cont == 'no': break
                time.sleep(60) ## Should deal with the 60 calls/minute limit
            print(f'i = {i}')
            try:
                financials_data = self.data_class.get_financials(ticker)['metric']
                if len(financials_data) < 100: 
                    i_offset += 1 ## To deal with the fact that the ticker isn't in the dataset anymore
                    print('Not sufficient data')
                    continue ## Including things that have less than 100 financial metrics would discount too many financial metrics

                if i == 0: ## Adds null values to every new column for each ticker
                    for key in financials_data.keys():
                        data_df[key] = [None for x in range(len(data_df))]
                        df_cols.append(key)
                # * new code snippet starts here
                ## Pare down df_cols so that it only includes columns that are included in the dataframe currently and in the current financial data
                ## Each column that you remove from df_cols should also be removed from the df
                else:
                    current_ticker_cols = set(financials_data.keys())
                    cols_to_remove = set(df_cols) - current_ticker_cols ## Gets the columns that aren't in the current ticker's financial metrics so they can be removed
                    print(cols_to_remove)
                    for col in cols_to_remove:
                        data_df.drop(col, axis=1, inplace=True) ## Removes the column from the dataframe inplace
                        df_cols.remove(col) ## Removes the column from the list of columns in the dataframe
                # * new code snippet ends here

                for col in df_cols:
                    if financials_data[col] == None: ## Failsafe for None-types that are somehow missed
                        ## * Hopefully this actually works
                        data_df.drop(col, axis=1, inplace=True)
                        df_cols.remove(col)
                        continue
                    data_df.at[i - i_offset, col] = financials_data[col]

            except Exception as err: 
                print(f'error: {traceback.format_exc()}')
                ## This might be necessary later when it gets to where Finnhub doesn't have data for a ticker, so handling that case
                ## Haven't hit that in tests so far, so it's just here and empty for now
            if i == 30: break ## For testing




        ###############################################
        ## Get VIX History
        ###############################################

        self.data_class.get_vix_history() ## Gets the history and saves it to the folder

        if not os.path.isfile('VIX_History.csv'):
            raise Exception('Something went wrong with downloading the VIX History')
        
        else:
            vix_data = pd.read_csv('VIX_History.csv')
            earliest_date = str(date.today() - timedelta(days=731)) ## 2 years + 1 day before, consistent with polygon data
            print(f'\n{earliest_date}')

            found = False
            while found == False:
                earliest_date = str(datetime.strptime(earliest_date, '%Y-%m-%d') + timedelta(days=1))
                if earliest_date in set(vix_data['DATE']):
                    slicing_index = vix_data.index.get_loc(vix_data[vix_data['DATE'] == earliest_date].index[0])
                    found = True

            vix_data = vix_data[slicing_index:] ## Slices the dataframe so that it only contains data from the last 2 years

            for index, row in vix_data.iterrows():
                ## TODO iterate over all rows and add the data to the data_df (list comprehension to make same value for each ticker)
                pass



            
        #data_df.dropna(inplace=True)
        print(data_df.columns)
        print(data_df.head())
        print(len(data_df))

