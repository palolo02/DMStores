import numpy as np
import pandas as pd
from pandas_profiling import ProfileReport
import psycopg2
import json
from datetime import date
import logging
from dags.ops.stores.db import ETL

class Profiler(ETL):

    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')

    def profileData(self):
        ''' Generate Profiling reports for all data in DB '''
        self._profile('stg_products', ['Product_ID'])
        self._profile('stg_sales', ['Sale_ID'])
        self._profile('stg_stores', ['Store_ID'])
        self._profile('stg_inventory', ['Store_ID','Product_ID'])
        self._profile('stg_customer', ['Customer_ID'])
        self._profile('stg_ratings', ['Customer_ID','Product_ID'])
     
    def _profile(self, table, index_col):
        ''' Generate Profiling report for Product '''
        self._initDBConnection()
        df = pd.read_sql_query(f'select * from public."{table}"',con=self.connection,index_col=index_col)
        logging.info(f'======= Profiling {table} ==========')
        profile = ProfileReport(df, title=f"Pandas Profiling Report for {table}")
        profile.to_file(f"logs/profiling_reports/{table}_{date.today()}.html")
        self._closeConnection()
     

# p = Profiler()
# p.profileData()