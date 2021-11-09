import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
from datetime import datetime as dtime
import logging
from ops.stores.db import ETL



class StoresETL(ETL):

    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')
      
       
    # -- Data Quality Rules
    def cleanData(self, row):
        ''' Apply the following rules to Stores and returns a cleaned record. The columns are:
        Store_Name -> Consistency (title)
        Store_City -> Consistency (title)
        Store_Location -> Consistency (title)
        Store_Open_Date -> 
        '''
        yield [ 
                row['Store_Name'].title(), 
                row['Store_City'].title(), 
                row['Store_Location'].title(),
                dtime.strptime(row['Store_Open_Date'], '%d/%m/%Y')
            ]

    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process:
        Store Name
        City Id
        Store Type Id
        Store Open Date
        Created_dt '''
        yield [ 
                row['store_name'], 
                row['city_id'],
                row['store_type_id'],
                row['store_open_date'],
                self.todayDate
            ]
    # -- Transformations
    def DQStores(self):
        ''' Process Products data from Stg'''
        self._initDBConnection()
        # Read information from stg_Products
        table1 = fromdb(self.connection, 'SELECT * FROM stg_stores')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_Products: {len(table1)}')
            # Uniqueness
            table2 = distinct(table1, ['Store_Name'])
            # Cleanig data
            logging.info('Cleaning data')
            table3 = rowmapmany(table2, self.cleanData,header=['store_name','city_name','store_type_name', 'store_open_date'])
            # Replace City and Store Type with their lookup
            city = fromdb(self.connection, 'SELECT * FROM city')
            table4 = lookupjoin(table3, city, key='city_name')
            store_type = fromdb(self.connection, 'SELECT * FROM store_type')
            table4 = lookupjoin(table4, store_type, key='store_type_name')
            # Validate existing records to either insert
            stores = fromdb(self.connection, 'SELECT * FROM store')
            if (len(stores) > 0):
                table4 = leftjoin(table4, stores, key="store_name")
                table4 = select(table4, "{store_id} is None")
                # Insert specific columns 
            table5 = rowmapmany(table4,self.selectColumns, header=['store_name','city_id','store_type_id','store_open_date','created_dt'])
            logging.info(look(table5))
            appenddb(table5, self.connection, 'store')
            #else:
            #    logging.info('No new records found')
        else:
            logging.info('No records to process')
        self._closeConnection()


# etl = StoresETL()
# etl.DQStores()
