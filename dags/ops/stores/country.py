import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging
from ops.stores.db import ETL



class CountryETL(ETL):

    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')
    
    # -- Data Quality Rules
    def cleanData(self, row):
        ''' Apply the following rules to Store Types and returns a cleaned record. The columns are:
        Store_Country -> Consistency (title)
        '''
        yield [
                row['Store_Country'].title()
            ]

    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process:
        Store Name
        City Id
        Store Type Id
        Store Open Date
        Created_dt '''
        yield [ 
                row['country_name'], 
                self.todayDate
            ]
    
    # -- Transformations
    def DQCountry(self):
        ''' Process Products data from Stg'''
        self._initDBConnection()
        # Read information from stg_Products
        table1 = fromdb(self.connection, 'SELECT * FROM stg_stores')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_Products: {len(table1)}')
            # Cleanig data
            logging.info('Cleaning data')
            table2 = rowmapmany(table1, self.cleanData, header=['country_name'])
            # Uniqueness
            table3 = distinct(table2,'country_name')
            # Replace Store Type with their lookup
            # Validate existing records to either insert
            countries = fromdb(self.connection, 'SELECT count(*) FROM country')
            logging.info(countries['count'][0])
            if (countries['count'][0] > 0):
                logging.info('Comparing rows in store type')
                countries = fromdb(self.connection, 'SELECT * FROM country')
                table3 = leftjoin(table3, countries, key="country_name")
                table3 = select(table3, "{country_id} is None")
            table4 = rowmapmany(table3, self.selectColumns , header=['country_name','created_dt'])
            logging.info(look(table4))
            appenddb(table4, self.connection, 'country')
            logging.info('No new records found')
        else:
            logging.info('No records to process')
        self._closeConnection()


# etl = CountryETL()
# etl.DQCountry()
