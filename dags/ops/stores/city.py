import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging
from ops.stores.db import ETL



class CityETL(ETL):

    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')
    
    # -- Data Quality Rules
    def cleanData(self, row):
        ''' Apply the following rules to Store Types and returns a cleaned record. The columns are:
        Store_Location -> Consistency (title)
        '''
        yield [ 
                row['Store_City'].title(), 
                row['Store_Country'].title()
            ]

    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process:
        city_name
        country_id
        Created_dt '''
        yield [ 
                row['city_name'], 
                row['country_id'],
                self.todayDate
            ]
    
    # -- Transformations
    def DQCity(self):
        ''' Process Products data from Stg'''
        self._initDBConnection()
        # Read information from stg_Products
        table1 = fromdb(self.connection, 'SELECT * FROM stg_stores')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_Products: {len(table1)}')
            # Uniqueness
            table2 = distinct(table1, ['Store_City','Store_Country'])
            # Cleanig data
            logging.info('Cleaning data')
            table3 = rowmapmany(table2, self.cleanData, header=['city_name','country_name'])
            # Uniqueness
            table3 = distinct(table3, ['city_name','country_name'])
            # Replace Country ID with lookup
            country = fromdb(self.connection, 'SELECT * FROM country')
            table4 = lookupjoin(table3, country, key='country_name')

            cities = fromdb(self.connection, 'SELECT count(*) FROM city')
            if (cities['count'][0] > 0):
                logging.info('Comparing rows in store type')
                cities = fromdb(self.connection, 'SELECT * FROM city')
                table4 = leftjoin(table4, cities, key="city_name")
                table4 = select(table4, "{city_id} is None")
            # Insert specific columns 
            table5 = rowmapmany(table4, self.selectColumns , header=['city_name','country_id','created_dt'])
            logging.info(look(table4))
            appenddb(table5, self.connection, 'city')
            logging.info('No new records found')
        else:
            logging.info('No records to process')
        self._closeConnection()


# etl = CityETL()
# etl.DQCity()
