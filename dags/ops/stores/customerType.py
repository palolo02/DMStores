import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging
from ops.stores.db import ETL



class CustomerTypeETL(ETL):

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
                row['Type_Customer'].title()
            ]

    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process:
        type_customer_name
        Created_dt '''
        yield [ 
                row['type_customer_name'], 
                self.todayDate
            ]
    
    # -- Transformations
    def DQCustomerType(self):
        ''' Process Products data from Stg'''
        self._initDBConnection()
        # Read information from stg_Products
        table1 = fromdb(self.connection, 'SELECT * FROM stg_customer')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_Customer: {len(table1)}')
            # Cleanig data
            logging.info('Cleaning data')
            table2 = rowmapmany(table1, self.cleanData, header=['type_customer_name'])
            # Uniqueness
            table3 = distinct(table2,'type_customer_name')
            # Replace Store Type with their lookup
            # Validate existing records to either insert
            customerTypes = fromdb(self.connection, 'SELECT count(*) FROM type_customer')
            logging.info(logging.info(customerTypes['count'][0]))
            if (customerTypes['count'][0] > 0):
                logging.info('Comparing rows in store type')
                customerTypes = fromdb(self.connection, 'SELECT * FROM type_customer')
                table3 = leftjoin(table3, customerTypes, key="type_customer_name")
                logging.info(look(table3))
                table3 = select(table3, "{type_customer_id} is None")
            table4 = rowmapmany(table3, self.selectColumns , header=['type_customer_name','created_dt'])
            logging.info(look(table4))
            appenddb(table4, self.connection, 'type_customer')
            logging.info('No new records found')
        else:
            logging.info('No records to process')
        self._closeConnection()

