import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging
from ops.stores.db import ETL


class SaleStatusETL(ETL):
    
    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')

    def cleanData(self, row):
        ''' Returns the final columns used in the Product Quality process'''
        yield [
                 'NA' if row['Status'] is None else row['Status'].title()
            ]
     
    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process'''
        yield [
                row['sales_status_name'],
                self.todayDate
            ]
     
    # ===== ETL Steps ========
        
    def DQSaleStatus(self):
        ''' Process Products data from Stg'''
        self._initDBConnection()
        # Read information from stg_Products
        table1 = fromdb(self.connection, 'SELECT * FROM stg_sales')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_Products: {len(table1)}')
            # Cleanig data
            table2 = rowmapmany(table1, self.cleanData,header=['sales_status_name'])
            # Uniqueness
            table3 = distinct(table2,'sales_status_name')
            status = fromdb(self.connection, 'SELECT count(*) FROM sales_status')
            logging.info(logging.info(status['count'][0]))
            if (status['count'][0] > 0):
                logging.info('Comparing rows in store type')
                status = fromdb(self.connection, 'SELECT * FROM sales_status')
                table3 = leftjoin(table3, status, key="sales_status_name")
                logging.info(look(table3))
                table3 = select(table3, "{sales_status_id} is None")
            table4 = rowmapmany(table3,self.selectColumns, header=['sales_status_name','created_dt'])
            appenddb(table4, self.connection, 'sales_status')
            logging.info('No new records found')
        else:
            logging.info('No records to process')
        self._closeConnection()


     


# etl = SaleSatatusETL()
# etl.DQSaleStatus()
