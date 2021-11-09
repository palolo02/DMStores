import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
from datetime import datetime as dtime
import logging
from ops.stores.db import ETL


class RatingETL(ETL):
    
    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')

    
    # ==== Data Profiling rules ===== 
    def cleanData(self, row):
        ''' Apply the following rules to Store Types and returns a cleaned record. The columns are:
        Date -> Consistency (title)
        Product -> Consistency (title)
        Customer -> Consistency (title)
        Rating -> Validity (int)
        '''
        yield [ 
                row['Customer_ID'].title(), 
                row['Product_ID'].title(),
                int(row['Rating']),
                dtime.strptime(row['Date'], '%d/%m/%Y')
            ]
   
    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process'''
        yield [
                row['customer_id'],
                row['product_id'],
                row['rating'],
                row['rating_dt']
            ]
     
    # ===== ETL Steps ========
        
    def DQRating(self):
        ''' Apply Data Quality Dimensions in Inventory Table
        '''
        # Read information from stg_Sales
        self._initDBConnection()
        table1 = fromdb(self.connection, 'SELECT * FROM stg_ratings')
        logging.info(f'rows to process in stg_Ratings: {len(table1)}')
        # uniqueness
        table2 = unique(table1, ['Customer_ID','Product_ID'])
        # Clean data
        logging.info('Cleaning data')
        table3 = rowmapmany(table2, self.cleanData,header=['customer_name','product_name','rating','rating_dt'])
        # Validate product and customer information
        prod = fromdb(self.connection, 'SELECT * FROM product')
        table4 = lookupjoin(table3, prod, key='product_name')
        customer = fromdb(self.connection, 'SELECT count(*) FROM customer')
        if(customer['count'][0] > 0):
            customer = fromdb(self.connection, 'SELECT * FROM customer')
            table4 = lookupjoin(table4, customer, key='customer_name')
            table4 = select(table4, "{customer_id} is not None and {product_id} is not None")
        table5 = rowmapmany(table4,self.selectColumns, header=['customer_id','product_id','rating','rating_dt'])
        logging.info(look(table5))
        appenddb(table5, self.connection, 'ratings')
        self._closeConnection()
        #stats(table, field) | stats(table, 'bar') | {'count': 3, 'errors': 2, 'min': 1.0, 'max': 3.0, 'sum': 6.0, 'mean': 2.0}
        # foopats = stringpatterns(table, 'foo') | >>> look(foopats) 

# etl = SalesETL()
# etl.DQSales()