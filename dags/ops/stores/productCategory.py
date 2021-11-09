import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging
from ops.stores.db import ETL

class ProductCategoryETL(ETL):
    
    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')
    
    # -- Data Quality Rules
    def cleanData(self, row):
        ''' Apply the following rules to product categories and returns a cleaned record. The column is:
        Product_Category -> Consistency (title)
        '''
        yield [
                row['Product_Category'].title()
            ]

    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Category Quality process:
        Product Category Name
        Created_dt '''
        yield [
                row['product_category_name'],
                self.todayDate
            ]

    # -- Transformations
    def DQProductCategories(self):
        ''' Process Product Categories data from Stg'''
        self._initDBConnection()
        # Read information from stg_Products
        table1 = fromdb(self.connection, 'SELECT * FROM stg_products')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_Products: {len(table1)}')
            # Cleanig data
            logging.info('Cleaning data')
            table2 = rowmapmany(table1, self.cleanData, header=['product_category_name'])
            # Uniqueness
            table3 = distinct(table2,'product_category_name')
            # Validate existing records to either insert
            categories = fromdb(self.connection, 'SELECT * FROM product_category')
            if (len(categories) > 0):
                logging.info('Comparing rows in product category')
                table4 = leftjoin(table3, categories, key="product_category_name")
                table5 = select(table4, "{product_category_id} is None")
                # Insert specific columns 
                table6 = rowmapmany(table5, self.selectColumns , header=['product_category_name','created_dt'])
                appenddb(table6, self.connection, 'product_category')
                logging.info(look(table6))
            else:
                logging.info('No new records found')
        else:
            logging.info('No records to process')
        self._closeConnection()
        
        

# etl = ProductCategoryETL()
# etl.DQProductCategories()

