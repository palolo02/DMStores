import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging
from ops.stores.db import ETL

class ProductETL(ETL):
    
    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')
   
    # ==== Data Profiling rules ===== 
    def cleanData(self, row):
        ''' Apply the following rules to Products and returns a cleaned record. The columns are:
        Product_Name -> Consistency (title)
        Product_Category -> Consistency (title)
        Product_Cost -> Validty and Precision (replace $ and convert into float)
        Product_Price -> Validity and Precision (replace $ and convert into float)
        '''
        yield [ 
                row['Product_Name'].title(), 
                row['Product_Category'].title(), 
                float(row['Product_Cost'].replace('$','')),
                float(row['Product_Price'].replace('$',''))
            ]
   
    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process:
        Product Name
        Product Category Id
        Product Cost
        Product Price
        Created_dt '''
        yield [
                row['product_name'],
                row['product_category_id'],
                row['product_cost'],
                row['product_price'],
                self.todayDate
            ]
     
    # ===== ETL Steps ========
        
    def DQProducts(self):
        ''' Process Products data from Stg'''
        self._initDBConnection()
        # Read information from stg_Products
        table1 = fromdb(self.connection, 'SELECT * FROM stg_products')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_Products: {len(table1)}')
            # Uniqueness
            table2 = distinct(table1,'Product_Name')
            # Cleanig data
            logging.info('Cleaning data')
            table3 = rowmapmany(table2, self.cleanData,header=['product_name','product_category_name','product_cost','product_price'])
            # Replace Category_id with lookup
            cat = fromdb(self.connection, 'SELECT * FROM product_category')
            table4 = lookupjoin(table3, cat, key='product_category_name')
            # Validate existing records to either insert
            products = fromdb(self.connection, 'SELECT * FROM product')
            if (len(products) > 0):
                logging.info('Comparing rows in product')
                table5 = leftjoin(table4, products, key="product_name")
                table5 = select(table5, "{product_id} is None")
                # Insert specific columns 
                table6 = rowmapmany(table5,self.selectColumns, header=['product_name','product_category_id','product_cost','product_price','created_dt'])
                appenddb(table6, self.connection, 'product')
                logging.info(look(table6))
            else:
                logging.info('No new records found')
        else:
            logging.info('No records to process')
        self._closeConnection()
           
        

# etl = ProductETL()
# etl.DQProducts()
