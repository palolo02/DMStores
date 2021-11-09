import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging
from ops.stores.db import ETL


class InventoryETL(ETL):
    
    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')
    
    
    # ==== Data Profiling rules ===== 
    def cleanData(self, row):
        ''' Apply the following rules to Stores and returns a cleaned record. The columns are:
        Store_ID -> Consistency (title)
        Product_ID -> Consistency (title)
        Stock_On_Hand -> Validity (int) '''
        yield [ 
                row['Store_ID'].title(), 
                row['Product_ID'].title(),
                int(row['Stock_On_Hand'])
            ]
   
    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process
        Store ID
        Product ID
        Stock On Hand
        '''
        yield [
                row['store_id'],
                row['product_id'],
                row['stock_on_hand'],
                date.today()
            ]
     
    # ===== ETL Steps ========
        
    def DQInventory(self):
        ''' Apply Data Quality Dimensions in Inventory Table'''
        self._initDBConnection()
        # Read information from stg_Products
        table1 = fromdb(self.connection, 'SELECT * FROM stg_inventory')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_Products: {len(table1)}')
            # Uniqueness
            table2 = unique(table1, ['Store_ID','Product_ID'])
            # Cleanig data
            logging.info('Cleaning data')
            table3 = rowmapmany(table2, self.cleanData,header=['store_name','product_name','stock_on_hand'])
            store = fromdb(self.connection, 'SELECT * FROM store')
            table4 = lookupjoin(table3, store, key='store_name')
            prod = fromdb(self.connection, 'SELECT * FROM product')
            table4 = lookupjoin(table4, prod, key='product_name')
            table5 = rowmapmany(table4,self.selectColumns, header=['store_id','product_id','stock_on_hand','inventory_dt'])
            logging.info(look(table5))
            appenddb(table5, self.connection, 'inventory')
        else:
            logging.info('No records to process')
        self._closeConnection()
   
# etl = InventoryETL()
# etl.DQInventory()
