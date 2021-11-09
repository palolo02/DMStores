import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
from datetime import datetime as dtime
import logging
from ops.stores.db import ETL


class SalesETL(ETL):
    
    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')

    
    # ==== Data Profiling rules ===== 
    def cleanData(self, row):
        ''' Receive all product-related columns and apply transformation to them
        Product_ID, Product_Name, Product_Category, Product_Cost, Product_Price'''
        yield [ 
                row['Sale_ID'], 
                dtime.strptime(row['Date'], '%d/%m/%Y'),
                row['Store'].title(),
                row['Product'].title(),
                row['Status'].title() if row['Status'] is not None else 'NA',
                dtime.strptime(row['Delivered_Date'], '%d/%m/%Y'),
                row['Purchase'].title(),
                row['Customer'].title(),
                int(row['Units']),
                float(row['Discount'].replace('%',''))
            ]
   
    def selectColumns(self, row):
        ''' Returns the final columns used in the Product Quality process'''
        yield [
                row['sales_dt'],
                row['store_id'],
                row['product_id'],
                row['sales_status_id'],
                row['delivered_dt'],
                row['sales_purchase_id'],
                row['customer_id'],
                row['units'],
                row['discount']
            ]
     
    # ===== ETL Steps ========
        
    def DQSales(self):
        ''' Apply Data Quality Dimensions in Inventory Table
        '''
        # Read information from stg_Sales
        self._initDBConnection()
        table1 = fromdb(self.connection, 'SELECT * FROM stg_sales limit 30000')
        logging.info(f'rows to process in stg_Products: {len(table1)}')
        table2 = unique(table1, 'Sale_ID')
        table3 = rowmapmany(table2, self.cleanData,header=['sales_id','sales_dt','store_name','product_name','sales_status_name','delivered_dt','sales_purchase_name','customer_name','units','discount'])
        logging.info(f'Cleaning Data \n {look(table3)}')
        stores = fromdb(self.connection, 'SELECT * FROM store')
        table4 = lookupjoin(table3, stores, key='store_name')
        logging.info(f'Store \n {look(table4)}')
        prod = fromdb(self.connection, 'SELECT * FROM product')
        table4 = lookupjoin(table4, prod, key='product_name')
        logging.info(f'Product \n {look(table4)}')
        status = fromdb(self.connection, 'SELECT * FROM sales_status')
        table4 = lookupjoin(table4, status, key='sales_status_name')
        logging.info(f'Sales Status \n {look(table4)}')
        purchase = fromdb(self.connection, 'SELECT * FROM sales_purchase')
        table4 = lookupjoin(table4, purchase, key='sales_purchase_name')
        logging.info(f'Purchase \n {look(table4)}')
        customer = fromdb(self.connection, 'SELECT * FROM customer')
        table4 = lookupjoin(table4, customer, key='customer_name')
        logging.info(f'Customer \n {look(table4)}')
        table4 = rowmapmany(table4,self.selectColumns, header=['sales_dt','store_id','product_id','sales_status_id','delivered_dt','sales_purchase_id','customer_id','units','discount'])
        appenddb(table4, self.connection, 'sales')
        logging.info(f'Final Results \n {look(table4)}')
        self._closeConnection()


# etl = SalesETL()
# etl.DQSales()