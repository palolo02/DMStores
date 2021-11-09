import psycopg2
from petl import look, fromdb, unique, rowmap, rowmapmany, distinct, fieldmap, appenddb, lookup, lookupjoin, leftjoin, select
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging
from ops.stores.db import ETL

class CustomerETL(ETL):

    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')

    # ==== Data Profiling rules ===== 
    def cleanData(self, row):
        ''' Apply the following rules to Customer and returns a cleaned record. The columns are:
        Customer_Name -> Consistency (title)
        Type_Customer -> 
        '''
        yield [ 
                row['Customer_Name'].title(), 
                row['Type_Customer'].title(),
                row['Sex'].title(),
                row['Marital status'].title(),
                int(row['Age']),
                int(row['Spending_Score']),
            ]
           
    def selectColumns(self, row):
        return [
                row['customer_name'],
                row['type_customer_id'],
                row['gender_id'],
                row['marital_status_id'],
                row['age'],
                row['spending_score'], 
                self.todayDate
                ]
    # ===== ETL Steps ========    
        
    def DQCustomers(self):
        ''' Process Products data from Stg'''
        self._initDBConnection()
        # Read information from stg_customer
        table1 = fromdb(self.connection, 'SELECT * FROM stg_customer')
        if (len(table1) > 0):
            logging.info(f'rows to process in stg_customer: {len(table1)}')
            # Uniqueness
            table2 = unique(table1, 'Customer_Name')
            # Cleanig data
            logging.info('Cleaning data')
            table3 = rowmapmany(table2, self.cleanData,header=['customer_name','type_customer_name','gender_name','marital_status_name','age','spending_score'])
            # Validate existing records to either insert
            typeCustomer = fromdb(self.connection, 'SELECT * FROM type_customer')
            table4 = lookupjoin(table3, typeCustomer, key='type_customer_name')
            logging.info(look(table4))
            gender = fromdb(self.connection, 'SELECT * FROM gender')
            table4 = lookupjoin(table4, gender, key='gender_name')
            logging.info(look(table4))
            maritalStatus = fromdb(self.connection, 'SELECT * FROM marital_status')
            table4 = lookupjoin(table4, maritalStatus, key='marital_status_name')
            logging.info(look(table4))
            customers = fromdb(self.connection, 'SELECT * FROM customer')
            if (len(customers) > 0):
                logging.info('Comparing rows in product')
                table4 = leftjoin(table4, customers, key="customer_name")
                table4 = select(table4, "{customer_id} is None")
                table5 = rowmap(table4, self.selectColumns , header=['customer_name','type_customer_id','gender_id','marital_status_id','age','spending_score','created_dt'])
                logging.info(look(table5))
                appenddb(table5, self.connection, 'customer')
            else:
                logging.info('No new records found')
        else:
            logging.info('No records to process')
        self._closeConnection()


# etl = CustomerETL()
# etl.DQCustomers()