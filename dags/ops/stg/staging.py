import json
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import io
import logging
from dags.ops.stores.db import ETL

class STG(ETL):

    def __init__(self):
        ''' Initializes the connection and the date for all the rows that would be processed '''
        ETL.__init__(self)
        self.engine = None
        logFile = f'{self.__class__.__name__}.log'
        logging.basicConfig(filename=logFile, level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s')
        logging.info('Starting STG')
        print('STG constructor with DB configuration')

    def loadCSV(self):
        self._readConfig()
        self.engine = create_engine(f'postgresql+psycopg2://{self.configuration["db_connection"]["user"]}:{self.configuration["db_connection"]["password"]}@{self.configuration["db_connection"]["host"]}:{self.configuration["db_connection"]["port"]}/{self.configuration["db_connection"]["database"]}')
        # Load all files and import them into the DB
        for _file in self.configuration["files"]:
            logging.info(f'===== Loading {_file} =====')
            content = pd.read_csv(f'{self.configuration["directory"]}{_file}',delimiter=';')
            # Rename file to cretae the table in DB
            _file = f'stg_{_file.replace(".csv","").lower()}'
            logging.info(f'{content.head()} \n {_file}')
            logging.info('Inserting Data into the DB')
            # Load header for adding the columns
            content.head(0).to_sql(_file, self.engine, if_exists='replace',index=False) #drops old table and creates new empty table
            conn = self.engine.raw_connection()
            cur = conn.cursor()
            output = io.StringIO()
            content.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            contents = output.getvalue()
            cur.copy_from(output, _file, null="") # null values become ''
            conn.commit()
            logging.info('File successfully loaded')
        #self._closeConnection()

    def loadIntoSTG(self):
        # Define the DB connection
        self._initDBConnection()
        self.engine = create_engine(f'postgresql+psycopg2://{self.configuration["db_connection"]["user"]}:{self.configuration["db_connection"]["password"]}@{self.configuration["db_connection"]["host"]}:{self.configuration["db_connection"]["port"]}/{self.configuration["db_connection"]["database"]}')
        logging.info(self.engine)
        # Load all sheets and import them into the DB
        i = 0
        for sheet in self.configuration["sheets_file"]:
            logging.info(f'===== Loading {sheet} in file =====')
            stores_xls = pd.read_excel('data/Stores_v2.xlsx', sheet_name=self.configuration["sheets_file"][i])
            logging.info('Inserting data into DB')
            stores_xls.head(0).to_sql(f'stg_{sheet.lower()}', self.engine, if_exists='replace',index=False) #drops old table and creates new empty table
            conn = self.engine.raw_connection()
            cur = conn.cursor()
            output = io.StringIO()
            stores_xls.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            contents = output.getvalue()
            cur.copy_from(output, f'stg_{sheet.lower()}', null="") # null values become ''
            conn.commit()
            logging.info('Success!')
            i += 1
        self._closeConnection()

# etl = STG()
# etl.loadIntoSTG()