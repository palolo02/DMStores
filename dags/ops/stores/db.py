import psycopg2
import json
from functools import partial
from collections import OrderedDict
from datetime import date
import logging


class ETL:

    def __init__(self):
        self.configuration = None
        self.todayDate = date.today()
        print('ETL Constructor for the configuration')
        
    
    def _readConfig(self):
        '''Read configuration file'''
        with open("dags/ops/config/config.json", "r") as config:
            print('Reading config file')
            self.configuration = json.loads(config.read())

    def _initDBConnection(self):
        ''' Initialize DB connection to load data '''
        logging.info('Connecting to DB')
        self._readConfig()
        if(self.configuration is not None):
            self.connection = psycopg2.connect(host=self.configuration["db_connection"]["host"], 
                                            port=self.configuration["db_connection"]["port"], 
                                            dbname=self.configuration["db_connection"]["database"], 
                                            user=self.configuration["db_connection"]["user"], 
                                            password=self.configuration["db_connection"]["password"])
    
    def _closeConnection(self):
        ''' Close the database connection and free resources '''
        if (self.connection is not None):
                self.connection.close()
    
    def cleanData(self):
        pass
    
    def selectColumns(self):
        pass