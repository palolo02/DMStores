import sys
import os
sys.path.append('/opt/airflow')
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from ops.stg.staging import STG

# Classes for all the tasks in Stores
class STGOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = STG()

    def execute(self, context):
        message = "Running STG Loading Process"
        self.etl.loadCSV()
        print(message)
        return message

