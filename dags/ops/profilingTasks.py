import sys
import os
sys.path.append('/opt/airflow')
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from ops.profiling.dataProfiling import Profiler

# Classes for all the tasks in Stores
class ProfilerOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = Profiler()

    def execute(self, context):
        message = "Running Profiling Process"
        self.etl.profileData()
        print(message)
        return message

