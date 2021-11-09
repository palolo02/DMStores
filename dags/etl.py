from datetime import timedelta
from airflow.utils.dates import days_ago
import sys
import os

from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from ops.storeTasks import CountryOperator, CityOperator, CustomerOperator, InventoryOperator, ProductOperator 
from ops.storeTasks import ProductCategoryOperator, PurchaseOperator, SalesOperator, SalesStatusOperator
from ops.storeTasks import  StoresOperator, StoreTypeOperator, CustomerTypeOperator, GenderOperator
from ops.storeTasks import  MaritalStatusOperator, RatingOperator
from ops.profilingTasks import ProfilerOperator
from ops.stgTasks import STGOperator

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'etl',
    default_args=default_args,
    description='ETL for Stores',
    #schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    tags=['dataCleaning', 'dataTransformation']
)

# Start Process
start_operator = DummyOperator(task_id='Begin_execution',dag=dag)
# load STG
stg = STGOperator(task_id='STG',dag=dag)
# Profiling
profiling = ProfilerOperator(task_id='Profiling',dag=dag)
# Catalogues
country = CountryOperator(task_id='Country',dag=dag)
city = CityOperator(task_id='City',dag=dag)
customer = CustomerOperator(task_id='Customer',dag=dag)
product = ProductOperator(task_id='Product',dag=dag)
category = ProductCategoryOperator(task_id='Product_category',dag=dag)
purchase = PurchaseOperator(task_id='Purchase',dag=dag)
sale_status = SalesStatusOperator(task_id='Sale_Status',dag=dag)
store = StoresOperator(task_id='Stores',dag=dag)
store_type = StoreTypeOperator(task_id='Store_Type',dag=dag)
marital_status = MaritalStatusOperator(task_id='Marital_Status',dag=dag)
customer_type = CustomerTypeOperator(task_id='Customer_Type',dag=dag)
gender = GenderOperator(task_id='Gender',dag=dag)
# Facts 
sales = SalesOperator(task_id='Sales',dag=dag)
inventory = InventoryOperator(task_id='Inventory',dag=dag)
rating = RatingOperator(task_id='Ratings',dag=dag)
# End process
end_operator = DummyOperator(task_id='Stop_execution',dag=dag)

# Tasks Dependency
start_operator >> stg >> profiling
profiling >> [country, category, store_type, purchase, sale_status, gender, marital_status, customer_type]
country >> city
category >> product
[gender, marital_status, customer_type] >> customer
[store_type, city] >> store
[store, product] >> inventory
[customer, product] >> rating
[store, product, customer, store_type, purchase, sale_status] >> sales
[sales, inventory, rating] >> end_operator
