import sys
import os
sys.path.append('/opt/airflow')
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from ops.stores.country import CountryETL
from ops.stores.city import CityETL
from ops.stores.customer import CustomerETL
from ops.stores.inventory import InventoryETL
from ops.stores.product import ProductETL
from ops.stores.productCategory import ProductCategoryETL
from ops.stores.purchase import PurchaseETL
from ops.stores.sales import SalesETL
from ops.stores.salesStatus import SaleStatusETL
from ops.stores.stores import StoresETL
from ops.stores.storeType import StoreTypesETL
from ops.stores.customerType import CustomerTypeETL
from ops.stores.gender import GenderETL
from ops.stores.maritalStatus import MaritalStatusETL
from ops.stores.rating import RatingETL



# Classes for all the tasks in Stores
class CountryOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = CountryETL()

    def execute(self, context):
        message = "Running Country Process"
        self.etl.DQCountry()
        print(message)
        return message


# Classes for all the tasks in Stores
class CityOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = CityETL()

    def execute(self, context):
        message = "Running City Process"
        self.etl.DQCity()
        print(message)
        return message

# Classes for all the tasks in Stores
class CustomerOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = CustomerETL()

    def execute(self, context):
        message = "Running Customer Process"
        self.etl.DQCustomers()
        print(message)
        return message

# Classes for all the tasks in Stores
class InventoryOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = InventoryETL()

    def execute(self, context):
        message = "Running Inventory Process"
        self.etl.DQInventory()
        print(message)
        return message

# Classes for all the tasks in Stores
class ProductOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = ProductETL()

    def execute(self, context):
        message = "Running Products Process"
        self.etl.DQProducts()
        print(message)
        return message


# Classes for all the tasks in Stores
class ProductCategoryOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = ProductCategoryETL()

    def execute(self, context):
        message = "Running Product Categories Process"
        self.etl.DQProductCategories()
        print(message)
        return message

# Classes for all the tasks in Stores
class PurchaseOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = PurchaseETL()

    def execute(self, context):
        message = "Running Purchase Process"
        self.etl.DQPurchase()
        print(message)
        return message


# Classes for all the tasks in Stores
class SalesOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = SalesETL()

    def execute(self, context):
        message = "Running Sales Process"
        self.etl.DQSales()
        print(message)
        return message


# Classes for all the tasks in Stores
class SalesStatusOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = SaleStatusETL()

    def execute(self, context):
        message = "Running Sale Status Process"
        self.etl.DQSaleStatus()
        print(message)
        return message


# Classes for all the tasks in Stores
class StoresOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = StoresETL()

    def execute(self, context):
        message = "Running Stores Process"
        self.etl.DQStores()
        print(message)
        return message


# Classes for all the tasks in Stores
class StoreTypeOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = StoreTypesETL()

    def execute(self, context):
        message = "Running Stores Process"
        self.etl.DQStoreType()
        print(message)
        return message


# Classes for all the tasks in Stores
class CustomerTypeOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = CustomerTypeETL()

    def execute(self, context):
        message = "Running Customer Type Process"
        self.etl.DQCustomerType()
        print(message)
        return message


# Classes for all the tasks in Stores
class GenderOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = GenderETL()

    def execute(self, context):
        message = "Running Gender Process"
        self.etl.DQGender()
        print(message)
        return message

# Classes for all the tasks in Stores
class MaritalStatusOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = MaritalStatusETL()

    def execute(self, context):
        message = "Running Gender Process"
        self.etl.DQMaritalStatus()
        print(message)
        return message

# Classes for all the tasks in Stores
class RatingOperator(BaseOperator):
    #ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.etl = RatingETL()

    def execute(self, context):
        message = "Running Rating Process"
        self.etl.DQRating()
        print(message)
        return message

