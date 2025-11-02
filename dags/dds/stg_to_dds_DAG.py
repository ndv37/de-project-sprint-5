import logging

import pendulum
from airflow.decorators import dag, task
from dds.dm_users import UserLoader
from dds.dm_restaurants import RestaurantLoader
from dds.dm_timestamps import TimestampLoader
from dds.dm_products import ProductLoader
from dds.dm_orders import OrderLoader
from dds.fct_product_sales import SaleLoader
from dds.dm_couriers import CourierLoader
from dds.dm_addresses import AddressLoader
from dds.dm_delivery_timestamps import TimestampDeliveryLoader
from dds.dm_deliveries import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id='stg_step9_to_dds',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'origin', 'stage_to_dds'],
    is_paused_upon_creation=True
)
def stg_to_dds_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_users_load")
    def load_users():
        rest_loader = UserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()

    @task(task_id="dm_restaurants_load")
    def load_restaurants():
        rest_loader = RestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()

    @task(task_id="dm_timestamps_load")
    def load_timestamps():
        rest_loader = TimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()

    @task(task_id="dm_products_load")
    def load_products():
        rest_loader = ProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()

    @task(task_id="dm_orders_load")
    def load_orders():
        rest_loader = OrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()

    @task(task_id="fct_product_sales_load")
    def load_product_sales():
        rest_loader = SaleLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_sales()

    @task(task_id="dm_couriers_load")
    def load_couriers():
        loader = CourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_couriers()

    @task(task_id="dm_addresses_load")
    def load_addresses():
        loader = AddressLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_addresses()

    @task(task_id="dm_timestamps_delivery_load")
    def load_timestamps_delivery():
        loader = TimestampDeliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_timestamps()

    @task(task_id="dm_deliveries_load")
    def load_deliveries():
        loader = DeliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_deliveries()

    # Инициализируем объявленные таски.
    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products_dict = load_products()
    orders_dict = load_orders()
    sales_dict = load_product_sales()
    couriers_dict = load_couriers()
    addresses_dict = load_addresses()
    timestamps_delivery_dict = load_timestamps_delivery()
    deliveries_dict = load_deliveries()

    users_dict >> orders_dict >> sales_dict
    restaurants_dict >> [products_dict, orders_dict] >> sales_dict
    timestamps_dict >> deliveries_dict >> orders_dict >> sales_dict
    couriers_dict >> deliveries_dict
    addresses_dict >> deliveries_dict
    timestamps_delivery_dict >> deliveries_dict


stg_to_dds_dag = stg_to_dds_dag()