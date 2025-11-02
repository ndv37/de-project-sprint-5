import logging

import pendulum
from airflow import DAG
from airflow.decorators import task
#from config_const import ConfigConst
from lib import ConnectionBuilder

from dds.dds_settings_repository import DdsEtlSettingsRepository
from dds.order_loader import OrderLoader
from dds.schema_ddl import SchemaDdl

log = logging.getLogger(__name__)

with DAG(
    dag_id='sprint5_case_dds_snowflake_test_orders',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'raw', 'dds'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    settings_repository = DdsEtlSettingsRepository()

    
    @task(task_id="dm_orders_load")
    def load_dm_orders(ds=None, **kwargs):
        order_loader = OrderLoader(dwh_pg_connect, settings_repository)
        order_loader.load_orders()
    

    dm_orders = load_dm_orders()
    dm_orders  # type: ignore
    