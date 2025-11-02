import logging

import pendulum
from airflow.decorators import dag, task
from cdm.dm_settlement_report import SettlementLoader
from cdm.dm_courier_ledger import CourierLedgerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id='reporting',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'origin', 'dds_to_cdm'],
    is_paused_upon_creation=True
)
def dds_to_cdm_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="cdm_settlement_report_load")
    def load_settlements():
        rest_loader = SettlementLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_settlements()

    @task(task_id="cdm_courier_ledger_load")
    def load_courier_ledgers():
        rest_loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_courier_ledgers()

    # Инициализируем объявленные таски.
    settlement_report_dict = load_settlements()
    courier_ledger_dict = load_courier_ledgers()

    [settlement_report_dict, courier_ledger_dict]


dds_to_cdm_dag = dds_to_cdm_dag()