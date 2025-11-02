import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from stg.delivery_system.delivery_system_load import LoadDataToStg

log = logging.getLogger(__name__)


@dag(
    dag_id='stg_step8_delivery',
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 3, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'delivery_system', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_delivery_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    load_delivery_data = LoadDataToStg(dwh_pg_connect)

    @task()
    def load_couriers():
        load_delivery_data.insert_couriers_data()

    @task()
    def load_deliveries():
        load_delivery_data.insert_deliveries_data()

    load_couriers_task = load_couriers()
    load_deliveries_task = load_deliveries()

    [load_couriers_task, load_deliveries_task]


stg_delivery_dag = stg_delivery_system_dag()