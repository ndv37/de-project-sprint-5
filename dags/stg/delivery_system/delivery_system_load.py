from datetime import datetime
import json
import requests
from typing import Optional, List

from airflow.hooks.http_hook import HttpHook
from lib import PgConnect
from lib.dict_util import json2str
from stg import EtlSetting, StgEtlSettingsRepository

from stg.delivery_system.exceptions import (GetCouriersDataException, GetDeliveriesDataException,
                                            LoadCouriersDataException, LoadDeliveriesDataException)
from stg.delivery_system.models import CouriersSchema, DeliveriesSchema


nickname = 'naumov-dv'
cohort = '1'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


class GetDeliverySystemData:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
        self.http_conn_id = HttpHook.get_connection('http_connection')
        self.base_url = self.http_conn_id.host
        self.settings_repository = StgEtlSettingsRepository()

    def get_couriers(self,
                     sort_field: Optional[str] = None,
                     sort_direction: Optional[str] = None,
                     limit: Optional[int] = None,
                     offset: Optional[int] = None
                     ) -> List[CouriersSchema]:
        """Получает данные о курьерах из системы доставок."""
        all_data: list = []
        limit: int = 50 if not limit else limit
        offset: int = 0 if not offset else offset
        sort_field: str = 'name' if not sort_field else sort_field
        sort_direction: str = 'asc' if not sort_direction else sort_direction

        while True:
            url: str = (f'{self.base_url}/couriers?sort_field={sort_field}&sort_direction={sort_direction}' +
                        f'&limit={limit}&offset={offset}')
            try:
                response = requests.get(url=url, headers=headers)
                response.raise_for_status()
                response = json.loads(response.content)

                if not response:
                    break
                all_data.extend(response)

                if len(response) < limit:
                    break
                offset += limit

            except Exception as e:
                raise GetCouriersDataException(f'Error while receiving data: {e}')
        return all_data

    def get_deliveries(self,
                       restaurant_id: str = '',
                       date_from: Optional[str] = datetime.strftime(datetime(2022, 1, 1), '%Y-%m-%d+%H:%M:%S'),
                       date_to: Optional[str] = datetime.strftime(datetime(2050, 1, 1), '%Y-%m-%d+%H:%M:%S'),
                       sort_field: Optional[str] = None,
                       sort_direction: Optional[str] = None,
                       limit: Optional[int] = None,
                       offset: Optional[int] = None
                       ) -> List[DeliveriesSchema]:
        """Получает данные о доставках из системы доставок."""
        all_data = []
        limit = 50 if not limit else limit
        offset = 0 if not offset else offset
        sort_field: str = 'order_ts' if not sort_field else sort_field
        sort_direction: str = 'asc' if not sort_direction else sort_direction

        while True:
            url: str = (f'{self.base_url}/deliveries?restaurant_id={restaurant_id}&from={date_from}&to={date_to}' +
                        f'&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}')
            try:
                response = requests.get(url=url, headers=headers)
                response.raise_for_status()
                response = json.loads(response.content)

                if not response:
                    break
                all_data.extend(response)

                if len(response) < limit:
                    break
                offset += limit

            except Exception as e:
                raise GetDeliveriesDataException(f'Error while receiving data: {e}')
        return all_data


class LoadDataToStg:
    def __init__(self, pg_dwh: PgConnect) -> None:
        self._pg_dwh = pg_dwh
        self.settings_repository = StgEtlSettingsRepository()
        self.get_data = GetDeliverySystemData(pg_dwh)

    def insert_couriers_data(self) -> None:
        try:
            with self._pg_dwh.connection() as conn:
                with conn.cursor() as cur:
                    couriers = self.get_data.get_couriers()
                    for courier in couriers:
                        object_value = json2str(courier)
                        cur.execute(
                            """
                            INSERT INTO stg.deliverysystem_couriers(object_id, object_value, load_ts)
                            VALUES (%(object_id)s, %(object_value)s, %(load_ts)s)
                            ON CONFLICT (object_id) DO UPDATE
                            SET object_value = EXCLUDED.object_value,
                                load_ts = EXCLUDED.load_ts;
                            """,
                            {
                                "object_id": courier.get('_id'),
                                "object_value": object_value,
                                "load_ts": datetime.now()
                            },
                        )
        except Exception as e:
            raise LoadCouriersDataException(f'Error while loading data: {e}')

    def insert_deliveries_data(self) -> None:
        WF_KEY = "delivery_system_deliveries_origin_to_stg_workflow"
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        try:
            with self._pg_dwh.connection() as conn:
                with conn.cursor() as cur:
                    wf_setting = self.settings_repository.get_setting(conn, WF_KEY)
                    if not wf_setting:
                        wf_setting = EtlSetting(
                            id=0,
                            workflow_key=WF_KEY,
                            workflow_settings={
                                LAST_LOADED_TS_KEY: datetime.strftime(datetime(2022, 1, 1), '%Y-%m-%d %H:%M:%S')
                            }
                        )
                    last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
                    deliveries = self.get_data.get_deliveries(date_from=last_loaded_ts_str)

                    for delivery in deliveries:
                        object_value = json2str(delivery)
                        cur.execute(
                            """
                            INSERT INTO stg.deliverysystem_deliveries(object_id, object_value, load_ts)
                            VALUES (%(object_id)s, %(object_value)s, %(load_ts)s)
                            ON CONFLICT (object_id) DO UPDATE
                            SET object_value = EXCLUDED.object_value,
                                load_ts = EXCLUDED.load_ts;
                            """,
                            {
                                "object_id": delivery.get('order_id'),
                                "object_value": object_value,
                                "load_ts": datetime.now()
                            },
                        )

                    if len(deliveries) > 0:
                        max_loaded_ts = str(max([t["order_ts"] for t in deliveries])).split('.')[0]
                        wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = max_loaded_ts
                        wf_setting_json = json2str(wf_setting.workflow_settings)
                        self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

        except Exception as e:
            raise LoadDeliveriesDataException(f'Error while loading data: {e}')