from logging import Logger
from typing import List

from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryObj(BaseModel):
    id: int
    delivery_id: str
    timestamp_id: int
    address_id: int
    courier_id: int
    rate: int
    tip_sum: float


class DeliveriesStg:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, delivery_threshold: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                SELECT 
                    d.id AS id,
                    d.object_value::json->>'delivery_id' AS delivery_id, 
                    dt.id AS timestamp_id,
                    da.id AS address_id,
                    dc.id AS courier_id,
                    d.object_value::json->>'rate' AS rate,
                    d.object_value::json->>'tip_sum' AS tip_sum
                FROM stg.deliverysystem_deliveries d
                INNER JOIN dds.dm_timestamps dt ON dt.ts = cast(d.object_value::json->>'delivery_ts' as timestamp(0))
                INNER JOIN dds.dm_addresses da ON da.address = d.object_value::json->>'address'
                INNER JOIN dds.dm_couriers dc ON dc.courier_id = d.object_value::json->>'courier_id'
                WHERE d.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                ORDER BY d.id ASC; --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": delivery_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveriesDDS:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_deliveries(delivery_id, timestamp_id, address_id, courier_id, rate, tip_sum)
                VALUES (%(delivery_id)s, %(timestamp_id)s, %(address_id)s, %(courier_id)s, %(rate)s, %(tip_sum)s)
                ON CONFLICT (delivery_id) DO UPDATE
                SET
                    timestamp_id = EXCLUDED.timestamp_id,
                    address_id = EXCLUDED.address_id,
                    courier_id = EXCLUDED.courier_id,
                    rate = EXCLUDED.rate,
                    tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "timestamp_id": delivery.timestamp_id,
                    "address_id": delivery.address_id,
                    "courier_id": delivery.courier_id,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum
                },
            )


class DeliveryLoader:
    WF_KEY = "dds.dm_deliveries_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesStg(pg_origin)
        self.dds = DeliveriesDDS()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.dds.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")