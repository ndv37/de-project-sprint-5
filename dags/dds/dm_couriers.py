from logging import Logger
from typing import List

from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierObj(BaseModel):
    id: int
    courier_id: str
    courier_first_name: str
    courier_last_name: str


class CouriersStg:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, courier_threshold: int) -> List[CourierObj]:
        with self._db.client().cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                SELECT
                    c.id AS id,
                    c.object_id AS courier_id,
                    (STRING_TO_ARRAY(c.object_value->>'name', ' '))[1] AS courier_first_name,
	                (STRING_TO_ARRAY(c.object_value->>'name', ' '))[2] AS courier_last_name
                FROM stg.deliverysystem_couriers c
                WHERE c.id > %(threshold)s
                ORDER BY c.id ASC;
                """, {
                    "threshold": courier_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class CouriersDDS:
    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_first_name, courier_last_name)
                    VALUES (%(courier_id)s, %(courier_first_name)s, %(courier_last_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_first_name = EXCLUDED.courier_first_name,
                        courier_last_name = EXCLUDED.courier_last_name;
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_first_name": courier.courier_first_name,
                    "courier_last_name": courier.courier_last_name
                },
            )


class CourierLoader:
    WF_KEY = "dds.dm_couriers_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersStg(pg_origin)
        self.dds = CouriersDDS()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
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
            load_queue = self.origin.list_couriers(last_loaded)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                self.dds.insert_courier(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")