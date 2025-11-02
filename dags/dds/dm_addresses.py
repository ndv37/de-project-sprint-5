from logging import Logger
from typing import List

from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class AddressObj(BaseModel):
    id: int
    address: str
    street: str
    building: str
    flat: str


class AddressesStg:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_addresses(self, address_threshold: int) -> List[AddressObj]:
        with self._db.client().cursor(row_factory=class_row(AddressObj)) as cur:
            cur.execute(
                """
                SELECT
                    d.id AS id,
                    d.object_value->>'address' AS address,
                    (STRING_TO_ARRAY(d.object_value->>'address', ','))[1] AS street,
                    (STRING_TO_ARRAY(d.object_value->>'address', ','))[2] AS building,
                    (STRING_TO_ARRAY(d.object_value->>'address', ','))[3] AS flat
                FROM stg.deliverysystem_deliveries d
                WHERE d.id > %(threshold)s
                ORDER BY d.id ASC;
                """, {
                    "threshold": address_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class AddressesDDS:
    def insert_address(self, conn: Connection, address: AddressObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_addresses(address, street, building, flat)
                    VALUES (%(address)s, %(street)s, %(building)s, %(flat)s)
                    ON CONFLICT (address) DO NOTHING;
                """,
                {
                    "address": address.address,
                    "street": address.street,
                    "building": address.building,
                    "flat": address.flat
                },
            )


class AddressLoader:
    WF_KEY = "dds.dm_addresses_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = AddressesStg(pg_origin)
        self.dds = AddressesDDS()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_addresses(self):
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
            load_queue = self.origin.list_addresses(last_loaded)
            self.log.info(f"Found {len(load_queue)} addresses to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for address in load_queue:
                self.dds.insert_address(conn, address)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")