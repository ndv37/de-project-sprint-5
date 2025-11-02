from logging import Logger
from typing import List

from dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class SalesObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class SalesStg:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sales(self, rank_threshold: int, limit: int) -> List[SalesObj]:
        with self._db.client().cursor(row_factory=class_row(SalesObj)) as cur:
            cur.execute(
                """
                    with cte_event as (
                        select
                            e.id as id,
                            e.event_value::json->>'order_id' as order_id,
                            jsonb_array_elements(e.event_value::jsonb -> 'product_payments')->>'product_id' as product_id,
                            (jsonb_array_elements(e.event_value::jsonb -> 'product_payments')->>'product_cost')::numeric(19,5) as total_sum,
                            (jsonb_array_elements(e.event_value::jsonb -> 'product_payments')->>'quantity')::int as count,
                            (jsonb_array_elements(e.event_value::jsonb -> 'product_payments')->>'price')::numeric(19,5) as price,
                            (jsonb_array_elements(e.event_value::jsonb -> 'product_payments')->>'bonus_payment')::numeric(19,5) as bonus_payment,
                            (jsonb_array_elements(e.event_value::jsonb -> 'product_payments')->>'bonus_grant')::numeric(19,5) as bonus_grant
                        from stg.bonussystem_events e
                        where event_type = 'bonus_transaction'
                          --and e.event_value::json->>'order_id' = '67d764e5427cfdf6f94bfcb7'
                    )
                    select 
                        e.id as id,
                        p.id as product_id,
                        o.id as order_id,
                        e.count as count,
                        e.price as price,
                        e.total_sum as total_sum,
                        e.bonus_payment as bonus_payment,
                        e.bonus_grant as bonus_grant
                    from cte_event e
                    join dds.dm_orders o on o.order_key = e.order_id
                    join dds.dm_products p on p.product_id = e.product_id 
                    WHERE e.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY e.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    --LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SalesDDS:

    def insert_sale(self, conn: Connection, sale: SalesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (product_id, order_id) DO UPDATE
                    SET
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "product_id": sale.product_id,
                    "order_id": sale.order_id,
                    "count": sale.count,
                    "price": sale.price,
                    "total_sum": sale.total_sum,
                    "bonus_payment": sale.bonus_payment,
                    "bonus_grant": sale.bonus_grant
                },
            )


class SaleLoader:
    WF_KEY = "dds.fct_sales_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SalesStg(pg_origin)
        self.dds = SalesDDS()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_sales(self):
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
            load_queue = self.origin.list_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rank in load_queue:
                self.dds.insert_sale(conn, rank)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")