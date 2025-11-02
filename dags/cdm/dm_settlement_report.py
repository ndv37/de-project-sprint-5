from datetime import datetime
from logging import Logger
from typing import List
import pendulum

from cdm import EtlSetting, CDMEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class SettlementObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: datetime
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class SettlementsDDS:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_settlements(self, rank_threshold: datetime) -> List[SettlementObj]:
        with self._db.client().cursor(row_factory=class_row(SettlementObj)) as cur:
            cur.execute(
                """
                    select 
                        r.restaurant_id ,
                        r.restaurant_name ,
                        date_trunc('day', t.ts) as settlement_date,
                        count(distinct f.order_id) as orders_count,
                        sum(f.total_sum) as orders_total_sum,
                        sum(f.bonus_payment) as orders_bonus_payment_sum,
                        sum(f.bonus_grant) as orders_bonus_granted_sum,
                        sum(f.total_sum) * 0.25 as order_processing_fee,
                        sum(f.total_sum) - sum(f.bonus_payment) - sum(f.total_sum) * 0.25 as restaurant_reward_sum
                    from dds.fct_product_sales f
                    join dds.dm_orders o on f.order_id = o.id
                    join dds.dm_restaurants r on r.id = o.restaurant_id 
                    join dds.dm_timestamps t on t.id = o.timestamp_id 
                    where o.order_status = 'CLOSED'
                      and date_trunc('day', t.ts) >= (%(threshold)s)::timestamp --Пропускаем те объекты, которые уже загрузили.
                    group by 
                        r.restaurant_id ,
                        r.restaurant_name,
                        date_trunc('day', t.ts)
                    ORDER BY settlement_date ASC; --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": rank_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class SettlementsCDM:
    def insert_settlement(self, conn: Connection, settlement: SettlementObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": settlement.restaurant_id,
                    "restaurant_name": settlement.restaurant_name,
                    "settlement_date": settlement.settlement_date,
                    "orders_count": settlement.orders_count,
                    "orders_total_sum": settlement.orders_total_sum,
                    "orders_bonus_payment_sum": settlement.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": settlement.orders_bonus_granted_sum,
                    "order_processing_fee": settlement.order_processing_fee,
                    "restaurant_reward_sum": settlement.restaurant_reward_sum
                },
            )


class SettlementLoader:
    WF_KEY = "cdm_settlement_report_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SettlementsDDS(pg_origin)
        self.dds = SettlementsCDM()
        self.settings_repository = CDMEtlSettingsRepository()
        self.log = log

    def load_settlements(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: pendulum.datetime(1900, 1, 1, tz="UTC")})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_settlements(last_loaded)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rank in load_queue:
                self.dds.insert_settlement(conn, rank)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.settlement_date for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")