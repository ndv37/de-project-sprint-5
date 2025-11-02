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


class CourierLedgerObj(BaseModel):
    courier_id: str
    courier_name: str
    settlement_date: datetime
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class CourierLedgersDDS:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_courier_ledgers(self, courier_ledger_threshold: datetime) -> List[CourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:
            cur.execute(
                """
                with product_sales_cte as (
                    select
                        order_id,
                        sum(total_sum) as total_sum
                    from dds.fct_product_sales
                    group by order_id
                )
                select
                    dc.courier_id,
                    dc.courier_first_name || ' ' || dc.courier_last_name as courier_name,
                    date_trunc('month', dt.ts) as settlement_date,
                    extract(year from dt.ts) as settlement_year,
                    extract(month from dt.ts) as settlement_month,
                    count(distinct f.order_id) as orders_count,
                    sum(f.total_sum) as orders_total_sum,
                    avg(dd.rate)::numeric(14,2) as rate_avg,
                    sum(f.total_sum) * 0.25 as order_processing_fee,
                    case 
                        when avg(dd.rate) < 4 then case when sum(f.total_sum) * 0.05 >= 100 then sum(f.total_sum) * 0.05 else 100 end
                        when avg(dd.rate) >= 4 and avg(dd.rate) < 4.5 then case when sum(f.total_sum) * 0.07 >= 150 then sum(f.total_sum) * 0.07 else 150 end
                        when avg(dd.rate) >= 4.5 and avg(dd.rate) < 4.9 then case when sum(f.total_sum) * 0.08 >= 175 then sum(f.total_sum) * 0.08 else 175 end
                        when avg(dd.rate) >= 4.9 then case when sum(f.total_sum) * 0.1 >= 200 then sum(f.total_sum) * 0.1 else 200 end
                    end as courier_order_sum,
                    sum(dd.tip_sum) as courier_tips_sum,
                    case 
                        when avg(dd.rate) < 4 then case when sum(f.total_sum) * 0.05 >= 100 then sum(f.total_sum) * 0.05 else 100 end
                        when avg(dd.rate) >= 4 and avg(dd.rate) < 4.5 then case when sum(f.total_sum) * 0.07 >= 150 then sum(f.total_sum) * 0.07 else 150 end
                        when avg(dd.rate) >= 4.5 and avg(dd.rate) < 4.9 then case when sum(f.total_sum) * 0.08 >= 175 then sum(f.total_sum) * 0.08 else 175 end
                        when avg(dd.rate) >= 4.9 then case when sum(f.total_sum) * 0.1 >= 200 then sum(f.total_sum) * 0.1 else 200 end
                    end + sum(dd.tip_sum) * 0.95 as courier_reward_sum
                from product_sales_cte f
                inner join dds.dm_orders do2 on f.order_id = do2.id
                inner join dds.dm_deliveries dd on cast(do2.delivery_id as int) = dd.id 
                inner join dds.dm_couriers dc on dd.courier_id = cast(dc.id as varchar) 
                inner join dds.dm_timestamps dt on do2.timestamp_id = dt.id     
                where date_trunc('month', dt.ts) >= date_trunc('month', (%(threshold)s)::timestamp)        
                group by 
                    dc.courier_id,
                    dc.courier_first_name, dc.courier_last_name,
                    date_trunc('month', dt.ts),
                    extract(year from dt.ts),
                    extract(month from dt.ts)
                order by courier_id, settlement_year, settlement_month asc;
                """, {
                    "threshold": courier_ledger_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class CourierLedgersCDM:
    def insert_courier_ledger(self, conn: Connection, courier_ledger: CourierLedgerObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": courier_ledger.courier_id,
                    "courier_name": courier_ledger.courier_name,
                    "settlement_year": courier_ledger.settlement_year,
                    "settlement_month": courier_ledger.settlement_month,
                    "orders_count": courier_ledger.orders_count,
                    "orders_total_sum": courier_ledger.orders_total_sum,
                    "rate_avg": courier_ledger.rate_avg,
                    "order_processing_fee": courier_ledger.order_processing_fee,
                    "courier_order_sum": courier_ledger.courier_order_sum,
                    "courier_tips_sum": courier_ledger.courier_tips_sum,
                    "courier_reward_sum": courier_ledger.courier_reward_sum
                },
            )


class CourierLedgerLoader:
    WF_KEY = "cdm_courier_ledger_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierLedgersDDS(pg_origin)
        self.dds = CourierLedgersCDM()
        self.settings_repository = CDMEtlSettingsRepository()
        self.log = log

    def load_courier_ledgers(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: pendulum.datetime(1900, 1, 1, tz="UTC")})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_courier_ledgers(last_loaded)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier_ledger in load_queue:
                self.dds.insert_courier_ledger(conn, courier_ledger)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.settlement_date for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")