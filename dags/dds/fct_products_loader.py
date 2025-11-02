import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

from lib import PgConnect
from psycopg import Connection
from pydantic import BaseModel

from dds.bonus_event_repository import BonusEventRepository
from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from dds.order_repositories import OrderDdsRepository
from dds.products_loader import ProductDdsObj, ProductDdsRepository

log = logging.getLogger(__name__)


class FctProductDdsObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class ProductPaymentJsonObj:
    def __init__(self, d: Dict) -> None:
        self.product_id: str = d["product_id"]
        self.product_name: str = d["product_name"]
        self.price: float = d["price"]
        self.quantity: int = d["quantity"]
        self.product_cost: float = d["product_cost"]
        self.bonus_payment: float = d["bonus_payment"]
        self.bonus_grant: float = d["bonus_grant"]


class BonusPaymentJsonObj:
    EVENT_TYPE = "bonus_transaction"

    def __init__(self, d: Dict) -> None:
        self.user_id: int = d["user_id"]
        self.order_id: str = d["order_id"]
        self.order_date: datetime = datetime.strptime(d["order_date"], "%Y-%m-%d %H:%M:%S")
        self.product_payments = [ProductPaymentJsonObj(it) for it in d["product_payments"]]


class FctProductDdsRepository:
    def insert_facts(self, conn: Connection, facts: List[FctProductDdsObj]) -> None:
        with conn.cursor() as cur:
            for fact in facts:
                cur.execute(
                    """
                        INSERT INTO dds.fct_product_sales(
                            order_id,
                            product_id,
                            count,
                            price,
                            total_sum,
                            bonus_payment,
                            bonus_grant
                        )
                        VALUES (
                            %(order_id)s,
                            %(product_id)s,
                            %(count)s,
                            %(price)s,
                            %(total_sum)s,
                            %(bonus_payment)s,
                            %(bonus_grant)s
                        )
                        ON CONFLICT (order_id, product_id) DO UPDATE
                        SET
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant
                        ;
                    """,
                    {
                        "product_id": fact.product_id,
                        "order_id": fact.order_id,
                        "count": fact.count,
                        "price": fact.price,
                        "total_sum": fact.total_sum,
                        "bonus_payment": fact.bonus_payment,
                        "bonus_grant": fact.bonus_grant
                    },
                )


class FctProductsLoader:
    PAYMENT_EVENT = "bonus_transaction"
    WF_KEY = "fact_product_events_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_event_id"

    _LOG_THRESHOLD = 100

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw_events = BonusEventRepository()
        self.dds_orders = OrderDdsRepository()
        self.dds_products = ProductDdsRepository()
        self.dds_facts = FctProductDdsRepository()
        self.settings_repository = settings_repository

    def parse_order_products(self,
                             order_raw: BonusPaymentJsonObj,
                             order_id: int,
                             products: Dict[str, ProductDdsObj]
                             ) -> Tuple[bool, List[FctProductDdsObj]]:

        res = []

        for p_json in order_raw.product_payments:
            if p_json.product_id not in products:
                return (False, [])

            t = FctProductDdsObj(id=0,
                                 order_id=order_id,
                                 product_id=products[p_json.product_id].id,
                                 count=p_json.quantity,
                                 price=p_json.price,
                                 total_sum=p_json.product_cost,
                                 bonus_grant=p_json.bonus_grant,
                                 bonus_payment=p_json.bonus_payment
                                 )
            res.append(t)

        return (True, res)

    def load_product_facts(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            log.info(f"Starting load from: {last_loaded_id}")

            load_queue = self.raw_events.load_raw_events(conn, self.PAYMENT_EVENT, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            log.info(f"Found {len(load_queue)} events to load.")

            products = self.dds_products.list_products(conn)
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p

            proc_cnt = 0
            for payment_raw in load_queue:
                payment_obj = BonusPaymentJsonObj(json.loads(payment_raw.event_value))
                order = self.dds_orders.get_order(conn, payment_obj.order_id)
                if not order:
                    log.info(f"Not found order {payment_obj.order_id}. Finishing.")
                    continue

                (success, facts_to_load) = self.parse_order_products(payment_obj, order.id, prod_dict)
                if not success:
                    log.info(f"Could not parse object for order {order.id}. Finishing.")
                    continue

                self.dds_facts.insert_facts(conn, facts_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = payment_raw.id
                self.settings_repository.save_setting(conn, wf_setting)

                proc_cnt += 1
                if proc_cnt % self._LOG_THRESHOLD == 0:
                    log.info(f"Processing events {proc_cnt} out of {len(load_queue)}.")

            log.info(f"Processed {proc_cnt} events out of {len(load_queue)}.")
