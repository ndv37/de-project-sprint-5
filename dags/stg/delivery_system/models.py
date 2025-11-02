from datetime import datetime
from pydantic import BaseModel

class CouriersSchema(BaseModel):
    id: str
    name: str


class DeliveriesSchema(BaseModel):
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: int
    sum: float
    tip_sum: float