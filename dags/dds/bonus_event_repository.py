from datetime import datetime
from typing import List

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class BonusEventRepository:

    def load_raw_events(self, conn: Connection, event_type: str, last_loaded_record_id: int) -> List[EventObj]:
        with conn.cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE event_type = %(event_type)s AND id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {
                    "event_type": event_type,
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs
