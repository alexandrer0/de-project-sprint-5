from typing import List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class DeliveryRawRepository:
    def load_raw_delivery(self, conn: Connection, last_loaded_record_id: int) -> List[DeliveryJsonObj]:
        with conn.cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        objs.sort(key=lambda x: x.id)
        return objs


class DeliveryDdsObj(BaseModel):
    id: int
    delivery_id: str

class DeliveryDdsRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id)
                    VALUES (%(delivery_id)s)
                    ON CONFLICT (delivery_id) DO NOTHING;
                """,
                {
                    "delivery_id": delivery.delivery_id
                },
            )

    def get_delivery(self, conn: Connection, delivery_id: str) -> Optional[DeliveryDdsObj]:
        with conn.cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        delivery_id
                    FROM dds.dm_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {"delivery_id": delivery_id},
            )
            obj = cur.fetchone()
        return obj
