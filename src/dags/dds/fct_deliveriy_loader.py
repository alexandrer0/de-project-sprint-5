import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

from lib import PgConnect
from psycopg import Connection
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from dds.delivery_repositories import DeliveryRawRepository, DeliveryDdsRepository, DeliveryJsonObj
from dds.order_repositories import OrderDdsRepository
from dds.courier_loader import CourierDdsRepository


log = logging.getLogger(__name__)


class FctDeliveryDdsObj(BaseModel):
    order_id: int
    delivery_id: int
    courier_id: int
    order_ts: datetime
    delivery_ts: datetime
    address: str
    rate: float
    tip_sum: float
    total_sum: float


class FctDeliveryDdsRepository:
    def insert_delivery(self, conn: Connection, delivery: FctDeliveryDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(
                        order_id,
                        delivery_id,
                        courier_id,
                        order_ts,
                        delivery_ts,
                        address,
                        rate,
                        tip_sum,
                        total_sum
                    )
                    VALUES (
                        %(order_id)s,
                        %(delivery_id)s,
                        %(courier_id)s,
                        %(order_ts)s,
                        %(delivery_ts)s,
                        %(address)s,
                        %(rate)s,
                        %(tip_sum)s,
                        %(total_sum)s
                    )
                    ON CONFLICT (order_id) DO UPDATE
                    SET
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        order_ts = EXCLUDED.order_ts,
                        delivery_ts = EXCLUDED.delivery_ts,
                        address = EXCLUDED.address,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum,
                        total_sum = EXCLUDED.total_sum
                    ;
                """,
                {
                    "order_id": delivery.order_id,
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "order_ts": delivery.order_ts,
                    "delivery_ts": delivery.delivery_ts,
                    "address": delivery.address,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum,
                    "total_sum": delivery.total_sum
                },
            )

class FctDeliveryLoader:
    WF_KEY = "fct_delivery_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = DeliveryRawRepository()
        self.dds_couriers = CourierDdsRepository()
        self.dds_order = OrderDdsRepository()
        self.dds_delivery = DeliveryDdsRepository()
        self.fct_dds_delivery = FctDeliveryDdsRepository()
        self.settings_repository = settings_repository

    def parse_delivery(self, delivery_raw: DeliveryJsonObj, order_id: int, delivery_id: int, courier_id: int) -> FctDeliveryDdsObj:

        delivery_json = json.loads(delivery_raw.object_value)

        t = FctDeliveryDdsObj(
            order_id=order_id,
            delivery_id=delivery_id,
            courier_id=courier_id,
            order_ts=datetime.strptime(delivery_json['order_ts'], "%Y-%m-%d %H:%M:%S.%f"),
            delivery_ts=datetime.strptime(delivery_json['delivery_ts'], "%Y-%m-%d %H:%M:%S.%f"),
            address=delivery_json['address'],
            rate=delivery_json['rate'],
            tip_sum=delivery_json['tip_sum'],
            total_sum=delivery_json['sum']
        )
        return t

    def load_delivery(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_delivery(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            for delivery_raw in load_queue:
                delivery_json = json.loads(delivery_raw.object_value)

                delivery = self.dds_delivery.get_delivery(conn, delivery_json['delivery_id'])
                if not delivery:
                    break

                order = self.dds_order.get_order(conn, delivery_json['order_id'])
                if not order:
                    break

                courier = self.dds_couriers.get_courier(conn, delivery_json['courier_id'])
                if not courier:
                    break

                delivery_to_load = self.parse_delivery(delivery_raw, order.id, delivery.id, courier.id)

                self.fct_dds_delivery.insert_delivery(conn, delivery_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = delivery_raw.id
                self.settings_repository.save_setting(conn, wf_setting)