import json
from datetime import datetime
from typing import List, Optional

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from dds.delivery_repositories import DeliveryRawRepository, DeliveryDdsRepository, DeliveryJsonObj, DeliveryDdsObj

class DeliveryLoader:
    WF_KEY = "delivery_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = DeliveryRawRepository()
        self.dds_delivery = DeliveryDdsRepository()
        self.settings_repository = settings_repository

    def parse_delivery(self, delivery_raw: DeliveryJsonObj) -> DeliveryDdsObj:
        delivery_json = json.loads(delivery_raw.object_value)

        t = DeliveryDdsObj(
            id=0,
            delivery_id=delivery_json['delivery_id']
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

                delivery_to_load = self.parse_delivery(delivery_raw)
                self.dds_delivery.insert_delivery(conn, delivery_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = delivery_raw.id
                self.settings_repository.save_setting(conn, wf_setting)