import logging

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from config_const import ConfigConst
from repositories.pg_connect import ConnectionBuilder
from stg.delivery_system.collection_copier import CollectionCopier
from stg.delivery_system.collection_loader import CollectionLoader
from stg.delivery_system.pg_saver import PgSaver

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=False
)
def sprint5_case_stg_delivery_system():
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    api_url = Variable.get(ConfigConst.API_URL)
    headers = {'X-Nickname': Variable.get(ConfigConst.X_NICKNAME),
               'X-Cohort': Variable.get(ConfigConst.X_COHORT),
               'X-API-KEY': Variable.get(ConfigConst.X_API_KEY)}

    @task()
    def load_couriers():
        pg_saver = PgSaver(dwh_pg_connect)
        collection_loader = CollectionLoader(api_url, headers)
        copier = CollectionCopier(collection_loader, pg_saver, log)

        copier.run_copy('couriers', '_id', 'asc', 50)

    @task()
    def load_deliveries():
        pg_saver = PgSaver(dwh_pg_connect)
        collection_loader = CollectionLoader(api_url, headers)
        copier = CollectionCopier(collection_loader, pg_saver, log)

        copier.run_copy('deliveries', 'delivery_id', 'asc', 50)

    courier_loader = load_couriers()
    delivery_loader = load_deliveries()

    courier_loader  # type: ignore
    delivery_loader  # type: ignore


delivery_stg_dag = sprint5_case_stg_delivery_system()  # noqa
