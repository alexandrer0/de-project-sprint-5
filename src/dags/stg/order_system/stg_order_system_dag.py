import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from config_const import ConfigConst
from repositories.mongo_connect import MongoConnect
from repositories.pg_connect import ConnectionBuilder
from stg.order_system.collection_copier import CollectionCopier
from stg.order_system.collection_loader import CollectionLoader
from stg.order_system.pg_saver import PgSaver

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin'],
    is_paused_upon_creation=False
)
def sprint5_case_stg_order_system():
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    cert_path = Variable.get(ConfigConst.MONGO_DB_CERTIFICATE_PATH)
    db_user = Variable.get(ConfigConst.MONGO_DB_USER)
    db_pw = Variable.get(ConfigConst.MONGO_DB_PASSWORD)
    rs = Variable.get(ConfigConst.MONGO_DB_REPLICA_SET)
    db = Variable.get(ConfigConst.MONGO_DB_DATABASE_NAME)
    host = Variable.get(ConfigConst.MONGO_DB_HOST)

    @task()
    def load_users():
        pg_saver = PgSaver(dwh_pg_connect)
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_loader = CollectionLoader(mongo_connect)
        copier = CollectionCopier(collection_loader, pg_saver, log)

        copier.run_copy('users')

    @task()
    def load_restaurants():
        pg_saver = PgSaver(dwh_pg_connect)
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_loader = CollectionLoader(mongo_connect)
        copier = CollectionCopier(collection_loader, pg_saver, log)

        copier.run_copy('restaurants')

    @task()
    def load_orders():
        pg_saver = PgSaver(dwh_pg_connect)
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_loader = CollectionLoader(mongo_connect)
        copier = CollectionCopier(collection_loader, pg_saver, log)

        copier.run_copy('orders')

    user_loader = load_users()
    restaurant_loader = load_restaurants()
    order_loader = load_orders()

    user_loader  # type: ignore
    restaurant_loader  # type: ignore
    order_loader  # type: ignore


order_stg_dag = sprint5_case_stg_order_system()  # noqa
