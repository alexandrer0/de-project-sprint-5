from lib import PgConnect


class SchemaDdl:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_schema(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_restaurants(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    restaurant_id VARCHAR NOT NULL,
    restaurant_name TEXT NOT NULL,

    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL
);
CREATE INDEX IF NOT EXISTS IDX_dm_restaurants__restaurant_id_active_from ON dds.dm_restaurants (restaurant_id, active_from);

CREATE TABLE IF NOT EXISTS dds.dm_products (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    product_id varchar NOT NULL,
    product_name text NOT NULL,
    product_price numeric(19, 5) NOT NULL DEFAULT 0 CHECK (product_price >= 0),

    active_from timestamp NOT NULL,
    active_to timestamp NOT NULL,

    restaurant_id int NOT NULL REFERENCES dds.dm_restaurants(id)
);
CREATE INDEX IF NOT EXISTS IDX_dm_products__restaurant_id ON dds.dm_products (restaurant_id);

CREATE TABLE IF NOT EXISTS dds.dm_timestamps(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    ts timestamp NOT NULL UNiQUE,

    year int NOT NULL CHECK(year >= 2020 AND year < 2500),
    month int NOT NULL CHECK(month >= 0 AND month <= 12),
    day int NOT NULL CHECK(day >= 0 AND day <= 31),
    time time NOT NULL,
    date date NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_users(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    user_id varchar NOT NULL,
    user_name varchar NOT NULL,
    user_login varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_orders(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    order_key varchar NOT NULL UNIQUE,
    order_status varchar NOT NULL,

    restaurant_id int NOT NULL REFERENCES dds.dm_restaurants(id),
    timestamp_id int NOT NULL REFERENCES dds.dm_timestamps(id),
    user_id int NOT NULL REFERENCES dds.dm_users(id)
);
CREATE INDEX IF NOT EXISTS IDX_dm_orders__restaurant_id ON dds.dm_orders (restaurant_id);
CREATE INDEX IF NOT EXISTS IDX_dm_orders__timestamp_id ON dds.dm_orders (timestamp_id);
CREATE INDEX IF NOT EXISTS IDX_dm_orders__user_id ON dds.dm_orders (user_id);

CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    product_id int NOT NULL REFERENCES dds.dm_products(id),
    order_id int NOT NULL REFERENCES dds.dm_orders(id),
    count int NOT NULL DEFAULT 0 CHECK (count >= 0),
    price numeric(19, 5) NOT NULL DEFAULT 0 CHECK (price >= 0),
    total_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (total_sum >= 0),
    bonus_payment numeric(19, 5) NOT NULL DEFAULT 0 CHECK (bonus_payment >= 0),
    bonus_grant numeric(19, 5) NOT NULL DEFAULT 0 CHECK (bonus_grant >= 0)
);
CREATE INDEX IF NOT EXISTS IDX_fct_product_sales__product_id ON dds.fct_product_sales (product_id);
CREATE UNIQUE INDEX IF NOT EXISTS IDX_fct_product_sales__order_id_product_id ON dds.fct_product_sales (order_id, product_id);

CREATE TABLE IF NOT EXISTS dds.dm_couriers(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id varchar NOT NULL UNIQUE,
    courier_name varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveries(
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	delivery_id varchar UNIQUE
);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries(
    order_id integer PRIMARY key,
	delivery_id	integer,
	courier_id integer,
	order_ts timestamp,
    delivery_ts timestamp,
	address varchar,
	rate  integer,
	tip_sum numeric (14, 2),
	total_sum numeric (14, 2),
    CONSTRAINT fct_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT fct_deliveries_delivery_id_fkey FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id),
    CONSTRAINT fct_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id)
);

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id varchar NOT NULL,
    courier_name varchar NOT NULL,
    settlement_year int NOT NULL CHECK(settlement_year >= 2020 AND settlement_year < 2500),
    settlement_month int NOT NULL CHECK(settlement_month >= 0 AND settlement_month <= 12),
    orders_count int NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    rate_avg numeric(19, 5),
    order_processing_fee numeric(19, 5) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    courier_order_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
    courier_tips_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
    courier_reward_sum numeric(19, 5) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0)
);

DO $do$ BEGIN IF EXISTS (
    SELECT
    FROM pg_catalog.pg_roles
    WHERE rolname = 'sp5_de_tester'
) THEN
GRANT SELECT ON all tables IN SCHEMA dds TO sp5_de_tester;
END IF;
END $do$;
"""
                )
