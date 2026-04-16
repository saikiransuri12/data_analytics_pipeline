"""
Ingestion module: loads raw CSVs into a SQLite database.
Creates tables with proper types and indexes for query performance.
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
DB_PATH = Path(__file__).resolve().parents[2] / "data" / "pipeline.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id     INTEGER PRIMARY KEY,
    name            TEXT    NOT NULL,
    email           TEXT    UNIQUE NOT NULL,
    age             INTEGER,
    gender          TEXT,
    region          TEXT,
    acquisition_channel TEXT,
    signup_date     DATE
);

CREATE TABLE IF NOT EXISTS products (
    product_id  INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    category    TEXT    NOT NULL,
    unit_price  REAL    NOT NULL,
    cost_price  REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        INTEGER PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date      DATE    NOT NULL,
    status          TEXT    NOT NULL,
    payment_method  TEXT,
    discount_pct    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id      INTEGER PRIMARY KEY,
    order_id     INTEGER NOT NULL REFERENCES orders(order_id),
    product_id   INTEGER NOT NULL REFERENCES products(product_id),
    quantity     INTEGER NOT NULL,
    unit_price   REAL    NOT NULL,
    discount_pct INTEGER DEFAULT 0,
    line_total   REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_customer  ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date      ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_items_order      ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_items_product    ON order_items(product_id);
"""


def run(datasets: dict | None = None) -> None:
    engine = create_engine(f"sqlite:///{DB_PATH}")

    with engine.begin() as conn:
        for stmt in SCHEMA_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))

    table_order = ["customers", "products", "orders", "order_items"]

    for table in table_order:
        if datasets and table in datasets:
            df = datasets[table]
        else:
            path = RAW_DIR / f"{table}.csv"
            df = pd.read_csv(path)

        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table}"))

        df.to_sql(table, engine, if_exists="append", index=False, chunksize=5000)
        print(f"  Loaded {len(df):,} rows → {table}")

    print(f"Database ready: {DB_PATH}")


if __name__ == "__main__":
    run()
