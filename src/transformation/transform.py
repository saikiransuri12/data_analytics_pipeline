"""
Transformation module: cleans raw data, engineers features, and writes
enriched tables to the processed/ directory and back to the database.

Transformations:
- orders_enriched  : joins orders → customers → order_items aggregates
- order_items_enriched : joins items → products with margin calculations
- monthly_revenue  : monthly aggregated revenue + order counts
- customer_metrics : per-customer RFM (Recency, Frequency, Monetary) signals
"""

from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "pipeline.db"
PROCESSED_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"


def load_tables(engine) -> dict[str, pd.DataFrame]:
    tables = ["customers", "products", "orders", "order_items"]
    return {t: pd.read_sql_table(t, engine) for t in tables}


def transform_order_items(items: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    df = items.merge(products[["product_id", "name", "category", "cost_price"]], on="product_id", how="left")
    df["cost_total"] = (df["cost_price"] * df["quantity"]).round(2)
    df["gross_profit"] = (df["line_total"] - df["cost_total"]).round(2)
    df["margin_pct"] = ((df["gross_profit"] / df["line_total"].replace(0, np.nan)) * 100).round(2)
    return df


def transform_orders(orders: pd.DataFrame, customers: pd.DataFrame, items_enriched: pd.DataFrame) -> pd.DataFrame:
    order_agg = (
        items_enriched.groupby("order_id")
        .agg(
            order_revenue=("line_total", "sum"),
            order_cost=("cost_total", "sum"),
            order_profit=("gross_profit", "sum"),
            n_items=("quantity", "sum"),
            n_line_items=("item_id", "count"),
        )
        .reset_index()
    )
    order_agg["order_revenue"] = order_agg["order_revenue"].round(2)
    order_agg["order_cost"] = order_agg["order_cost"].round(2)
    order_agg["order_profit"] = order_agg["order_profit"].round(2)

    df = orders.merge(order_agg, on="order_id", how="left")
    df = df.merge(customers[["customer_id", "region", "acquisition_channel", "signup_date"]], on="customer_id", how="left")

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["signup_date"] = pd.to_datetime(df["signup_date"])
    df["year"] = df["order_date"].dt.year
    df["month"] = df["order_date"].dt.month
    df["year_month"] = df["order_date"].dt.to_period("M").astype(str)
    df["days_since_signup"] = (df["order_date"] - df["signup_date"]).dt.days
    df["is_completed"] = (df["status"] == "Completed").astype(int)
    return df


def build_monthly_revenue(orders_enriched: pd.DataFrame) -> pd.DataFrame:
    completed = orders_enriched[orders_enriched["status"] == "Completed"].copy()
    monthly = (
        completed.groupby("year_month")
        .agg(
            revenue=("order_revenue", "sum"),
            profit=("order_profit", "sum"),
            orders=("order_id", "count"),
            unique_customers=("customer_id", "nunique"),
            avg_order_value=("order_revenue", "mean"),
        )
        .reset_index()
    )
    monthly["revenue"] = monthly["revenue"].round(2)
    monthly["profit"] = monthly["profit"].round(2)
    monthly["avg_order_value"] = monthly["avg_order_value"].round(2)
    monthly["profit_margin_pct"] = ((monthly["profit"] / monthly["revenue"]) * 100).round(2)
    monthly = monthly.sort_values("year_month")
    monthly["revenue_mom_pct"] = monthly["revenue"].pct_change().mul(100).round(2)
    return monthly


def build_customer_metrics(orders_enriched: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    completed = orders_enriched[orders_enriched["status"] == "Completed"].copy()
    snapshot_date = completed["order_date"].max()

    rfm = (
        completed.groupby("customer_id")
        .agg(
            last_order_date=("order_date", "max"),
            frequency=("order_id", "count"),
            monetary=("order_revenue", "sum"),
        )
        .reset_index()
    )
    rfm["recency_days"] = (snapshot_date - rfm["last_order_date"]).dt.days
    rfm["monetary"] = rfm["monetary"].round(2)

    # RFM scoring (1–5 each)
    rfm["r_score"] = pd.qcut(rfm["recency_days"], q=5, labels=[5, 4, 3, 2, 1]).astype(int)
    rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]).astype(int)
    rfm["m_score"] = pd.qcut(rfm["monetary"], q=5, labels=[1, 2, 3, 4, 5]).astype(int)
    rfm["rfm_score"] = rfm["r_score"] + rfm["f_score"] + rfm["m_score"]

    def segment(row):
        if row["rfm_score"] >= 12:
            return "Champions"
        elif row["rfm_score"] >= 9:
            return "Loyal"
        elif row["rfm_score"] >= 6:
            return "Potential"
        elif row["r_score"] <= 2:
            return "At-Risk"
        else:
            return "Dormant"

    rfm["segment"] = rfm.apply(segment, axis=1)

    df = rfm.merge(customers[["customer_id", "name", "region", "acquisition_channel", "age", "gender"]], on="customer_id", how="left")
    return df


def build_product_performance(items_enriched: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    completed_orders = orders[orders["status"] == "Completed"]["order_id"]
    items = items_enriched[items_enriched["order_id"].isin(completed_orders)].copy()

    perf = (
        items.groupby(["product_id", "name", "category"])
        .agg(
            units_sold=("quantity", "sum"),
            revenue=("line_total", "sum"),
            profit=("gross_profit", "sum"),
            orders=("order_id", "nunique"),
        )
        .reset_index()
    )
    perf["revenue"] = perf["revenue"].round(2)
    perf["profit"] = perf["profit"].round(2)
    perf["margin_pct"] = ((perf["profit"] / perf["revenue"]) * 100).round(2)
    perf["avg_order_qty"] = (perf["units_sold"] / perf["orders"]).round(2)
    return perf.sort_values("revenue", ascending=False)


def run(engine=None) -> dict[str, pd.DataFrame]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if engine is None:
        engine = create_engine(f"sqlite:///{DB_PATH}")

    print("Loading raw tables...")
    tables = load_tables(engine)

    print("Transforming order items...")
    items_enriched = transform_order_items(tables["order_items"], tables["products"])

    print("Transforming orders...")
    orders_enriched = transform_orders(tables["orders"], tables["customers"], items_enriched)

    print("Building monthly revenue...")
    monthly_revenue = build_monthly_revenue(orders_enriched)

    print("Building customer metrics (RFM)...")
    customer_metrics = build_customer_metrics(orders_enriched, tables["customers"])

    print("Building product performance...")
    product_performance = build_product_performance(items_enriched, tables["orders"])

    results = {
        "orders_enriched": orders_enriched,
        "items_enriched": items_enriched,
        "monthly_revenue": monthly_revenue,
        "customer_metrics": customer_metrics,
        "product_performance": product_performance,
    }

    for name, df in results.items():
        path = PROCESSED_DIR / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"  Saved {len(df):,} rows → {path.name}")

        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
        df.to_sql(name, engine, if_exists="replace", index=False, chunksize=5000)

    return results


if __name__ == "__main__":
    run()
