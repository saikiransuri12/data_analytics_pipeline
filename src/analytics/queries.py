"""
Analytics module: runs SQL queries against the processed tables to produce
insight DataFrames used by the visualization layer.
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "pipeline.db"


QUERIES = {
    "top_products_by_revenue": """
        SELECT
            p.name        AS product,
            p.category,
            SUM(oi.line_total)  AS revenue,
            SUM(oi.quantity)    AS units_sold,
            COUNT(DISTINCT oi.order_id) AS orders
        FROM order_items oi
        JOIN orders o  ON oi.order_id  = o.order_id
        JOIN products p ON oi.product_id = p.product_id
        WHERE o.status = 'Completed'
        GROUP BY p.product_id, p.name, p.category
        ORDER BY revenue DESC
        LIMIT 15
    """,

    "revenue_by_category": """
        SELECT
            p.category,
            ROUND(SUM(oi.line_total), 2)                        AS revenue,
            ROUND(SUM(oi.line_total - oi.quantity * p.cost_price), 2) AS profit,
            SUM(oi.quantity)                                     AS units_sold,
            COUNT(DISTINCT oi.order_id)                          AS orders
        FROM order_items oi
        JOIN orders   o  ON oi.order_id  = o.order_id
        JOIN products p  ON oi.product_id = p.product_id
        WHERE o.status = 'Completed'
        GROUP BY p.category
        ORDER BY revenue DESC
    """,

    "revenue_by_region": """
        SELECT
            c.region,
            ROUND(SUM(oi.line_total), 2)   AS revenue,
            COUNT(DISTINCT o.order_id)     AS orders,
            COUNT(DISTINCT o.customer_id)  AS customers
        FROM orders o
        JOIN customers  c  ON o.customer_id  = c.customer_id
        JOIN order_items oi ON o.order_id     = oi.order_id
        WHERE o.status = 'Completed'
        GROUP BY c.region
        ORDER BY revenue DESC
    """,

    "customer_segment_distribution": """
        SELECT
            segment,
            COUNT(*)                    AS customers,
            ROUND(AVG(monetary), 2)     AS avg_spend,
            ROUND(AVG(frequency), 2)    AS avg_orders,
            ROUND(AVG(recency_days), 2) AS avg_recency_days
        FROM customer_metrics
        GROUP BY segment
        ORDER BY avg_spend DESC
    """,

    "acquisition_channel_performance": """
        SELECT
            c.acquisition_channel,
            COUNT(DISTINCT c.customer_id)       AS customers,
            COUNT(DISTINCT o.order_id)          AS orders,
            ROUND(SUM(oi.line_total), 2)        AS revenue,
            ROUND(SUM(oi.line_total) / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS revenue_per_customer
        FROM customers c
        LEFT JOIN orders o       ON c.customer_id = o.customer_id AND o.status = 'Completed'
        LEFT JOIN order_items oi ON o.order_id    = oi.order_id
        GROUP BY c.acquisition_channel
        ORDER BY revenue DESC
    """,

    "payment_method_share": """
        SELECT
            payment_method,
            COUNT(*)                            AS orders,
            ROUND(SUM(oi.line_total), 2)        AS revenue
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.status = 'Completed'
        GROUP BY payment_method
        ORDER BY orders DESC
    """,

    "monthly_cohort_retention": """
        SELECT
            strftime('%Y-%m', c.signup_date)    AS cohort_month,
            strftime('%Y-%m', o.order_date)     AS order_month,
            COUNT(DISTINCT o.customer_id)       AS customers
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE o.status = 'Completed'
          AND strftime('%Y-%m', c.signup_date) >= strftime('%Y-%m', date('now', '-18 months'))
        GROUP BY cohort_month, order_month
        ORDER BY cohort_month, order_month
    """,

    "order_status_breakdown": """
        SELECT
            status,
            COUNT(*)                     AS orders,
            ROUND(SUM(oi.line_total), 2) AS gross_value
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY status
        ORDER BY orders DESC
    """,
}


def run(engine=None) -> dict[str, pd.DataFrame]:
    if engine is None:
        engine = create_engine(f"sqlite:///{DB_PATH}")

    results = {}
    for name, sql in QUERIES.items():
        df = pd.read_sql(text(sql), engine)
        results[name] = df
        print(f"  Query '{name}': {len(df)} rows")

    return results


if __name__ == "__main__":
    results = run()
    for name, df in results.items():
        print(f"\n=== {name} ===")
        print(df.head())
