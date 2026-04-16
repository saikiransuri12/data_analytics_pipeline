"""
Data generation module: creates synthetic e-commerce data and saves to CSV.
Produces four tables: customers, products, orders, order_items.
"""

import random
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

fake = Faker()
fake.seed_instance(SEED)

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

CATEGORIES = {
    "Electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", "Smartwatch", "Monitor", "Keyboard", "Mouse"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Hoodie", "Shorts", "Socks"],
    "Home & Kitchen": ["Coffee Maker", "Blender", "Air Fryer", "Toaster", "Vacuum Cleaner", "Bed Sheets", "Pillow", "Cookware Set"],
    "Books": ["Fiction Novel", "Self-Help Book", "Technical Manual", "Biography", "Cookbook", "Science Book", "Art Book", "Travel Guide"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Running Shoes", "Water Bottle", "Gym Bag", "Resistance Bands", "Jump Rope", "Protein Powder"],
}

PRICE_RANGES = {
    "Electronics": (49.99, 1299.99),
    "Clothing": (9.99, 149.99),
    "Home & Kitchen": (19.99, 349.99),
    "Books": (7.99, 49.99),
    "Sports": (9.99, 199.99),
}

REGIONS = ["North", "South", "East", "West", "Central"]
CHANNELS = ["Organic Search", "Paid Search", "Social Media", "Email", "Direct", "Referral"]
SEGMENTS = ["New", "Returning", "VIP", "At-Risk"]


def generate_customers(n: int = 2000) -> pd.DataFrame:
    records = []
    for i in range(1, n + 1):
        signup = fake.date_between(start_date="-3y", end_date="-1d")
        records.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.unique.email(),
            "age": random.randint(18, 70),
            "gender": random.choice(["M", "F", "Other"]),
            "region": random.choice(REGIONS),
            "acquisition_channel": random.choice(CHANNELS),
            "signup_date": signup,
        })
    return pd.DataFrame(records)


def generate_products(n_per_category: int = 8) -> pd.DataFrame:
    records = []
    product_id = 1
    for category, items in CATEGORIES.items():
        low, high = PRICE_RANGES[category]
        for item in items[:n_per_category]:
            records.append({
                "product_id": product_id,
                "name": item,
                "category": category,
                "unit_price": round(random.uniform(low, high), 2),
                "cost_price": None,  # filled below
            })
            product_id += 1
    df = pd.DataFrame(records)
    # cost is 40–70% of unit price
    margin_factor = np.random.uniform(0.40, 0.70, len(df))
    df["cost_price"] = (df["unit_price"] * margin_factor).round(2)
    return df


def generate_orders(customers: pd.DataFrame, n: int = 15000) -> pd.DataFrame:
    customer_ids = customers["customer_id"].tolist()
    # skew towards repeat buyers — some customers order much more
    weights = np.random.power(0.3, len(customer_ids))
    weights /= weights.sum()

    records = []
    for order_id in range(1, n + 1):
        cust_id = np.random.choice(customer_ids, p=weights)
        signup = customers.loc[customers["customer_id"] == cust_id, "signup_date"].values[0]
        order_date = fake.date_between(start_date=pd.Timestamp(signup), end_date="today")
        status = np.random.choice(
            ["Completed", "Completed", "Completed", "Returned", "Cancelled"],
            p=[0.70, 0.10, 0.10, 0.06, 0.04],
        )
        records.append({
            "order_id": order_id,
            "customer_id": int(cust_id),
            "order_date": order_date,
            "status": status,
            "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Bank Transfer"]),
            "discount_pct": random.choice([0, 0, 0, 5, 10, 15, 20]),
        })
    return pd.DataFrame(records)


def generate_order_items(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    product_ids = products["product_id"].tolist()
    records = []
    item_id = 1
    for _, order in orders.iterrows():
        n_items = np.random.choice([1, 2, 3, 4, 5], p=[0.50, 0.25, 0.13, 0.08, 0.04])
        chosen = random.sample(product_ids, min(n_items, len(product_ids)))
        for pid in chosen:
            price = float(products.loc[products["product_id"] == pid, "unit_price"].values[0])
            qty = random.randint(1, 4)
            discount = order["discount_pct"] / 100
            records.append({
                "item_id": item_id,
                "order_id": int(order["order_id"]),
                "product_id": int(pid),
                "quantity": qty,
                "unit_price": price,
                "discount_pct": int(order["discount_pct"]),
                "line_total": round(price * qty * (1 - discount), 2),
            })
            item_id += 1
    return pd.DataFrame(records)


def run() -> dict[str, pd.DataFrame]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print("Generating customers...")
    customers = generate_customers(2000)

    print("Generating products...")
    products = generate_products()

    print("Generating orders...")
    orders = generate_orders(customers, 15000)

    print("Generating order items...")
    order_items = generate_order_items(orders, products)

    datasets = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    for name, df in datasets.items():
        path = RAW_DIR / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"  Saved {len(df):,} rows → {path.name}")

    return datasets


if __name__ == "__main__":
    run()
