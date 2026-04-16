# E-Commerce Data Analytics Pipeline

An end-to-end data analytics pipeline built with Python, Pandas, SQL (SQLite), and Matplotlib/Seaborn. Designed as a portfolio project demonstrating the full data engineering and analytics workflow.

## Pipeline Architecture

```
generate_data.py  →  load_to_db.py  →  transform.py  →  queries.py  →  charts.py
   (Ingestion)        (SQLite)        (Transform)      (Analytics)    (Visualise)
```

| Stage | What it does |
|---|---|
| **Ingestion** | Generates 2,000 customers, 40 products, 15,000 orders, ~28,500 line items using Faker |
| **Load** | Persists raw CSVs into a normalised SQLite schema with indexes |
| **Transform** | Enriches orders with revenue/margin, engineers RFM signals, builds monthly aggregates |
| **Analytics** | Runs 8 SQL queries: category performance, regional breakdown, cohort retention, etc. |
| **Visualise** | Produces 9 PNG charts + a self-contained HTML dashboard |

## Data Model

```
customers ──< orders ──< order_items >── products
```

- **customers** — demographics, region, acquisition channel, signup date
- **products** — category, unit price, cost price
- **orders** — status (Completed / Returned / Cancelled), payment method, discount
- **order_items** — quantity, line total

## Reports Generated

| Chart | Insight |
|---|---|
| Monthly Revenue & Profit Trend | 24-month revenue/profit trajectory |
| Top 10 Products by Revenue | Best-performing SKUs |
| Category Performance | Revenue + margin % per category |
| RFM Customer Segmentation | Champions, Loyal, Potential, At-Risk, Dormant |
| Cohort Retention Heatmap | Customer retention by signup cohort |
| Revenue by Region | Geographic revenue breakdown |
| Acquisition Channel Performance | Customers + revenue per channel |
| Average Order Value Trend | AOV over time |
| Order Status Breakdown | Completed / Returned / Cancelled split |

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python pipeline.py

# Or run individual stages
python pipeline.py --stage ingest
python pipeline.py --stage transform
python pipeline.py --stage analyse
python pipeline.py --stage visualise
```

Open `reports/index.html` in a browser to view the interactive dashboard.

## Tech Stack

- **Python 3.10+**
- **Pandas** — data wrangling and transformation
- **SQLAlchemy + SQLite** — storage and SQL analytics
- **Matplotlib + Seaborn** — charting
- **Faker** — synthetic data generation

## Project Structure

```
data_analytics_pipeline/
├── data/
│   ├── raw/               # Generated CSVs (customers, products, orders, order_items)
│   ├── processed/         # Enriched/aggregated tables
│   └── pipeline.db        # SQLite database
├── reports/               # PNG charts + index.html dashboard
├── src/
│   ├── ingestion/         # Data generation + DB loading
│   ├── transformation/    # Cleaning, enrichment, RFM
│   ├── analytics/         # SQL query definitions
│   └── visualization/     # Chart generation + HTML report
├── pipeline.py            # Main orchestrator
└── requirements.txt
```
