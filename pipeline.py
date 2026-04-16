"""
Main pipeline orchestrator.

Stages:
  1. ingest   — generate synthetic data + load into SQLite
  2. transform — clean, enrich, build aggregated tables
  3. analyse  — run SQL analytics queries
  4. visualise — generate charts + HTML report

Usage:
  python pipeline.py             # run all stages
  python pipeline.py --stage ingest
  python pipeline.py --stage transform
  python pipeline.py --stage analyse
  python pipeline.py --stage visualise
"""

import argparse
import time
from pathlib import Path

from sqlalchemy import create_engine

DB_PATH = Path(__file__).parent / "data" / "pipeline.db"


def stage_ingest():
    from src.ingestion.generate_data import run as generate
    from src.ingestion.load_to_db import run as load

    print("\n[1/4] INGESTION")
    print("  Generating synthetic data...")
    datasets = generate()
    print("  Loading into SQLite...")
    load(datasets)


def stage_transform():
    from src.transformation.transform import run as transform

    print("\n[2/4] TRANSFORMATION")
    engine = create_engine(f"sqlite:///{DB_PATH}")
    return transform(engine)


def stage_analyse(processed_data=None):
    from src.analytics.queries import run as query

    print("\n[3/4] ANALYTICS")
    engine = create_engine(f"sqlite:///{DB_PATH}")
    return query(engine)


def stage_visualise(analytics_results, processed_data):
    from src.visualization.charts import run as visualise

    print("\n[4/4] VISUALISATION")
    visualise(analytics_results, processed_data)


def main(stage: str | None = None):
    start = time.time()

    if stage in (None, "ingest"):
        stage_ingest()
        if stage:
            return

    if stage in (None, "transform"):
        processed_data = stage_transform()
        if stage:
            return
    else:
        # Load processed data from CSVs if skipping earlier stages
        import pandas as pd
        processed_dir = Path(__file__).parent / "data" / "processed"
        processed_data = {
            "monthly_revenue": pd.read_csv(processed_dir / "monthly_revenue.csv"),
            "customer_metrics": pd.read_csv(processed_dir / "customer_metrics.csv"),
            "product_performance": pd.read_csv(processed_dir / "product_performance.csv"),
        }

    if stage in (None, "analyse"):
        analytics_results = stage_analyse(processed_data)
        if stage:
            return
    else:
        analytics_results = {}

    if stage in (None, "visualise"):
        stage_visualise(analytics_results, processed_data)

    elapsed = time.time() - start
    print(f"\nPipeline complete in {elapsed:.1f}s")
    print("Open reports/index.html in a browser to view the dashboard.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Data Analytics Pipeline")
    parser.add_argument(
        "--stage",
        choices=["ingest", "transform", "analyse", "visualise"],
        default=None,
        help="Run a single pipeline stage (default: run all)",
    )
    args = parser.parse_args()
    main(args.stage)
