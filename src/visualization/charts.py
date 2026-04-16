"""
Visualization module: generates all charts and saves them to reports/.
Produces a self-contained HTML summary report at the end.
"""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for script execution

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

REPORTS_DIR = Path(__file__).resolve().parents[2] / "reports"
PROCESSED_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"

PALETTE = "steelblue"
ACCENT = "#E04B3A"
BG = "#F8F9FA"

sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams.update({"figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG})


def _save(fig: plt.Figure, name: str) -> Path:
    path = REPORTS_DIR / f"{name}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved → {path.name}")
    return path


# ---------------------------------------------------------------------------
# 1. Monthly Revenue & Profit Trend
# ---------------------------------------------------------------------------
def plot_monthly_revenue(monthly_revenue: pd.DataFrame) -> Path:
    df = monthly_revenue.copy()
    df["period"] = pd.PeriodIndex(df["year_month"], freq="M").to_timestamp()
    df = df.sort_values("period").tail(24)  # last 24 months

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.fill_between(df["period"], df["revenue"], alpha=0.15, color="steelblue")
    ax.plot(df["period"], df["revenue"], marker="o", markersize=4, linewidth=2, color="steelblue", label="Revenue")
    ax.plot(df["period"], df["profit"], marker="s", markersize=4, linewidth=2, color=ACCENT, linestyle="--", label="Profit")

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_title("Monthly Revenue & Profit Trend", fontsize=15, fontweight="bold", pad=12)
    ax.set_xlabel("")
    ax.set_ylabel("Amount (USD)")
    ax.legend()
    fig.autofmt_xdate()
    return _save(fig, "01_monthly_revenue_trend")


# ---------------------------------------------------------------------------
# 2. Top 10 Products by Revenue
# ---------------------------------------------------------------------------
def plot_top_products(top_products: pd.DataFrame) -> Path:
    df = top_products.head(10).sort_values("revenue")
    colors = [ACCENT if c == "Electronics" else PALETTE for c in df["category"]]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(df["product"], df["revenue"], color=colors, edgecolor="white", height=0.6)

    for bar, rev in zip(bars, df["revenue"]):
        ax.text(bar.get_width() + rev * 0.01, bar.get_y() + bar.get_height() / 2,
                f"${rev:,.0f}", va="center", fontsize=9)

    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_title("Top 10 Products by Revenue", fontsize=15, fontweight="bold", pad=12)
    ax.set_xlabel("Revenue (USD)")
    return _save(fig, "02_top_products_revenue")


# ---------------------------------------------------------------------------
# 3. Revenue & Margin by Category
# ---------------------------------------------------------------------------
def plot_category_performance(revenue_by_category: pd.DataFrame) -> Path:
    df = revenue_by_category.sort_values("revenue", ascending=False)
    df["margin_pct"] = (df["profit"] / df["revenue"] * 100).round(1)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Revenue bars
    sns.barplot(data=df, x="category", y="revenue", hue="category", legend=False, ax=axes[0], palette="Blues_d")
    axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    axes[0].set_title("Revenue by Category", fontweight="bold")
    axes[0].set_xlabel("")
    axes[0].tick_params(axis="x", rotation=25)

    # Margin bars
    sns.barplot(data=df, x="category", y="margin_pct", hue="category", legend=False, ax=axes[1], palette="Greens_d")
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    axes[1].set_title("Profit Margin % by Category", fontweight="bold")
    axes[1].set_xlabel("")
    axes[1].tick_params(axis="x", rotation=25)

    fig.suptitle("Category Performance", fontsize=15, fontweight="bold", y=1.01)
    fig.tight_layout()
    return _save(fig, "03_category_performance")


# ---------------------------------------------------------------------------
# 4. Customer Segment Distribution (RFM)
# ---------------------------------------------------------------------------
def plot_rfm_segments(segment_dist: pd.DataFrame) -> Path:
    df = segment_dist.sort_values("customers", ascending=False)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Pie
    wedge_props = {"edgecolor": "white", "linewidth": 2}
    axes[0].pie(
        df["customers"], labels=df["segment"], autopct="%1.1f%%",
        colors=sns.color_palette("Set2", len(df)), wedgeprops=wedge_props,
        startangle=140,
    )
    axes[0].set_title("Customer Segments (% of Base)", fontweight="bold")

    # Avg spend bar
    sns.barplot(data=df, x="segment", y="avg_spend", hue="segment", legend=False, ax=axes[1], palette="Set2")
    axes[1].yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    axes[1].set_title("Average Lifetime Spend by Segment", fontweight="bold")
    axes[1].set_xlabel("")

    fig.suptitle("RFM Customer Segmentation", fontsize=15, fontweight="bold", y=1.01)
    fig.tight_layout()
    return _save(fig, "04_rfm_customer_segments")


# ---------------------------------------------------------------------------
# 5. Revenue by Region
# ---------------------------------------------------------------------------
def plot_revenue_by_region(revenue_by_region: pd.DataFrame) -> Path:
    df = revenue_by_region.sort_values("revenue", ascending=False)

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = sns.barplot(data=df, x="region", y="revenue", hue="region", legend=False, palette="coolwarm", ax=ax)

    for bar in ax.patches:
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + df["revenue"].max() * 0.01,
            f"${bar.get_height():,.0f}",
            ha="center", va="bottom", fontsize=9,
        )

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_title("Revenue by Region", fontsize=15, fontweight="bold", pad=12)
    ax.set_xlabel("")
    return _save(fig, "05_revenue_by_region")


# ---------------------------------------------------------------------------
# 6. Acquisition Channel Performance
# ---------------------------------------------------------------------------
def plot_acquisition_channels(channel_perf: pd.DataFrame) -> Path:
    df = channel_perf.sort_values("revenue", ascending=False)

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(df))
    w = 0.35

    ax.bar(x - w / 2, df["customers"], width=w, label="Customers", color="steelblue", alpha=0.85)

    ax2 = ax.twinx()
    ax2.bar(x + w / 2, df["revenue"], width=w, label="Revenue", color=ACCENT, alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels(df["acquisition_channel"], rotation=20, ha="right")
    ax.set_ylabel("Customers")
    ax2.set_ylabel("Revenue (USD)")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

    ax.set_title("Acquisition Channel: Customers vs Revenue", fontsize=14, fontweight="bold", pad=12)
    fig.tight_layout()
    return _save(fig, "06_acquisition_channels")


# ---------------------------------------------------------------------------
# 7. Monthly Average Order Value
# ---------------------------------------------------------------------------
def plot_aov_trend(monthly_revenue: pd.DataFrame) -> Path:
    df = monthly_revenue.copy()
    df["period"] = pd.PeriodIndex(df["year_month"], freq="M").to_timestamp()
    df = df.sort_values("period").tail(24)

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(df["period"], df["avg_order_value"], marker="D", markersize=5, linewidth=2, color="#2ca02c")
    ax.fill_between(df["period"], df["avg_order_value"], alpha=0.1, color="#2ca02c")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_title("Average Order Value (AOV) Trend", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("")
    ax.set_ylabel("AOV (USD)")
    fig.autofmt_xdate()
    return _save(fig, "07_aov_trend")


# ---------------------------------------------------------------------------
# 8. Cohort Retention Heatmap
# ---------------------------------------------------------------------------
def plot_cohort_retention(cohort_df: pd.DataFrame) -> Path:
    pivot = cohort_df.pivot_table(index="cohort_month", columns="order_month", values="customers", aggfunc="sum")
    # Normalize each row by its first-month value
    pivot_pct = pivot.div(pivot.iloc[:, 0], axis=0).mul(100).round(1)

    # Keep only first 6 months after acquisition for readability
    pivot_pct = pivot_pct.iloc[:, :7]

    fig, ax = plt.subplots(figsize=(13, 7))
    sns.heatmap(
        pivot_pct, annot=True, fmt=".0f", cmap="YlOrRd_r",
        linewidths=0.4, ax=ax, cbar_kws={"label": "Retention %"},
    )
    ax.set_title("Cohort Retention Heatmap (%)", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Order Month")
    ax.set_ylabel("Cohort (Signup Month)")
    fig.tight_layout()
    return _save(fig, "08_cohort_retention")


# ---------------------------------------------------------------------------
# 9. Order Status Breakdown
# ---------------------------------------------------------------------------
def plot_order_status(order_status: pd.DataFrame) -> Path:
    df = order_status.sort_values("orders", ascending=False)

    fig, ax = plt.subplots(figsize=(7, 4))
    colors = {"Completed": "#2ca02c", "Returned": "#ff7f0e", "Cancelled": "#d62728"}
    bar_colors = [colors.get(s, "steelblue") for s in df["status"]]

    ax.bar(df["status"], df["orders"], color=bar_colors, edgecolor="white", width=0.5)
    for i, (orders, val) in enumerate(zip(df["orders"], df["orders"])):
        ax.text(i, orders + df["orders"].max() * 0.01, f"{orders:,}", ha="center", fontsize=10)

    ax.set_title("Order Status Breakdown", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("Number of Orders")
    ax.set_xlabel("")
    return _save(fig, "09_order_status")


# ---------------------------------------------------------------------------
# HTML Report
# ---------------------------------------------------------------------------
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>E-Commerce Analytics Dashboard</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #f0f2f5; color: #333; }}
  header {{ background: linear-gradient(135deg, #1a1a2e, #16213e); color: white; padding: 2rem 3rem; }}
  header h1 {{ font-size: 2rem; font-weight: 700; }}
  header p {{ opacity: 0.75; margin-top: 0.4rem; }}
  .kpi-bar {{ display: flex; gap: 1.5rem; padding: 1.5rem 3rem; flex-wrap: wrap; background: #fff; box-shadow: 0 2px 6px rgba(0,0,0,.07); }}
  .kpi {{ flex: 1; min-width: 160px; background: #f8f9fa; border-radius: 10px; padding: 1rem 1.5rem; border-left: 4px solid #1a6fc4; }}
  .kpi .label {{ font-size: 0.8rem; text-transform: uppercase; letter-spacing: .05em; color: #888; }}
  .kpi .value {{ font-size: 1.7rem; font-weight: 700; color: #1a1a2e; margin-top: 4px; }}
  .section {{ padding: 1.5rem 3rem; }}
  .section h2 {{ font-size: 1.2rem; font-weight: 600; color: #1a1a2e; margin-bottom: 1rem; border-bottom: 2px solid #e0e4ea; padding-bottom: 0.5rem; }}
  .grid {{ display: grid; gap: 1.5rem; }}
  .grid-1 {{ grid-template-columns: 1fr; }}
  .grid-2 {{ grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); }}
  .card {{ background: white; border-radius: 12px; padding: 1.2rem; box-shadow: 0 2px 10px rgba(0,0,0,.06); }}
  .card img {{ width: 100%; border-radius: 6px; }}
  .card .caption {{ font-size: 0.82rem; color: #888; margin-top: 0.6rem; text-align: center; }}
  footer {{ text-align: center; padding: 2rem; color: #999; font-size: 0.85rem; background: #fff; margin-top: 2rem; }}
</style>
</head>
<body>
<header>
  <h1>E-Commerce Analytics Pipeline</h1>
  <p>End-to-end data pipeline · Python · Pandas · SQL · Matplotlib/Seaborn</p>
</header>

<div class="kpi-bar">
  {kpis}
</div>

<div class="section">
  <h2>Revenue Performance</h2>
  <div class="grid grid-1">
    <div class="card"><img src="01_monthly_revenue_trend.png"><p class="caption">Monthly revenue and profit over the last 24 months</p></div>
    <div class="card"><img src="07_aov_trend.png"><p class="caption">Average order value trend</p></div>
  </div>
</div>

<div class="section">
  <h2>Product & Category Insights</h2>
  <div class="grid grid-2">
    <div class="card"><img src="02_top_products_revenue.png"><p class="caption">Top 10 products by revenue</p></div>
    <div class="card"><img src="03_category_performance.png"><p class="caption">Revenue and margin % by product category</p></div>
  </div>
</div>

<div class="section">
  <h2>Customer Analytics</h2>
  <div class="grid grid-2">
    <div class="card"><img src="04_rfm_customer_segments.png"><p class="caption">RFM-based customer segmentation</p></div>
    <div class="card"><img src="08_cohort_retention.png"><p class="caption">Monthly cohort retention heatmap</p></div>
  </div>
</div>

<div class="section">
  <h2>Geography & Channels</h2>
  <div class="grid grid-2">
    <div class="card"><img src="05_revenue_by_region.png"><p class="caption">Revenue distribution across regions</p></div>
    <div class="card"><img src="06_acquisition_channels.png"><p class="caption">Customer acquisition channel performance</p></div>
  </div>
</div>

<div class="section">
  <h2>Operations</h2>
  <div class="grid grid-1">
    <div class="card" style="max-width:600px;margin:auto"><img src="09_order_status.png"><p class="caption">Order fulfillment status breakdown</p></div>
  </div>
</div>

<footer>
  Generated by the Data Analytics Pipeline &nbsp;·&nbsp; Python · Pandas · SQLite · Matplotlib · Seaborn
</footer>
</body>
</html>
"""


def build_kpis(monthly_revenue: pd.DataFrame, customer_metrics: pd.DataFrame) -> str:
    total_rev = monthly_revenue["revenue"].sum()
    total_profit = monthly_revenue["profit"].sum()
    total_orders = monthly_revenue["orders"].sum()
    total_customers = len(customer_metrics)
    avg_aov = monthly_revenue["avg_order_value"].mean()
    margin = total_profit / total_rev * 100 if total_rev else 0

    kpis = [
        ("Total Revenue", f"${total_rev:,.0f}"),
        ("Total Profit", f"${total_profit:,.0f}"),
        ("Gross Margin", f"{margin:.1f}%"),
        ("Total Orders", f"{total_orders:,}"),
        ("Unique Customers", f"{total_customers:,}"),
        ("Avg Order Value", f"${avg_aov:,.2f}"),
    ]
    return "\n  ".join(
        f'<div class="kpi"><div class="label">{label}</div><div class="value">{value}</div></div>'
        for label, value in kpis
    )


def run(analytics_results: dict, processed_data: dict) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    monthly_revenue = processed_data["monthly_revenue"]
    customer_metrics = processed_data["customer_metrics"]

    print("Generating charts...")
    plot_monthly_revenue(monthly_revenue)
    plot_top_products(analytics_results["top_products_by_revenue"])
    plot_category_performance(analytics_results["revenue_by_category"])
    plot_rfm_segments(analytics_results["customer_segment_distribution"])
    plot_revenue_by_region(analytics_results["revenue_by_region"])
    plot_acquisition_channels(analytics_results["acquisition_channel_performance"])
    plot_aov_trend(monthly_revenue)
    plot_cohort_retention(analytics_results["monthly_cohort_retention"])
    plot_order_status(analytics_results["order_status_breakdown"])

    kpis_html = build_kpis(monthly_revenue, customer_metrics)
    html = HTML_TEMPLATE.format(kpis=kpis_html)

    report_path = REPORTS_DIR / "index.html"
    report_path.write_text(html, encoding="utf-8")
    print(f"  HTML report → {report_path}")


if __name__ == "__main__":
    from src.analytics.queries import run as run_queries
    monthly = pd.read_csv(PROCESSED_DIR / "monthly_revenue.csv")
    customer_metrics = pd.read_csv(PROCESSED_DIR / "customer_metrics.csv")
    analytics = run_queries()
    run(analytics, {"monthly_revenue": monthly, "customer_metrics": customer_metrics})
