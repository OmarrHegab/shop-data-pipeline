from datetime import timezone
from decimal import Decimal
import logging
import pandas as pd
import streamlit as st
import db
from metrics import totals, rollup

st.set_page_config(page_title="Shop | Sales overview", page_icon="🛍️", layout="wide")
st.caption("SHOP ANALYTICS / SALES OVERVIEW")
st.title("Every order. A clearer picture.")
st.write("Explore sales performance across days, countries, and products.")
st.sidebar.title("Shop analytics")
st.sidebar.button("Refresh warehouse data", type="primary")
st.sidebar.caption("Refresh after an Airflow run finishes to see the newly loaded results.")

try:
    snapshot = db.read_snapshot()
except Exception:
    logging.exception("Warehouse read failed")
    st.error("Could not read the sales tables. Check that PostgreSQL is healthy and the Airflow load_postgres task has succeeded, then click Refresh warehouse data.")
    st.stop()

daily = snapshot["daily_sales"]
if not daily or not snapshot["state"]:
    st.info("No published sales yet. Run the pipeline in Airflow, then refresh this page.")
    st.stop()

first = min(row["event_date"] for row in daily)
last = max(row["event_date"] for row in daily)
selected = st.sidebar.date_input("Order dates (UTC)", value=(first, last), min_value=first, max_value=last)
if len(selected) != 2:
    st.info("Choose an end date to complete the date range.")
    st.stop()
start, end = selected
filtered = {name: [row for row in snapshot[name] if start <= row["event_date"] <= end]
            for name in ("daily_sales", "country_sales", "product_sales")}
if not filtered["daily_sales"]:
    st.info("No orders in this date range. Choose another range in the sidebar.")
    st.stop()

orders, units, revenue, average = totals(filtered["daily_sales"])
for column, label, value in zip(st.columns(4), ["Revenue", "Orders", "Units sold", "Average order value"],
                                [f"€{revenue:,.2f}", f"{orders:,}", f"{units:,}", f"€{average:,.2f}"]):
    column.metric(label, value, border=True)

state = snapshot["state"]
loaded = state["loaded_at"].astimezone(timezone.utc).strftime("%d %b %Y, %H:%M UTC")
st.caption(f"Orders dated {start:%d %b %Y} – {end:%d %b %Y} · Warehouse loaded {loaded}")

def chart_frame(rows):
    # Keep Decimal for calculations/CSV; convert only for chart rendering.
    return pd.DataFrame([{k: float(v) if isinstance(v, Decimal) else v for k, v in row.items()} for row in rows])

st.subheader("Revenue by day")
st.bar_chart(chart_frame(filtered["daily_sales"]), x="event_date", y="revenue_eur", color="#0D9488", x_label="Order date (UTC)", y_label="Revenue (EUR)")
st.caption("Only dates with recorded orders are shown; gaps do not establish zero sales.")

countries = rollup(filtered["country_sales"], ["country"])
products = rollup(filtered["product_sales"], ["product_id", "product_name"])
left, right = st.columns(2)
with left:
    st.subheader("Revenue by country")
    st.bar_chart(chart_frame(countries), x="country", y="revenue_eur", horizontal=True, color="#0D9488", x_label="Revenue (EUR)", y_label="Country")
with right:
    st.subheader("Revenue by product")
    product_chart = chart_frame(products)
    product_chart["product"] = product_chart["product_name"] + " · " + product_chart["product_id"]
    st.bar_chart(product_chart, x="product", y="revenue_eur", horizontal=True, color="#3B82F6", x_label="Revenue (EUR)", y_label="Product")

st.subheader("Explore the numbers")
tabs = st.tabs(["Daily sales", "Countries", "Products"])
for tab, rows, filename in zip(tabs, [filtered["daily_sales"], countries, products], ["daily_sales", "country_sales", "product_sales"]):
    with tab:
        st.dataframe(chart_frame(rows), hide_index=True, width="stretch")
        st.download_button("Download CSV", pd.DataFrame(rows).to_csv(index=False).encode("utf-8"),
                           file_name=f"{filename}_{start}_{end}.csv", mime="text/csv", key=filename)
st.caption("Date range applies to every chart and table. Average order value = total revenue ÷ total orders. Unique customers are not summed across dates or groups.")
with st.expander("Data provenance"):
    st.code(state["gold_run_id"], language=None)
    st.write("Source: PostgreSQL analytics tables published by the Airflow pipeline. Each page refresh reads one consistent warehouse snapshot.")
