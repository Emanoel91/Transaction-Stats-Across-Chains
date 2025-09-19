import streamlit as st
import pandas as pd
import requests
import snowflake.connector
import plotly.express as px
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Transaction Stats Across Chains",
    layout="wide"
)

st.title("⛓ Transaction Stats Across Chains")
st.info("⏳ On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection ---------------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Load API Data ----------------------------------------------------------------------------------------------------
api_url = "https://api.dune.com/api/v1/query/5804139/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"
resp = requests.get(api_url)
api_data = resp.json()

# تبدیل API نتیجه به DataFrame
df_api = pd.DataFrame(api_data["result"]["rows"])
df_api["Date"] = pd.to_datetime(df_api["Date"])

# --- Load Snowflake Data -----------------------------------------------------------------------------------------------
query = """
select date_trunc('day',block_timestamp) as "Date", 
       count(distinct tx_id) as "Txns Count", 
       'Axelar' as "Chain"
from AXELAR.CORE.FACT_TRANSACTIONS
where block_timestamp::date >= current_date - 30
group by 1
order by 1
"""
df_axelar = pd.read_sql(query, conn)
df_axelar["Date"] = pd.to_datetime(df_axelar["Date"])

# --- Combine API + Snowflake ------------------------------------------------------------------------------------------
df_all = pd.concat([df_api, df_axelar], ignore_index=True)

# اطمینان از نوع داده‌ها
df_all["Txns Count"] = df_all["Txns Count"].astype(int)
df_all["Chain"] = df_all["Chain"].astype(str)

# --- Row 1: Line Chart - Daily Txns -----------------------------------------------------------------------------------
st.subheader("📈 Daily Transactions per Chain (Last 30 Days)")
df_line = df_all.sort_values("Date")  # مرتب‌سازی تاریخ
fig_line = px.line(
    df_line,
    x="Date",
    y="Txns Count",
    color="Chain",
    title="Daily Transactions Across Chains"
)
fig_line.update_layout(legend_title_text="Chain")
st.plotly_chart(fig_line, use_container_width=True)

# --- Row 2: Bar Chart - Total Txns ------------------------------------------------------------------------------------
st.subheader("📊 Total Transactions per Chain (Last 30 Days)")
df_total = df_all.groupby("Chain", as_index=False)["Txns Count"].sum()
df_total = df_total.sort_values("Txns Count", ascending=False)  # مرتب‌سازی از بزرگ به کوچک
fig_bar_total = px.bar(
    df_total,
    x="Chain",
    y="Txns Count",
    text="Txns Count",
    color="Chain",
    title="Total Transactions by Chain (30 Days)"
)
fig_bar_total.update_traces(texttemplate='%{text}', textposition='inside')
st.plotly_chart(fig_bar_total, use_container_width=True)

# --- Row 3: Bar Chart - Avg Daily Txns --------------------------------------------------------------------------------
st.subheader("📊 Average Daily Transactions per Chain (Last 30 Days)")
df_avg = df_all.groupby("Chain", as_index=False)["Txns Count"].mean()
df_avg["Txns Count"] = df_avg["Txns Count"].round().astype(int)  # رند بدون اعشار
df_avg = df_avg.sort_values("Txns Count", ascending=False)  # مرتب‌سازی از بزرگ به کوچک
fig_bar_avg = px.bar(
    df_avg,
    x="Chain",
    y="Txns Count",
    text="Txns Count",
    color="Chain",
    title="Average Daily Transactions by Chain (30 Days)"
)
fig_bar_avg.update_traces(texttemplate='%{text}', textposition='inside')
st.plotly_chart(fig_bar_avg, use_container_width=True)
