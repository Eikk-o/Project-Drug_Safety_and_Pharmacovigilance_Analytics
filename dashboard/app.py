import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px

st.set_page_config(page_title="Pharmacovigilance Dashboard", layout="wide")

client = MongoClient("mongodb://localhost:27017")
db = client["pharmacovigilance"]

st.title("Drug Safety & Pharmacovigilance Dashboard")


def load(collection):
    return pd.DataFrame(list(db[collection].find({}, {"_id": 0})))


col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Reported Drugs")
    df = load("top_drugs")
    if not df.empty:
        st.plotly_chart(px.bar(df, x="drug_name", y="num_reports"), use_container_width=True)

with col2:
    st.subheader("Risk Scores by Drug")
    df = load("risk_scores")
    if not df.empty:
        df = df.sort_values("risk_score", ascending=False)
        st.plotly_chart(px.bar(df, x="drug_name", y="risk_score"), use_container_width=True)

st.subheader("Monthly Report Evolution")
df = load("monthly_reports")
if not df.empty:
    df = df.sort_values("report_month")
    st.plotly_chart(px.line(df, x="report_month", y="num_reports"), use_container_width=True)

st.subheader("Detected Safety Signals (PRR > 2)")
df = load("signals")
if not df.empty:
    st.dataframe(df)