"""
Streamlit UI for EDU Insights Platform
======================================
Interactive web interface for generating and exploring educational reports
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="EDU Insights Platform", page_icon="📊", layout="wide")

st.markdown('<div style="font-size:2.5rem;font-weight:bold;color:#1f77b4">Educational Insights Platform</div>', unsafe_allow_html=True)

page = st.sidebar.radio("Navigation", ["🏠 Home", "📝 Generate", "📊 Explore", "📚 Archive"])

if page == "🏠 Home":
    st.markdown("### 💬 What would you like to explore today?")
    user_query = st.text_input("Ask a question", placeholder='e.g., "Generate MTC pupil characteristics for 2024-25"')
    if st.button("🔍 Ask"):
        st.success("✅ Query understood!")

elif page == "📝 Generate":
    st.markdown("### Generate Report")
    report_type = st.selectbox("Report Type", ["MTC Pupil Characteristics", "MTC School Characteristics"])
    time_period = st.selectbox("Time Period", ["2024-25", "2023-24"])
    if st.button("🚀 Generate"):
        st.success("✅ Report generated!")
        df = pd.DataFrame({'Characteristic': ['Boys', 'Girls'], 'Score': [19.2, 20.5]})
        st.dataframe(df)

elif page == "📊 Explore":
    st.markdown("### Explore Data")
    df = pd.DataFrame({'Characteristic': ['Boys', 'Girls', 'No SEN', 'SEN Support'], 'Score': [19.2, 20.5, 20.8, 15.2]})
    fig = px.bar(df, x='Characteristic', y='Score')
    st.plotly_chart(fig)

elif page == "📚 Archive":
    st.markdown("### Report Archive")
    st.markdown("**Certified Reports:**")
    st.markdown("- MTC National 2022-2025 ✅")
    st.download_button("💾 Download", data="mock", file_name="report.csv")
