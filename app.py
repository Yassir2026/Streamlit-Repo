import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Import database connection modules
from database_connections.supabase_connection import (
    load_fda_volume_analysis,
    load_top_drugs_analysis,
    load_global_adverse_distribution,
    load_fda_safety_trends,
    load_top_suspect_drugs
)
from database_connections.snowflake_connection import (
    load_executive_dashboard_data,
    load_patient_demographics_data,
    load_encounter_analysis_data,
    load_claims_management_data,
    load_financial_insights_data,
    load_geographic_analysis_data,
    load_fraud_fact_claims,
    load_fraud_daily_summary,
    load_fraud_monthly_trend,
    load_fraud_denial_reason,
    load_fraud_providers,
    load_fraud_demographics,
    load_fraud_procedures,
    load_fraud_heatmap
)

# Page configuration
st.set_page_config(
    page_title="Data Engineering Portfolio",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS styles
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #555;
        text-align: center;
        margin-bottom: 2rem;
    }
    .project-card {
        background: linear-gradient(135deg, 
            var(--secondary-background-color) 0%, 
            var(--background-color) 100%);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 12px;
        padding: 24px;
        margin: 16px 0;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }
    .metric-card {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 8px;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

# Sidebar navigation
st.sidebar.title("📊 Navigation")
page = st.sidebar.radio(
    "Choose a page:",
    ["🏠 Home", "☁️ AWS + OpenFDA Pipeline", "❄️ Snowflake + Synthea Pipeline", "🔍 Claims Fraud ETL Pipeline"]
)

st.sidebar.markdown("---")

# Add a refresh button to clear the cache
if st.sidebar.button("🔄 Refresh Data"):
    # Clears all st.cache_data functions
    st.cache_data.clear()
    # Reruns the app to fetch the latest data
    st.rerun()

st.sidebar.info("💡 Data is cached for up to 15 minutes. Click refresh for the latest data.")

# ==================== Home Page ====================
if page == "🏠 Home":
    st.markdown('<div class="main-header">Data Engineering Portfolio</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Showcasing Real-time Data Pipelines & Analytics</div>', unsafe_allow_html=True)
    st.markdown("""<div class="sub-header" style="text-align: right;">by Yassir Adam
    <a href="https://www.linkedin.com/in/yassir-adam2023/" target="_blank" style="text-decoration: none; margin-left: 10px;">
            🔗 LinkedIn
        </a></div>""", unsafe_allow_html=True)

    # Introduction
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        ### 👨‍💻 About This Portfolio
        
        Welcome! This platform demonstrates three production-ready data engineering projects 
        that showcase skills in **cloud architecture**, **real-time data processing**, 
        **data warehousing**, and **fraud detection analytics**.
        
        All pipelines process real-world healthcare data and provide actionable insights 
        through interactive visualizations.
        """)
    
    st.markdown("---")
    
    # Project cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="project-card">
            <h3>☁️ AWS + OpenFDA Real-time Pipeline</h3>
            <p><strong>Technologies:</strong> AWS Lambda, Kinesis, Redshift, Python</p>
            <p><strong>Highlights:</strong></p>
            <ul>
                <li>Real-time data ingestion from OpenFDA API</li>
                <li>Stream processing with AWS Kinesis</li>
                <li>Automated & scalable ETL pipeline</li>
                <li>Data warehousing in AWS Redshift</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown("""
        <div class="project-card">
            <h3>❄️ Snowflake + Synthea Pipeline</h3>
            <p><strong>Technologies:</strong> Snowflake, Snowpipe, dbt, Python</p>
            <p><strong>Highlights:</strong></p>
            <ul>
                <li>Comprehensive Data Analysis with SQL & Python</li>
                <li>Real-time Insights via Snowpipe</li>
                <li>Interactive Visualization with Streamlit</li>
                <li>Scalable & Cost-effective Cloud Architecture</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="project-card">
            <h3>🔍 Claims Fraud ETL Pipeline</h3>
            <p><strong>Technologies:</strong> MWAA (Airflow), Databricks, Snowflake, Streamlit</p>
            <p><strong>Highlights:</strong></p>
            <ul>
                <li>Simulated synthetic health data via Synthea & SDV</li>
                <li>Large-scale Spark data transformations in Databricks</li>
                <li>Enterprise scalable DWH via Snowflake Star-Schema</li>
                <li>Interactive Streamlit dashboard for Fraud KPIs</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Architecture comparison
    st.markdown("### 🏗️ Architecture Overview")
    
    tab1, tab2, tab3 = st.tabs(["AWS Architecture", "Snowflake Architecture", "Claims Fraud Architecture"])
    
    with tab1:
        st.markdown("""
        ```
        ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
        │   Open FDA API   │───▶│  Data Ingestion │───▶│  Kinesis Stream │
        │                 │    │   (Lambda)      │    │                 │
        └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                            │
                                                            ▼
        ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
        │   Data Analysis │◀───│ Data Processing │───▶│   Data Storage  │
        │(This Dashboard) │    │   (Lambda)      │    │   (Firehose +   │
        │                 │    │                 │    │    Redshift)    │
        └─────────────────┘    └─────────────────┘    └─────────────────┘
        ```
        **Key Features:**
        - Serverless architecture for cost efficiency
        - Stream processing for real-time insights
        - Scalable data warehouse for historical analysis
        """)
        
    with tab2:
        st.markdown("""
        ```
        ┌───────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐
        │   Data Source │───▶│    Raw Layer    │───▶│ Processed Layer │───▶│  Analytics Layer │
        │    (Synthea)  │    │ (S3, Snowpipe)  │    │ (Fact/Dimension)│    │      (Views)     │
        └───────────────┘    └─────────────────┘    └─────────────────┘    └──────────────────┘
                                                                                  │
                                                                                  ▼
                                                                           ┌────────────────┐
                                                                           │ Presentation   │
                                                                           │     Layer      │
                                                                           │   (Streamlit)  │
                                                                           └────────────────┘
        ```
        **Key Features:**
        - 4-layer architecture for clarity and separation of concerns.
        - Automated data ingestion using Snowpipe.
        - Scalable processing with Snowflake's virtual warehouses.
        """)

    with tab3:
        st.markdown("""
        ```text
        ┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
        │ Synthea/SDV │───▶│   Data Lake  │───▶│  Databricks  │───▶│ Snowflake DWH│
        │ Generation  │    │  (Amazon S3) │    │Transformation│    │              │
        └─────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                                                                           │
        Orchestrated by: Amazon MWAA (Airflow)                             ▼
                                                                  ┌──────────────┐
                                                                  │ Streamlit UI │
                                                                  └──────────────┘
        ```
        **Key Features:**
        - E2E Pipeline orchestration using managed Apache Airflow.
        - Scalable Spark transformations and data cleaning using Databricks.
        - High-concurrency analytical queries on Snowflake Cloud DWH.
        - Interactive visualizations mapped directly to aggregates.
        """)
    
    st.markdown("---")
    st.info("👈 Use the sidebar to explore each project in detail with live data and interactive visualizations!")

# ==================== AWS + OpenFDA Page ====================
elif page == "☁️ AWS + OpenFDA Pipeline":
    st.title("☁️ Real-time Data Streaming ETL for FDA Drug Events")
    st.markdown("""
    This project is a comprehensive real-time data streaming and ETL (Extract, Transform, Load) system 
    built on the AWS platform. It is designed to process and analyze FDA drug event data from the 
    Open FDA API, transforming it into a structured format for analysis and visualization.
    """)
    
    # Tech stack
    with st.expander("🔧 View Technical Details & Architecture"):
        st.markdown("""
        This project is a comprehensive real-time data streaming and ETL (Extract, Transform, Load) system built on the AWS platform. It is designed to process and analyze FDA drug event data from the Open FDA API, transforming it into a structured format for analysis and visualization.

        **Key Features:**
        - **Real-time Data Ingestion**: Continuously fetches drug event data from the Open FDA API.
        - **Stream Processing**: Uses AWS Kinesis for real-time data streaming and processing.
        - **Data Transformation**: Cleans and transforms raw data into a structured format.
        - **Scalable Storage**: Stores processed data in AWS Redshift for efficient querying.
        - **Automated Pipeline**: Fully automated ETL pipeline with monitoring and state management.
        """)
        st.subheader("Architecture")
        st.markdown("""
        ```
        ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
        │   Open FDA API   │───▶│  Data Ingestion │───▶│  Kinesis Stream │
        │                 │    │   (Lambda)      │    │                 │
        └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                                │
                                                                ▼
        ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
        │   Data Analysis │◀───│ Data Processing │───▶│   Data Storage  │
        │   (Streamlit)   │    │   (Lambda)      │    │   (Firehose +   │
        │                 │    │                 │    │    Redshift)    │
        └─────────────────┘    └─────────────────┘    └─────────────────┘
        ```
        """)
        st.subheader("Technology Stack")
        st.markdown("""
        | Component | Technology | Purpose |
        |-----------|------------|---------|
        | **Compute** | AWS Lambda | Serverless functions for data processing |
        | **Streaming** | AWS Kinesis Data Streams | Real-time data streaming |
        | **Delivery** | AWS Kinesis Data Firehose | Data delivery and transformation |
        | **Storage** | AWS S3 | Object storage for intermediate data |
        | **Database** | AWS Redshift | Data warehouse for analytics |
        | **Orchestration** | AWS EventBridge | Event scheduling and orchestration |
        | **State Management** | AWS DynamoDB | Tracking ingestion state |
        | **Visualization** | Streamlit | Data visualization and dashboards |
        | **Language** | Python 3.x | Lambda function implementation |
        """)

    st.markdown("---")
    
    # Load FDA data from Redshift
    @st.cache_data(ttl=15 * 60)
    def load_fda_dashboard_data():
        # Try to load real data from Redshift, fall back to mock data if connection fails
        volume_df = load_fda_volume_analysis()
        top_drugs_df = load_top_drugs_analysis()
        global_dist_df = load_global_adverse_distribution()
        trends_df = load_fda_safety_trends()
        suspect_drugs_df = load_top_suspect_drugs()
        
        if volume_df is not None and top_drugs_df is not None:
            return volume_df, top_drugs_df, global_dist_df, trends_df, suspect_drugs_df
        
        # Fallback to mock data if database connection fails
        st.warning("⚠️ Using mock data - database connection not available")
        
        # Mock volume data
        dates = pd.date_range(end=datetime.now(), periods=12, freq='M')
        volume_data = []
        for date in dates:
            volume_data.append({
                'total_reports': np.random.randint(1000, 5000),
                'serious_reports': np.random.randint(100, 500),
                'fatal_reports': np.random.randint(5, 50),
                'countries_affected': np.random.randint(10, 30),
                'report_month': date
            })
        volume_df = pd.DataFrame(volume_data)
        
        # Mock top drugs data
        drugs = ['ASPIRIN', 'IBUPROFEN', 'ACETAMINOPHEN', 'LISINOPRIL', 'METFORMIN',
                 'ATORVASTATIN', 'AMLODIPINE', 'OMEPRAZOLE', 'SIMVASTATIN', 'PANTOPRAZOLE']
        top_drugs_data = []
        for drug in drugs:
            top_drugs_data.append({
                'medicinalproduct': drug,
                'generic_name': drug.lower(),
                'report_count': np.random.randint(100, 1000),
                'recovered_count': np.random.randint(50, 800),
                'fatal_count': np.random.randint(0, 20),
                'avg_age': np.random.randint(30, 80),
                'male_count': np.random.randint(40, 200),
                'female_count': np.random.randint(40, 200)
            })
        top_drugs_df = pd.DataFrame(top_drugs_data)
        
        # Mock global distribution data
        countries = ['US', 'GB', 'CA', 'AU', 'DE', 'FR', 'JP', 'IT', 'ES', 'BR']
        global_dist_data = []
        for country in countries:
            global_dist_data.append({
                'occurcountry': country,
                'report_count': np.random.randint(100, 2000),
                'serious_reports': np.random.randint(10, 200),
                'fatal_reports': np.random.randint(0, 20),
                'unique_serious_reports': np.random.randint(10, 150)
            })
        global_dist_df = pd.DataFrame(global_dist_data)
        
        # Mock trends data
        trend_dates = pd.date_range(end=datetime.now(), periods=26, freq='W')
        trends_data = []
        for date in trend_dates:
            trends_data.append({
                'receivedate': date,
                'total_reports': np.random.randint(200, 1000),
                'serious_reports': np.random.randint(20, 100),
                'fatal_reports': np.random.randint(0, 10),
                'hospitalization_reports': np.random.randint(10, 80)
            })
        trends_df = pd.DataFrame(trends_data)
        
        # Mock suspect drugs data
        suspect_drugs_data = []
        for drug in drugs[:8]:
            suspect_drugs_data.append({
                'medicinalproduct': drug,
                'generic_name': drug.lower(),
                'drugcharacterization': '1',
                'report_count': np.random.randint(80, 800),
                'recovered_count': np.random.randint(40, 600),
                'fatal_count': np.random.randint(0, 15),
                'unique_reports': np.random.randint(50, 400)
            })
        suspect_drugs_df = pd.DataFrame(suspect_drugs_data)
        
        return volume_df, top_drugs_df, global_dist_df, trends_df, suspect_drugs_df
    
    with st.spinner("Loading FDA adverse events data..."):
        volume_df, top_drugs_df, global_dist_df, trends_df, suspect_drugs_df = load_fda_dashboard_data()
    
    # Volume Analysis Section
    st.markdown("### 📊 Volume Analysis")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_reports = volume_df['total_reports'].sum()
        st.metric("Total Reports", f"{total_reports:,}")
    
    with col2:
        serious_reports = volume_df['serious_reports'].sum()
        st.metric("Serious Reports", f"{serious_reports:,}")
    
    with col3:
        fatal_reports = volume_df['fatal_reports'].sum()
        st.metric("Fatal Reports", f"{fatal_reports:,}")
    
    st.markdown("---")
    
    # Top 10 Drugs Section
    st.markdown("### 💊 Top 10 Drugs: Patient Outcomes & Demographics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Top 10 Drugs: Patient Outcomes")
        top_outcomes = top_drugs_df.head(10)[['medicinalproduct', 'report_count', 'recovered_count', 'fatal_count']]
        fig_outcomes = px.bar(
            top_outcomes,
            x='medicinalproduct',
            y=['recovered_count', 'fatal_count'],
            title="Patient Outcomes by Drug",
            labels={'medicinalproduct': 'Drug Name', 'value': 'Number of Cases', 'variable': 'Outcome'},
            color_discrete_sequence=['#2ca02c', '#d62728']
        )
        st.plotly_chart(fig_outcomes, use_container_width=True)
    
    with col2:
        st.markdown("#### Top 10 Drugs: Patient Demographics")
        top_demographics = top_drugs_df.head(10)[['medicinalproduct', 'avg_age', 'male_count', 'female_count']]
        fig_demographics = px.bar(
            top_demographics,
            x='medicinalproduct',
            y=['male_count', 'female_count'],
            title="Patient Gender Distribution by Drug",
            labels={'medicinalproduct': 'Drug Name', 'value': 'Number of Patients', 'variable': 'Gender'},
            color_discrete_sequence=['#1f77b4', '#ff69b4']
        )
        st.plotly_chart(fig_demographics, use_container_width=True)
    
    # Global Adverse Event Distribution
    st.markdown("---")
    st.markdown("### 🌍 Global Adverse Event Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Top Countries by Report Count")
        top_countries = global_dist_df.head(15)
        fig_countries = px.bar(
            top_countries,
            x='occurcountry',
            y='report_count',
            title="Reports by Country",
            color='report_count',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_countries, use_container_width=True)
    
    with col2:
        st.markdown("#### Global Report Distribution")
        
        # Manual mapping from 2-letter to 3-letter ISO codes for choropleth map
        code_mapping = {
            'US': 'USA', 'GB': 'GBR', 'CA': 'CAN', 'AU': 'AUS', 'DE': 'DEU',
            'FR': 'FRA', 'JP': 'JPN', 'IT': 'ITA', 'ES': 'ESP', 'BR': 'BRA',
            'CN': 'CHN', 'IN': 'IND', 'MX': 'MEX', 'RU': 'RUS', 'ZA': 'ZAF',
            'KR': 'KOR', 'NL': 'NLD', 'CH': 'CHE', 'SE': 'SWE', 'AR': 'ARG',
            # Add other common codes as needed
        }
        map_df = global_dist_df.copy()
        map_df['iso_alpha'] = map_df['occurcountry'].map(code_mapping)

        tab1, tab2 = st.tabs(["🌍 Choropleth Map", "🔥 Heatmap Table"])

        with tab1:
            fig_map = px.choropleth(
                map_df.dropna(subset=['iso_alpha']),
                locations="iso_alpha",
                color="report_count",
                hover_name="occurcountry",
                hover_data=["serious_reports", "fatal_reports"],
                color_continuous_scale=px.colors.sequential.Plasma,
                title="Global Distribution of Adverse Event Reports"
            )
            fig_map.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
            st.plotly_chart(fig_map, use_container_width=True)

        with tab2:
            st.markdown("Top 15 Countries by Report Metrics")
            heatmap_df = global_dist_df.head(15).set_index('occurcountry')
            st.dataframe(
                heatmap_df[['report_count', 'serious_reports', 'fatal_reports']].style.background_gradient(cmap='Reds'),
                use_container_width=True
            )
    
    # FDA Safety Report Trends
    st.markdown("---")
    st.markdown("### 📊 FDA Safety Report Trends (Last 6 Months)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Weekly Report Trends")
        fig_weekly = px.line(
            trends_df,
            x='receivedate',
            y=['total_reports', 'serious_reports', 'hospitalization_reports'],
            title="Weekly Report Trends",
            labels={'value': 'Number of Reports', 'variable': 'Report Type'},
            color_discrete_sequence=['#1f77b4', '#ff7f0e', '#9467bd']
        )
        st.plotly_chart(fig_weekly, use_container_width=True)
    
    with col2:
        st.markdown("#### Report Type Distribution")
        # Check if trends_df is not empty before accessing the last row
        if not trends_df.empty:
            latest_week = trends_df.iloc[-1]
            trend_data = pd.DataFrame({
                'Report Type': ['Total Reports', 'Serious Reports', 'Fatal Reports', 'Hospitalization Reports'],
                'Count': [latest_week['total_reports'], latest_week['serious_reports'],
                         latest_week['fatal_reports'], latest_week['hospitalization_reports']]
            })
            fig_pie = px.pie(
                trend_data,
                values='Count',
                names='Report Type',
                title="Latest Week Report Distribution",
                hole=0.3
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.warning("No trend data available for visualization")
    
    # Most Reported Suspect Drugs
    st.markdown("---")
    st.markdown("### ⚠️ Most Reported Suspect Drugs - Top 10")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Suspect Drugs Report Count")
        top_suspect = suspect_drugs_df.head(10)
        fig_suspect = px.bar(
            top_suspect,
            x='medicinalproduct',
            y='report_count',
            title="Top Suspect Drugs by Report Count",
            color='report_count',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_suspect, use_container_width=True)
    
    with col2:
        st.markdown("#### Suspect Drugs Recovery Rate")
        top_suspect = suspect_drugs_df.head(10).copy()
        top_suspect['recovery_rate'] = (top_suspect['recovered_count'] / top_suspect['report_count'] * 100).round(1)
        fig_recovery = px.bar(
            top_suspect,
            x='medicinalproduct',
            y='recovery_rate',
            title="Recovery Rate by Suspect Drug",
            color='recovery_rate',
            color_continuous_scale='Greens'
        )
        st.plotly_chart(fig_recovery, use_container_width=True)
    
    # Data Sample
    with st.expander("📋 View Raw Data Sample"):
        st.markdown("#### Volume Data Sample")
        st.dataframe(volume_df.head())
        
        st.markdown("#### Top Drugs Data Sample")
        st.dataframe(top_drugs_df.head())
        
        st.markdown("#### Global Distribution Data Sample")
        st.dataframe(global_dist_df.head())

# ==================== Snowflake + Synthea Page ====================
elif page == "❄️ Snowflake + Synthea Pipeline":
    st.title("❄️ Healthcare Insurance Analytics Platform")
    st.markdown("This project is a sophisticated healthcare insurance analytics system designed to help users better understand and manage their healthcare insurance needs. Built on the Snowflake platform, it processes healthcare insurance data generated by Synthea, providing comprehensive data collection, cleaning, analysis, and visualization capabilities.")

    # Technical Information
    with st.expander("🔧 View Technical Details & Architecture"):
        st.markdown("""
        This project is a sophisticated healthcare insurance analytics system designed to help users better understand and manage their healthcare insurance needs. Built on the Snowflake platform, it processes healthcare insurance data generated by Synthea, providing comprehensive data collection, cleaning, analysis, and visualization capabilities.

        **Key Benefits:**
        - **Comprehensive Data Analysis**: Process and analyze large-scale healthcare insurance data with advanced SQL and Python capabilities.
        - **Real-time Insights**: Access up-to-date analytics with automated data loading through Snowpipe.
        - **Interactive Visualization**: User-friendly dashboards with filtering, sorting, and export capabilities.
        - **Scalable Architecture**: Built on Snowflake's cloud-native platform for unlimited scalability.
        - **Cost-effective**: Optimized data processing and storage to minimize operational costs.
        """)
        st.subheader("Architecture")
        st.markdown("""
        ```
        ┌───────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐
        │   Data Source │───▶│    Raw Layer    │───▶│ Processed Layer │───▶│  Analytics Layer │
        │    (Synthea)  │    │ (S3, Snowpipe)  │    │ (Fact/Dimension)│    │      (Views)     │
        └───────────────┘    └─────────────────┘    └─────────────────┘    └──────────────────┘
                                                                                  │
                                                                                  ▼
                                                                           ┌────────────────┐
                                                                           │ Presentation   │
                                                                           │     Layer      │
                                                                           │   (Streamlit)  │
                                                                           └────────────────┘
        ```
        The platform follows a 4-layer data modeling architecture: Raw, Processed, Analytics, and Presentation.
        """)
        st.subheader("Technology Stack")
        st.markdown("""
        | Component | Technology | Purpose |
        |---|---|---|
        | **Data Storage** | Amazon S3 | Raw healthcare insurance data storage |
        | **Data Warehouse** | Snowflake | Primary data warehouse platform for large-scale data processing |
        | **Data Loading** | Snowpipe | Automated data loading and real-time updates |
        | **Data Analysis** | Snowflake SQL, Python | Data analysis and modeling |
        | **Data Visualization** | Streamlit, Plotly | Interactive data visualization interface |
        | **Application** | Streamlit | Web-based analytics platform |
        | **Data Generation** | Synthea | Synthetic healthcare data generation for testing and development |
        """)
        
    st.markdown("---")

    # Load healthcare data from Snowflake
    @st.cache_data(ttl=15 * 60)
    def load_healthcare_dashboard_data():
        # Try to load real data from Snowflake, fall back to mock data if connection fails
        exec_df = load_executive_dashboard_data()
        patient_df = load_patient_demographics_data()
        encounter_df = load_encounter_analysis_data()
        claims_df = load_claims_management_data()
        financial_df = load_financial_insights_data()
        geo_df = load_geographic_analysis_data()
        
        if all(df is not None for df in [exec_df, patient_df, encounter_df, claims_df, financial_df, geo_df]):
            # Standardize all column names to lowercase for consistency
            exec_df.columns = exec_df.columns.str.lower()
            patient_df.columns = patient_df.columns.str.lower()
            encounter_df.columns = encounter_df.columns.str.lower()
            claims_df.columns = claims_df.columns.str.lower()
            financial_df.columns = financial_df.columns.str.lower()
            geo_df.columns = geo_df.columns.str.lower()
            return exec_df, patient_df, encounter_df, claims_df, financial_df, geo_df
        
        # Fallback to mock data if database connection fails
        st.warning("⚠️ Using mock data - database connection not available")
        
        # Mock executive dashboard data
        exec_data = []
        for i in range(24):  # 24 months
            exec_data.append({
                'month_date': pd.Timestamp.now() - pd.DateOffset(months=i),
                'year': pd.Timestamp.now().year - (i // 12),
                'month': pd.Timestamp.now().month - (i % 12),
                'encounter_count': np.random.randint(1000, 5000),
                'unique_patients': np.random.randint(800, 4000),
                'total_revenue': np.random.uniform(500000, 2000000),
                'avg_cost_per_encounter': np.random.uniform(200, 800),
                'total_payer_coverage': np.random.uniform(400000, 1500000),
                'total_patient_payment': np.random.uniform(100000, 500000),
                'avg_coverage_percentage': np.random.uniform(70, 95),
                'encounter_class': np.random.choice(['ambulatory', 'emergency', 'inpatient', 'wellness']),
                'cost_category': np.random.choice(['low', 'medium', 'high']),
                'encounter_day_name': np.random.choice(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']),
                'state': np.random.choice(['MA', 'NY', 'CA', 'TX', 'FL'])
            })
        exec_df = pd.DataFrame(exec_data)
        
        # Mock patient demographics data
        patient_data = []
        for i in range(1000):
            patient_data.append({
                'patient_id': f'P{i:05d}',
                'full_name': f'Patient {i}',
                'age': np.random.randint(18, 90),
                'age_group': np.random.choice(['18-30', '31-45', '46-60', '61-75', '75+']),
                'gender': np.random.choice(['M', 'F']),
                'gender_full': np.random.choice(['Male', 'Female']),
                'race': np.random.choice(['White', 'Black', 'Asian', 'Hispanic', 'Other']),
                'ethnicity': np.random.choice(['Non-Hispanic', 'Hispanic']),
                'marital_status': np.random.choice(['Single', 'Married', 'Divorced', 'Widowed']),
                'state': np.random.choice(['MA', 'NY', 'CA', 'TX', 'FL']),
                'county': np.random.choice(['Suffolk', 'Middlesex', 'Essex', 'Norfolk', 'Bristol']),
                'city': np.random.choice(['Boston', 'NYC', 'Los Angeles', 'Houston', 'Miami']),
                'zip': np.random.randint(1000, 9999),
                'income': np.random.uniform(20000, 200000),
                'income_level': np.random.choice(['Very Low', 'Low', 'Medium', 'High', 'Very High']),
                'healthcare_expenses': np.random.uniform(1000, 50000),
                'healthcare_coverage': np.random.uniform(0.5, 1.0),
                'coverage_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
                'is_alive': True,
                'total_encounters': np.random.randint(1, 50),
                'total_medical_costs': np.random.uniform(1000, 100000),
                'avg_cost_per_encounter': np.random.uniform(100, 1000),
                'total_out_of_pocket': np.random.uniform(100, 10000)
            })
        patient_df = pd.DataFrame(patient_data)
        
        # Mock encounter data
        encounter_data = []
        for i in range(5000):
            encounter_data.append({
                'encounter_id': f'E{i:05d}',
                'encounter_date': pd.Timestamp.now() - pd.DateOffset(days=np.random.randint(0, 730)),
                'encounter_year': pd.Timestamp.now().year,
                'encounter_month': pd.Timestamp.now().month,
                'encounter_quarter': (pd.Timestamp.now().month - 1) // 3 + 1,
                'encounter_day_of_week': np.random.randint(0, 7),
                'encounter_day_name': np.random.choice(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']),
                'encounter_class': np.random.choice(['ambulatory', 'emergency', 'inpatient', 'wellness']),
                'encounter_code': np.random.choice(['Z00', 'Z01', 'Z02', 'Z03']),
                'encounter_description': np.random.choice(['Routine', 'Emergency', 'Surgical', 'Preventive']),
                'reason_code': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'reason_description': np.random.choice(['Abdominal Pain', 'Skin Rash', 'Fever', 'Chest Pain']),
                'cost_category': np.random.choice(['low', 'medium', 'high']),
                'duration_minutes': np.random.randint(15, 480),
                'duration_hours': np.random.uniform(0.25, 8),
                'base_encounter_cost': np.random.uniform(100, 5000),
                'total_claim_cost': np.random.uniform(200, 10000),
                'payer_coverage': np.random.uniform(100, 8000),
                'patient_out_of_pocket': np.random.uniform(50, 2000),
                'payer_coverage_percentage': np.random.uniform(70, 95),
                'patient_id': f'P{np.random.randint(0, 1000):05d}',
                'provider_id': f'PR{np.random.randint(0, 100):03d}',
                'payer_id': f'PY{np.random.randint(0, 50):03d}',
                'organization_id': f'ORG{np.random.randint(0, 20):02d}',
                'age': np.random.randint(18, 90),
                'age_group': np.random.choice(['18-30', '31-45', '46-60', '61-75', '75+']),
                'gender_full': np.random.choice(['Male', 'Female']),
                'state': np.random.choice(['MA', 'NY', 'CA', 'TX', 'FL']),
                'income_level': np.random.choice(['Very Low', 'Low', 'Medium', 'High', 'Very High']),
                'coverage_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'])
            })
        encounter_df = pd.DataFrame(encounter_data)
        
        # Mock claims data
        claims_data = []
        for i in range(2000):
            claims_data.append({
                'claim_id': f'C{i:05d}',
                'patient_id': f'P{np.random.randint(0, 1000):05d}',
                'provider_id': f'PR{np.random.randint(0, 100):03d}',
                'service_date': pd.Timestamp.now() - pd.DateOffset(days=np.random.randint(0, 730)),
                'service_year': pd.Timestamp.now().year,
                'service_month': pd.Timestamp.now().month,
                'service_quarter': (pd.Timestamp.now().month - 1) // 3 + 1,
                'current_illness_date': pd.Timestamp.now() - pd.DateOffset(days=np.random.randint(1, 30)),
                'primary_insurance_id': f'INS{np.random.randint(0, 50):03d}',
                'secondary_insurance_id': f'INS{np.random.randint(0, 50):03d}',
                'insurance_coverage_type': np.random.choice(['PPO', 'HMO', 'POS', 'EPO']),
                'primary_status': np.random.choice(['Pending', 'Approved', 'Denied', 'In Review']),
                'secondary_status': np.random.choice(['Pending', 'Approved', 'Denied', 'In Review']),
                'patient_status': np.random.choice(['Pending', 'Approved', 'Denied', 'In Review']),
                'primary_outstanding': np.random.uniform(0, 5000),
                'secondary_outstanding': np.random.uniform(0, 3000),
                'patient_outstanding': np.random.uniform(0, 2000),
                'total_outstanding': np.random.uniform(0, 10000),
                'outstanding_risk_level': np.random.choice(['Low Risk', 'Medium Risk', 'High Risk']),
                'primary_last_billed_date': pd.Timestamp.now() - pd.DateOffset(days=np.random.randint(1, 90)),
                'secondary_last_billed_date': pd.Timestamp.now() - pd.DateOffset(days=np.random.randint(1, 90)),
                'patient_last_billed_date': pd.Timestamp.now() - pd.DateOffset(days=np.random.randint(1, 90)),
                'primary_claim_type': np.random.choice(['Medical', 'Dental', 'Vision', 'Pharmacy']),
                'secondary_claim_type': np.random.choice(['Medical', 'Dental', 'Vision', 'Pharmacy']),
                'age': np.random.randint(18, 90),
                'age_group': np.random.choice(['18-30', '31-45', '46-60', '61-75', '75+']),
                'gender_full': np.random.choice(['Male', 'Female']),
                'state': np.random.choice(['MA', 'NY', 'CA', 'TX', 'FL']),
                'income_level': np.random.choice(['Very Low', 'Low', 'Medium', 'High', 'Very High']),
                'coverage_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
                'diagnosis1': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'diagnosis2': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'diagnosis3': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'diagnosis4': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'diagnosis5': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'diagnosis6': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'diagnosis7': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'diagnosis8': np.random.choice(['R10', 'R20', 'R30', 'R40']),
                'diagnosis_count': np.random.randint(1, 8)
            })
        claims_df = pd.DataFrame(claims_data)
        
        # Mock financial insights data
        financial_data = []
        for i in range(1000):
            financial_data.append({
                'encounter_date': pd.Timestamp.now() - pd.DateOffset(days=np.random.randint(0, 730)),
                'encounter_year': pd.Timestamp.now().year,
                'encounter_month': pd.Timestamp.now().month,
                'encounter_quarter': (pd.Timestamp.now().month - 1) // 3 + 1,
                'encounter_class': np.random.choice(['ambulatory', 'emergency', 'inpatient', 'wellness']),
                'cost_category': np.random.choice(['low', 'medium', 'high']),
                'patient_id': f'P{np.random.randint(0, 1000):05d}',
                'provider_id': f'PR{np.random.randint(0, 100):03d}',
                'payer_id': f'PY{np.random.randint(0, 50):03d}',
                'base_encounter_cost': np.random.uniform(100, 5000),
                'total_claim_cost': np.random.uniform(200, 10000),
                'payer_coverage': np.random.uniform(100, 8000),
                'patient_out_of_pocket': np.random.uniform(50, 2000),
                'payer_coverage_percentage': np.random.uniform(70, 95),
                'state': np.random.choice(['MA', 'NY', 'CA', 'TX', 'FL']),
                'county': np.random.choice(['Suffolk', 'Middlesex', 'Essex', 'Norfolk', 'Bristol']),
                'age_group': np.random.choice(['18-30', '31-45', '46-60', '61-75', '75+']),
                'gender_full': np.random.choice(['Male', 'Female']),
                'income_level': np.random.choice(['Very Low', 'Low', 'Medium', 'High', 'Very High']),
                'income': np.random.uniform(20000, 200000),
                'healthcare_expenses': np.random.uniform(1000, 50000),
                'healthcare_coverage': np.random.uniform(0.5, 1.0),
                'total_claim_outstanding': np.random.uniform(0, 10000),
                'total_claims': np.random.randint(1, 20),
                'monthly_total_revenue': np.random.uniform(500000, 2000000),
                'monthly_payer_coverage': np.random.uniform(400000, 1500000),
                'monthly_patient_payment': np.random.uniform(100000, 500000),
                'state_total_revenue': np.random.uniform(1000000, 5000000),
                'state_payer_coverage': np.random.uniform(800000, 4000000),
                'state_patient_payment': np.random.uniform(200000, 1000000),
                'class_total_revenue': np.random.uniform(500000, 2000000),
                'payer_total_revenue': np.random.uniform(200000, 1000000),
                'monthly_payer_coverage': np.random.uniform(400000, 1500000),
                'monthly_patient_payment': np.random.uniform(100000, 500000),
                'avg_coverage_by_payer': np.random.uniform(70, 95),
                'avg_coverage_by_income': np.random.uniform(70, 95),
                'avg_coverage_by_age': np.random.uniform(70, 95),
                'avg_cost_by_class': np.random.uniform(200, 1000),
                'avg_cost_by_provider': np.random.uniform(200, 1000),
                'patient_burden_percentage': np.random.uniform(1, 20),
                'financial_burden_category': np.random.choice(['Low Burden', 'Medium Burden', 'High Burden'])
            })
        financial_df = pd.DataFrame(financial_data)
        
        # Mock geographic analysis data
        geo_data = []
        states = ['MA', 'NY', 'CA', 'TX', 'FL']
        for state in states:
            for county in ['Suffolk', 'Middlesex', 'Essex', 'Norfolk', 'Bristol']:
                for city in ['Boston', 'NYC', 'Los Angeles', 'Houston', 'Miami']:
                    geo_data.append({
                        'state': state,
                        'county': county,
                        'city': city,
                        'patient_count': np.random.randint(100, 5000),
                        'avg_age': np.random.uniform(30, 70),
                        'avg_income': np.random.uniform(30000, 150000),
                        'avg_coverage_rate': np.random.uniform(0.7, 0.95),
                        'total_expenses': np.random.uniform(100000, 5000000),
                        'avg_expenses_per_patient': np.random.uniform(1000, 10000),
                        'encounter_count': np.random.randint(500, 25000),
                        'total_medical_costs': np.random.uniform(200000, 10000000),
                        'avg_cost_per_encounter': np.random.uniform(200, 1000),
                        'total_insurance_paid': np.random.uniform(150000, 8000000),
                        'total_patient_paid': np.random.uniform(50000, 2000000),
                        'avg_coverage_percentage': np.random.uniform(70, 95),
                        'unique_encounter_patients': np.random.randint(400, 4000),
                        'claim_count': np.random.randint(200, 10000),
                        'total_outstanding': np.random.uniform(10000, 500000),
                        'avg_outstanding_per_claim': np.random.uniform(50, 500),
                        'unique_claim_patients': np.random.randint(150, 3000),
                        'encounters_per_patient': np.random.uniform(1, 10),
                        'medical_cost_per_patient': np.random.uniform(1000, 20000),
                        'outstanding_per_patient': np.random.uniform(50, 1000),
                        'outstanding_percentage': np.random.uniform(5, 30),
                        'geographic_risk_level': np.random.choice(['Low Risk', 'Medium Risk', 'High Risk'])
                    })
        geo_df = pd.DataFrame(geo_data)
        
        return exec_df, patient_df, encounter_df, claims_df, financial_df, geo_df
    
    with st.spinner("Loading healthcare analytics data..."):
        exec_df, patient_df, encounter_df, claims_df, financial_df, geo_df = load_healthcare_dashboard_data()
    
    # Executive Summary KPIs
    st.markdown("### 📊 Executive Summary")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        # Check for patient_id column, if not found use id column
        if 'patient_id' in patient_df.columns:
            total_patients = patient_df['patient_id'].nunique()
        elif 'id' in patient_df.columns:
            total_patients = patient_df['id'].nunique()
        else:
            total_patients = len(patient_df)
        st.metric("Total Patients", f"{total_patients:,}")
    
    with col2:
        total_revenue = exec_df['total_revenue'].sum() / 1e6 if 'total_revenue' in exec_df.columns else 0
        st.metric("Total Revenue (2Yr)", f"${total_revenue:.2f}M")
    
    with col3:
        total_claims = claims_df['claim_id'].nunique() if 'claim_id' in claims_df.columns else len(claims_df)
        st.metric("Total Claims", f"{total_claims:,}")
    
    with col4:
        total_outstanding = claims_df['total_outstanding'].sum() if 'total_outstanding' in claims_df.columns else 0
        st.metric("Outstanding Amount", f"${total_outstanding:,.0f}")
    
    with col5:
        avg_coverage = financial_df['avg_coverage_by_income'].mean() if 'avg_coverage_by_income' in financial_df.columns else 0
        st.metric("Avg Coverage Rate", f"{avg_coverage:.1f}%")
    
    st.markdown("---")
    
    # Revenue and Volume Trends
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 💰 Revenue Trend")
        monthly_revenue = exec_df.groupby('month_date')[['total_revenue', 'total_payer_coverage']].sum().reset_index()
        fig_revenue = px.area(
            monthly_revenue,
            x='month_date',
            y=['total_revenue', 'total_payer_coverage'],
            labels={'value': 'Amount ($)', 'variable': 'Metric'},
            color_discrete_sequence=['#636EFA', '#00CC96']
        )
        st.plotly_chart(fig_revenue, use_container_width=True)
    
    with col2:
        st.markdown("#### 🏥 Encounter Volume Trend")
        monthly_volume = exec_df.groupby('month_date')['encounter_count'].sum().reset_index()
        fig_volume = px.bar(
            monthly_volume,
            x='month_date',
            y='encounter_count',
            color='encounter_count',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_volume, use_container_width=True)
    
    # Patient Demographics
    st.markdown("---")
    st.markdown("### 👥 Patient Demographics Analysis")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### Age Group Distribution")
        age_dist = patient_df['age_group'].value_counts().reset_index()
        age_dist.columns = ['AGE_GROUP', 'COUNT']
        fig_age = px.pie(age_dist, values='COUNT', names='AGE_GROUP', hole=0.3, color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_age, use_container_width=True)
    
    with col2:
        st.markdown("#### Income Level Analysis")
        income_stats = patient_df.groupby('income_level')['healthcare_expenses'].mean().reset_index()
        fig_income = px.bar(
            income_stats,
            x='income_level',
            y='healthcare_expenses',
            title="Avg Healthcare Expenses by Income",
            color='healthcare_expenses',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_income, use_container_width=True)
    
    with col3:
        st.markdown("#### Insurance Coverage by State")
        state_cov = patient_df.groupby('state')['healthcare_coverage'].mean().sort_values(ascending=False).head(10).reset_index()
        fig_state = px.bar(state_cov, x='healthcare_coverage', y='state', orientation='h')
        st.plotly_chart(fig_state, use_container_width=True)
    
    # Encounter Analysis
    st.markdown("---")
    st.markdown("### 🏥 Encounter & Provider Performance")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Encounters", f"{len(encounter_df):,}")
    
    with col2:
        avg_cost = encounter_df['total_claim_cost'].mean() if 'total_claim_cost' in encounter_df.columns else 0
        st.metric("Avg Cost", f"${avg_cost:,.2f}")
    
    with col3:
        total_payer_paid = encounter_df['payer_coverage'].sum()/1e6 if 'payer_coverage' in encounter_df.columns else 0
        st.metric("Total Payer Paid", f"${total_payer_paid:.2f}M")
    
    with col4:
        unique_providers = encounter_df['provider_id'].nunique() if 'provider_id' in encounter_df.columns else 0
        st.metric("Unique Providers", f"{unique_providers:,}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Top Providers by Revenue")
        provider_metrics = encounter_df.groupby('provider_id').agg({
            'total_claim_cost': 'sum',
            'encounter_id': 'count',
            'duration_minutes': 'mean'
        }).reset_index()
        
        provider_metrics.columns = ['PROVIDER_ID', 'TOTAL_REVENUE', 'ENCOUNTER_COUNT', 'AVG_DURATION']
        top_providers = provider_metrics.sort_values('TOTAL_REVENUE', ascending=False).head(15)
        
        fig_providers = px.bar(
            top_providers,
            x='PROVIDER_ID',
            y='TOTAL_REVENUE',
            color='TOTAL_REVENUE',
            color_continuous_scale='Greens',
            hover_data=['AVG_DURATION'],
            labels={'TOTAL_REVENUE': 'Total Revenue ($)'}
        )
        st.plotly_chart(fig_providers, use_container_width=True)
    
    with col2:
        st.markdown("#### Weekly Volume")
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        daily_counts = encounter_df['encounter_day_name'].value_counts().reindex(day_order).reset_index()
        daily_counts.columns = ['Day', 'Count']
        
        fig_weekly = px.line(daily_counts, x='Day', y='Count', markers=True)
        st.plotly_chart(fig_weekly, use_container_width=True)
    
    # Claims Management
    st.markdown("---")
    st.markdown("### 📋 Claims Management & Diagnosis Trends")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Claims Status Overview")
        status_dist = claims_df['primary_status'].value_counts().reset_index()
        status_dist.columns = ['Status', 'Count']
        fig_status = px.pie(status_dist, values='Count', names='Status', hole=0.4, title="Primary Claim Status")
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        st.markdown("#### Risk Level Analysis")
        risk_dist = claims_df.groupby('outstanding_risk_level')['total_outstanding'].sum().reset_index()
        risk_order = ['Low Risk', 'Medium Risk', 'High Risk']
        
        fig_risk = px.bar(
            risk_dist,
            x='outstanding_risk_level',
            y='total_outstanding',
            title="Outstanding Amount by Risk Level",
            color='total_outstanding',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_risk, use_container_width=True)
    
    # Financial Insights
    st.markdown("---")
    st.markdown("### 💰 Financial Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Income vs Patient Burden Trends")
        monthly_fin = financial_df.groupby(['encounter_year', 'encounter_month'])[['monthly_total_revenue', 'monthly_payer_coverage', 'monthly_patient_payment']].mean().reset_index()
        monthly_fin['Date'] = pd.to_datetime(monthly_fin['encounter_year'].astype(str) + '-' + monthly_fin['encounter_month'].astype(str) + '-01')
        monthly_fin = monthly_fin.sort_values('Date')
        
        fig_financial = go.Figure()
        fig_financial.add_trace(go.Scatter(x=monthly_fin['Date'], y=monthly_fin['monthly_total_revenue'], name='Total Revenue', fill='tozeroy'))
        fig_financial.add_trace(go.Scatter(x=monthly_fin['Date'], y=monthly_fin['monthly_patient_payment'], name='Patient OOP', line=dict(color='red')))
        st.plotly_chart(fig_financial, use_container_width=True)
    
    with col2:
        st.markdown("#### Financial Burden Categories")
        burden_dist = financial_df['financial_burden_category'].value_counts().reset_index()
        burden_dist.columns = ['Category', 'Count']
        
        fig_burden = px.pie(burden_dist, values='Count', names='Category', color_discrete_map={'High Burden':'red', 'Medium Burden':'orange', 'Low Burden':'green'})
        st.plotly_chart(fig_burden, use_container_width=True)
        
        st.info("""
        **Burden Definition:**
        - High Burden: Out-of-Pocket > 10% of Income
        - Medium Burden: Out-of-Pocket > 5% of Income
        - Low Burden: Others
        """)
    
    # Geographic Analysis
    st.markdown("---")
    st.markdown("### 🗺️ Geographic Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Total Medical Costs by State")
        state_costs = geo_df.groupby('state')['total_medical_costs'].sum().reset_index()
        state_costs = state_costs.sort_values('total_medical_costs', ascending=True)
        
        fig_geo = px.bar(
            state_costs,
            x='total_medical_costs',
            y='state',
            orientation='h',
            title='Total Medical Costs by State',
            labels={'total_medical_costs': 'Total Medical Costs ($)', 'state': 'State'},
            color='total_medical_costs',
            color_continuous_scale=px.colors.sequential.Blues
        )
        st.plotly_chart(fig_geo, use_container_width=True)
    
    with col2:
        st.markdown("#### Top Cities by Outstanding Debt")
        top_cities = geo_df.sort_values('total_outstanding', ascending=False).head(15)
        fig_cities = px.bar(top_cities, x='city', y='total_outstanding', color='geographic_risk_level',
                           labels={'total_outstanding': 'Outstanding Debt ($)'})
        st.plotly_chart(fig_cities, use_container_width=True)
    
    # Data Sample
    with st.expander("📋 View Raw Data Sample"):
        st.markdown("#### Executive Dashboard Data Sample")
        st.dataframe(exec_df.head())
        
        st.markdown("#### Patient Demographics Data Sample")
        st.dataframe(patient_df.head())
        
        st.markdown("#### Encounter Analysis Data Sample")
        st.dataframe(encounter_df.head())

# ==================== Claims Fraud ETL Pipeline Page ====================
elif page == "🔍 Claims Fraud ETL Pipeline":
    st.title("🔍 Claims Fraud ETL Pipeline")
    st.markdown("""
    A production-grade Claims Data Processing Platform leveraging **Amazon MWAA (Airflow)** for orchestration, 
    **Databricks** for high-performance Spark transformations, and **Snowflake** as the enterprise cloud data warehouse. 
    This pipeline handles simulated healthcare domain data (Synthea & SDV) spanning from generation to interactive fraud analytics.
    """)

    with st.expander("🔧 View Technical Details & Architecture"):
        st.markdown("""
        **Pipeline Overview:**
        - **Data Generation**: Uses Synthea & SDV to simulate real-world patient lifecycles, encounters, and financial claims.
        - **Data Lake (Staging)**: Amazon S3 serves as the central repository for raw landing data and intermediate persistence logic.
        - **Transformation**: Databricks (Spark) handles large-scale data cleaning, transformation, and business rule modeling.
        - **Data Warehouse**: Snowflake stores the structured analytics layer (star schema and data vault).

        **Key Metrics Tracked:**  
        - Fraud risk score, fraud indicator, denial indicator, risk level
        - Claim amount, approved amount, processing time, submission delay
        - Provider fraud rate, procedure-level fraud patterns, patient demographics
        """)
        st.subheader("Architecture")
        st.markdown("""
        ```text
        ┌──────────────────┐    ┌──────────────┐    ┌──────────────┐    ┌────────────────────────────────────────┐
        │ Data Generation  │    │  Data Lake   │    │  Databricks  │    │       Snowflake Data Warehouse         │
        │  (Synthea & SDV) │───▶│ (Amazon S3)  │───▶│   (Spark)    │───▶│ fact_claims |  dim_*  |  agg_*         │
        └──────────────────┘    └──────────────┘    └──────────────┘    └────────────────────────────────────────┘
                                                                                            │
        Orchestrated & Monitored by: Amazon MWAA (Airflow) & CloudWatch                     ▼
                                                                                  ┌──────────────────┐
                                                                                  │  Streamlit Dash  │
                                                                                  └──────────────────┘
        ```
        """)
        st.subheader("Technology Stack")
        st.markdown("""
        | Component | Technology | Purpose |
        |-----------|------------|---------|
        | **Data Generation** | Synthea & SDV | Simulate realistic patient lifecycles and structured financial claims |
        | **Data Lake Storage**| Amazon S3 | Central repository for raw landing data and intermediate persistence |
        | **Orchestration** | Amazon MWAA | Manage ETL workflows, track S3 file arrivals, and schedule Databricks jobs |
        | **Transformation** | Databricks (Spark) | Large-scale data cleaning, transformation, and business rule modeling |
        | **Data Warehouse** | Snowflake | Enterprise-grade cloud DWH for structured analytics and aggregated data |
        | **BI / Dashboards** | Streamlit | Interactive dashboards for KPIs, claims tracking, and fraud detection |
        | **Monitoring** | CloudWatch | Track cluster performance metrics, pipeline reliability, and infrastructure |
        """)

    st.markdown("---")

    # ── Data loader ──────────────────────────────────────────────────────────
    @st.cache_data(ttl=15 * 60)
    def load_fraud_etl_data():
        # 1. Try to load from Snowflake first
        fact_sf = load_fraud_fact_claims()
        daily_sf = load_fraud_daily_summary()
        monthly_sf = load_fraud_monthly_trend()
        denial_sf = load_fraud_denial_reason()
        prov_sf = load_fraud_providers()
        demo_sf = load_fraud_demographics()
        proc_sf = load_fraud_procedures()
        heatmap_sf = load_fraud_heatmap()
        
        # Check if all dataframes loaded correctly from Snowflake
        all_dbs_loaded = all(
            df is not None and not df.empty 
            for df in [fact_sf, daily_sf, monthly_sf, denial_sf, prov_sf, demo_sf, proc_sf, heatmap_sf]
        )
        
        if all_dbs_loaded:
            # Ensure column names match the lowercase formats expected by downstream code
            for df in [fact_sf, daily_sf, monthly_sf, denial_sf, prov_sf, demo_sf, proc_sf, heatmap_sf]:
                df.columns = df.columns.str.lower()
                
            return {
                'fact': fact_sf,
                'daily': daily_sf,
                'fraud_monthly': monthly_sf,
                'denial': denial_sf,
                'providers': prov_sf,
                'demographics': demo_sf,
                'procedures': proc_sf,
                'heatmap': heatmap_sf,
            }

        # 2. Fallback to mock data if Snowflake is unavailable or empty
        st.warning("⚠️ Using mock data - database connection not available or tables empty")
        
        np.random.seed(42)
        n_claims = 3000
        states = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
        insurance_types = ['Medicare', 'Medicaid', 'Commercial', 'Self-Pay']
        claim_types = ['Inpatient', 'Outpatient', 'Emergency', 'Pharmacy', 'Dental']
        fraud_types = ['Upcoding', 'Phantom Billing', 'Unbundling', 'Duplicate Claim', 'Identity Fraud']
        risk_levels = ['Low', 'Medium', 'High', 'Critical']
        denial_reasons = ['Medical Necessity', 'Prior Authorization', 'Duplicate Claim',
                          'Eligibility Issue', 'Missing Documentation', 'Coding Error']
        days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        encounter_classes = ['ambulatory', 'emergency', 'inpatient', 'wellness']
        age_groups = ['Child (<18)', 'Young Adult (18-34)', 'Middle Age (35-49)', 'Senior (50-64)', 'Elderly (65+)']
        procedure_codes = ['99213', '99214', '99232', '93000', '71046', '80053', '85025', '97110', '20610', '27447']
        procedure_descs = ['Office Visit Lvl3', 'Office Visit Lvl4', 'Subsequent Hospital Care',
                           'ECG', 'Chest X-Ray', 'Comprehensive Metabolic Panel',
                           'CBC', 'Therapeutic Exercise', 'Joint Aspiration', 'Total Knee Replacement']

        dates = pd.date_range('2023-01-01', '2024-12-31', periods=n_claims)

        # ── fact_claims ──────────────────────────────────────────────────────
        fraud_indicators = np.random.binomial(1, 0.12, n_claims)
        denial_indicators = np.random.binomial(1, 0.18, n_claims)
        claim_amounts = np.random.lognormal(7.5, 1.2, n_claims)
        approved_amounts = claim_amounts * np.random.uniform(0.5, 1.0, n_claims)
        fraud_risk_scores = np.where(
            fraud_indicators == 1,
            np.random.uniform(0.65, 1.0, n_claims),
            np.random.uniform(0.0, 0.45, n_claims)
        )
        fact_df = pd.DataFrame({
            'claim_id': [f'CLM{i:05d}' for i in range(n_claims)],
            'provider_id': [f'PRV{np.random.randint(0, 80):03d}' for _ in range(n_claims)],
            'patient_id': [f'PAT{np.random.randint(0, 500):04d}' for _ in range(n_claims)],
            'claim_amount': claim_amounts,
            'approved_amount': approved_amounts,
            'fraud_indicator': fraud_indicators,
            'denial_indicator': denial_indicators,
            'fraud_risk_score': fraud_risk_scores,
            'risk_level': np.random.choice(risk_levels, n_claims, p=[0.50, 0.28, 0.15, 0.07]),
            'claim_status': np.random.choice(['Approved', 'Denied', 'Pending', 'In Review'], n_claims, p=[0.65, 0.18, 0.12, 0.05]),
            'processing_time_days': np.random.randint(1, 45, n_claims),
            'submission_delay_days': np.random.randint(0, 30, n_claims),
            'encounter_date': dates,
            'fraud_type': np.where(fraud_indicators == 1,
                                   np.random.choice(fraud_types, n_claims),
                                   None),
            'denial_reason': np.where(denial_indicators == 1,
                                      np.random.choice(denial_reasons, n_claims),
                                      None),
            'provider_state': np.random.choice(states, n_claims),
            'insurance_type': np.random.choice(insurance_types, n_claims),
            'claim_type': np.random.choice(claim_types, n_claims),
            'day_of_week': [d.strftime('%A') for d in dates],
            'year': [d.year for d in dates],
            'month': [d.month for d in dates],
            'patient_age': np.random.randint(1, 90, n_claims),
            'patient_gender': np.random.choice(['Male', 'Female'], n_claims),
            'procedure_code': np.random.choice(procedure_codes, n_claims),
            'procedure_description': None,
            'encounter_class': np.random.choice(encounter_classes, n_claims),
        })
        # Map procedure description
        code_to_desc = dict(zip(procedure_codes, procedure_descs))
        fact_df['procedure_description'] = fact_df['procedure_code'].map(code_to_desc)
        # Age group
        fact_df['age_group'] = pd.cut(
            fact_df['patient_age'],
            bins=[0, 17, 34, 49, 64, 200],
            labels=age_groups
        ).astype(str)

        # ── agg_daily_claims_summary ────────────────────────────────────────
        daily_agg = fact_df.rename(columns={'encounter_date': 'summary_date'}).groupby(['summary_date', 'provider_state']).agg(
            total_claims=('claim_id', 'count'),
            unique_patients=('patient_id', 'nunique'),
            total_claim_amount=('claim_amount', 'sum'),
            avg_claim_amount=('claim_amount', 'mean'),
            total_approved_amount=('approved_amount', 'sum'),
            fraud_count=('fraud_indicator', 'sum'),
            denial_count=('denial_indicator', 'sum'),
            avg_fraud_score=('fraud_risk_score', 'mean'),
            avg_processing_days=('processing_time_days', 'mean'),
        ).reset_index()
        daily_agg['fraud_rate'] = (daily_agg['fraud_count'] / daily_agg['total_claims'] * 100).round(2)
        daily_agg['denial_rate'] = (daily_agg['denial_count'] / daily_agg['total_claims'] * 100).round(2)

        # ── agg_fraud_type_monthly ──────────────────────────────────────────
        fraud_df = fact_df[fact_df['fraud_type'].notna()].copy()
        fraud_monthly = fraud_df.groupby(['year', 'month', 'fraud_type', 'risk_level']).agg(
            claim_count=('claim_id', 'count'),
            total_claim_amount=('claim_amount', 'sum'),
            fraud_count=('fraud_indicator', 'sum'),
            avg_fraud_risk_score=('fraud_risk_score', 'mean'),
        ).reset_index()
        fraud_monthly['month_date'] = pd.to_datetime(
            fraud_monthly[['year', 'month']].assign(day=1))
        fraud_monthly['fraud_rate'] = (fraud_monthly['fraud_count'] / fraud_monthly['claim_count'] * 100).round(2)

        # ── agg_denial_reason ───────────────────────────────────────────────
        denial_df = fact_df[fact_df['denial_reason'].notna()].copy()
        denial_agg = denial_df.groupby(['year', 'month', 'denial_reason', 'provider_state']).agg(
            total_claims=('claim_id', 'count'),
            denied_count=('denial_indicator', 'sum'),
            total_claim_amount=('claim_amount', 'sum'),
            denied_amount=('claim_amount', lambda x: x.sum() - denial_df.loc[x.index, 'approved_amount'].sum()),
        ).reset_index()
        denial_agg['month_date'] = pd.to_datetime(denial_agg[['year', 'month']].assign(day=1))
        denial_agg['denial_rate'] = (denial_agg['denied_count'] / denial_agg['total_claims'] * 100).round(2)

        # ── dim_providers ───────────────────────────────────────────────────
        provider_dim = fact_df.groupby(['provider_id', 'provider_state']).agg(
            total_claims=('claim_id', 'count'),
            total_amount=('claim_amount', 'sum'),
            avg_amount=('claim_amount', 'mean'),
            fraud_count=('fraud_indicator', 'sum'),
        ).reset_index()
        provider_dim['fraud_rate'] = (provider_dim['fraud_count'] / provider_dim['total_claims'] * 100).round(2)
        provider_dim['risk_category'] = pd.cut(
            provider_dim['fraud_rate'],
            bins=[-1, 5, 10, 100],
            labels=['Low Risk', 'Medium Risk', 'High Risk']
        ).astype(str)

        # ── agg_patient_demographics ────────────────────────────────────────
        demo_agg = fact_df.groupby(['age_group', 'patient_gender', 'insurance_type']).agg(
            unique_patients=('patient_id', 'nunique'),
            total_claims=('claim_id', 'count'),
            total_claim_amount=('claim_amount', 'sum'),
            avg_claim_amount=('claim_amount', 'mean'),
            fraud_count=('fraud_indicator', 'sum'),
            denial_count=('denial_indicator', 'sum'),
            avg_fraud_risk_score=('fraud_risk_score', 'mean'),
        ).reset_index()
        demo_agg['fraud_rate'] = (demo_agg['fraud_count'] / demo_agg['total_claims'] * 100).round(2)
        demo_agg['denial_rate'] = (demo_agg['denial_count'] / demo_agg['total_claims'] * 100).round(2)

        # ── agg_procedure_fraud ─────────────────────────────────────────────
        proc_agg = fact_df.groupby(['procedure_code', 'procedure_description', 'encounter_class']).agg(
            total_claims=('claim_id', 'count'),
            unique_patients=('patient_id', 'nunique'),
            unique_providers=('provider_id', 'nunique'),
            total_claim_amount=('claim_amount', 'sum'),
            avg_claim_amount=('claim_amount', 'mean'),
            fraud_count=('fraud_indicator', 'sum'),
            denial_count=('denial_indicator', 'sum'),
            avg_fraud_risk_score=('fraud_risk_score', 'mean'),
            avg_processing_days=('processing_time_days', 'mean'),
        ).reset_index()
        proc_agg['fraud_rate'] = (proc_agg['fraud_count'] / proc_agg['total_claims'] * 100).round(2)
        proc_agg['denial_rate'] = (proc_agg['denial_count'] / proc_agg['total_claims'] * 100).round(2)

        # ── agg_submission_heatmap ──────────────────────────────────────────
        heatmap_agg = fact_df.groupby(['day_of_week', 'insurance_type', 'claim_type']).agg(
            total_claims=('claim_id', 'count'),
            fraud_count=('fraud_indicator', 'sum'),
            avg_fraud_risk_score=('fraud_risk_score', 'mean'),
            avg_submission_delay=('submission_delay_days', 'mean'),
            avg_processing_days=('processing_time_days', 'mean'),
            denial_count=('denial_indicator', 'sum'),
        ).reset_index()
        heatmap_agg['fraud_rate'] = (heatmap_agg['fraud_count'] / heatmap_agg['total_claims'] * 100).round(2)
        heatmap_agg['denial_rate'] = (heatmap_agg['denial_count'] / heatmap_agg['total_claims'] * 100).round(2)

        return {
            'fact': fact_df,
            'daily': daily_agg,
            'fraud_monthly': fraud_monthly,
            'denial': denial_agg,
            'providers': provider_dim,
            'demographics': demo_agg,
            'procedures': proc_agg,
            'heatmap': heatmap_agg,
        }

    with st.spinner("Loading Claims Fraud ETL data…"):
        fd = load_fraud_etl_data()

    fact_df        = fd['fact']
    daily_agg      = fd['daily']
    fraud_monthly  = fd['fraud_monthly']
    denial_agg     = fd['denial']
    provider_dim   = fd['providers']
    demo_agg       = fd['demographics']
    proc_agg       = fd['procedures']
    heatmap_agg    = fd['heatmap']

    # ── KPI Banner ───────────────────────────────────────────────────────────
    st.markdown("### 📊 Executive KPIs")
    k1, k2, k3, k4, k5 = st.columns(5)
    total_claims    = len(fact_df)
    total_fraud     = fact_df['fraud_indicator'].sum()
    fraud_rate_pct  = total_fraud / total_claims * 100
    total_amount    = fact_df['claim_amount'].sum()
    avg_risk_score  = fact_df['fraud_risk_score'].mean()
    denial_rate_pct = fact_df['denial_indicator'].sum() / total_claims * 100

    k1.metric("Total Claims",     f"{total_claims:,}")
    k2.metric("Fraud Cases",      f"{int(total_fraud):,}",   delta=f"{fraud_rate_pct:.1f}% rate",  delta_color="inverse")
    k3.metric("Total Claim Value",f"${total_amount/1e6:.2f}M")
    k4.metric("Avg Fraud Score",  f"{avg_risk_score:.3f}",   delta="0–1 scale",  delta_color="off")
    k5.metric("Denial Rate",      f"{denial_rate_pct:.1f}%", delta_color="inverse")

    st.markdown("---")

    # ── Section 1: Daily Claims Trend & Fraud Rate ───────────────────────────
    st.markdown("### 📈 Daily Claims Volume & Fraud Trend")
    col1, col2 = st.columns(2)

    with col1:
        daily_total = daily_agg.groupby('summary_date').agg(
            total_claims=('total_claims', 'sum'),
            fraud_count=('fraud_count', 'sum'),
            total_claim_amount=('total_claim_amount', 'sum')
        ).reset_index()
        daily_total['summary_date'] = pd.to_datetime(daily_total['summary_date'])
        daily_total['fraud_rate'] = (daily_total['fraud_count'] / daily_total['total_claims'] * 100).round(2)
        # Weekly resample for readability
        daily_total = daily_total.set_index('summary_date').resample('W').sum().reset_index()
        daily_total['fraud_rate'] = (daily_total['fraud_count'] / daily_total['total_claims'] * 100).round(2)

        fig_daily = go.Figure()
        fig_daily.add_trace(go.Bar(
            x=daily_total['summary_date'], y=daily_total['total_claims'],
            name='Total Claims', marker_color='#4C78A8', opacity=0.7
        ))
        fig_daily.add_trace(go.Scatter(
            x=daily_total['summary_date'], y=daily_total['fraud_rate'],
            name='Fraud Rate (%)', yaxis='y2',
            mode='lines+markers', line=dict(color='#E45756', width=2)
        ))
        fig_daily.update_layout(
            title='Weekly Claims Volume vs Fraud Rate',
            yaxis=dict(title='Claims Count'),
            yaxis2=dict(title='Fraud Rate (%)', overlaying='y', side='right', showgrid=False),
            legend=dict(x=0.01, y=0.99), hovermode='x unified',
            height=380
        )
        st.plotly_chart(fig_daily, use_container_width=True)

    with col2:
        # Risk level distribution donut
        risk_counts = fact_df['risk_level'].value_counts().reset_index()
        risk_counts.columns = ['risk_level', 'count']
        colors_risk = {'Low': '#54A24B', 'Medium': '#F58518', 'High': '#E45756', 'Critical': '#B10DC9'}
        fig_risk = px.pie(
            risk_counts, values='count', names='risk_level',
            title='Claim Risk Level Distribution',
            color='risk_level', color_discrete_map=colors_risk,
            hole=0.45
        )
        fig_risk.update_traces(textposition='outside', textinfo='percent+label')
        fig_risk.update_layout(showlegend=True, height=380)
        st.plotly_chart(fig_risk, use_container_width=True)

    st.markdown("---")

    # ── Section 2: Fraud Type Monthly Trend ─────────────────────────────────
    st.markdown("### 🚨 Fraud Type Monthly Trend")
    col1, col2 = st.columns(2)

    with col1:
        fraud_pivot = fraud_monthly.groupby(['month_date', 'fraud_type'])['claim_count'].sum().reset_index()
        fig_fraud_trend = px.bar(
            fraud_pivot, x='month_date', y='claim_count', color='fraud_type',
            title='Monthly Fraud Cases by Type',
            labels={'claim_count': 'Fraud Cases', 'month_date': 'Month', 'fraud_type': 'Fraud Type'},
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig_fraud_trend.update_layout(barmode='stack', height=380, hovermode='x unified')
        st.plotly_chart(fig_fraud_trend, use_container_width=True)

    with col2:
        fraud_type_total = fraud_monthly.groupby('fraud_type').agg(
            total_cases=('fraud_count', 'sum'),
            avg_risk_score=('avg_fraud_risk_score', 'mean'),
            total_amount=('total_claim_amount', 'sum')
        ).reset_index().sort_values('total_cases', ascending=True)

        fig_fraud_hbar = px.bar(
            fraud_type_total, y='fraud_type', x='total_cases',
            orientation='h',
            color='avg_risk_score',
            color_continuous_scale='Reds',
            title='Total Fraud Cases by Type (colored by Avg Risk Score)',
            labels={'total_cases': 'Total Cases', 'fraud_type': 'Fraud Type', 'avg_risk_score': 'Avg Risk Score'}
        )
        fig_fraud_hbar.update_layout(height=380)
        st.plotly_chart(fig_fraud_hbar, use_container_width=True)

    st.markdown("---")

    # ── Section 3: Denial Reason Analysis ───────────────────────────────────
    st.markdown("### ❌ Claim Denial Analysis")
    col1, col2 = st.columns(2)

    with col1:
        denial_by_reason = denial_agg.groupby('denial_reason').agg(
            denied_count=('denied_count', 'sum'),
            total_claim_amount=('total_claim_amount', 'sum'),
            denied_amount=('denied_amount', 'sum')
        ).reset_index().sort_values('denied_count', ascending=False)

        fig_denial = px.bar(
            denial_by_reason, x='denial_reason', y='denied_count',
            color='denied_amount',
            color_continuous_scale='OrRd',
            title='Denial Count & Financial Impact by Reason',
            labels={'denied_count': 'Denied Claims', 'denial_reason': 'Denial Reason', 'denied_amount': 'Denied Amount ($)'}
        )
        fig_denial.update_layout(height=380, xaxis_tickangle=-25)
        st.plotly_chart(fig_denial, use_container_width=True)

    with col2:
        # Denial monthly trend top-3 reasons
        top3_reasons = denial_by_reason['denial_reason'].head(3).tolist()
        denial_trend = denial_agg[denial_agg['denial_reason'].isin(top3_reasons)]\
            .groupby(['month_date', 'denial_reason'])['denied_count'].sum().reset_index()

        fig_denial_trend = px.line(
            denial_trend, x='month_date', y='denied_count', color='denial_reason',
            title='Monthly Denial Trend (Top 3 Reasons)',
            labels={'denied_count': 'Denied Claims', 'month_date': 'Month', 'denial_reason': 'Reason'},
            markers=True, color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig_denial_trend.update_layout(height=380, hovermode='x unified')
        st.plotly_chart(fig_denial_trend, use_container_width=True)

    st.markdown("---")

    # ── Section 4: Provider Risk Analysis ───────────────────────────────────
    st.markdown("### 🏥 Provider Risk Analysis")
    col1, col2 = st.columns(2)

    with col1:
        # Top 20 providers by fraud rate bubble chart
        top_prov = provider_dim.nlargest(20, 'fraud_rate')
        fig_prov_bubble = px.scatter(
            top_prov, x='total_claims', y='fraud_rate',
            size='total_amount', color='risk_category',
            hover_name='provider_id',
            hover_data={'provider_state': True, 'fraud_count': True},
            color_discrete_map={'Low Risk': '#54A24B', 'Medium Risk': '#F58518', 'High Risk': '#E45756'},
            title='Top 20 High-Risk Providers (bubble = claim value)',
            labels={'total_claims': 'Total Claims', 'fraud_rate': 'Fraud Rate (%)'}
        )
        fig_prov_bubble.update_layout(height=400)
        st.plotly_chart(fig_prov_bubble, use_container_width=True)

    with col2:
        # Provider fraud rate by state choropleth — bar fallback
        state_fraud = provider_dim.groupby('provider_state').agg(
            total_claims=('total_claims', 'sum'),
            fraud_count=('fraud_count', 'sum'),
            total_amount=('total_amount', 'sum')
        ).reset_index()
        state_fraud['fraud_rate'] = (state_fraud['fraud_count'] / state_fraud['total_claims'] * 100).round(2)
        state_fraud = state_fraud.sort_values('fraud_rate', ascending=False)

        fig_state = px.bar(
            state_fraud, x='provider_state', y='fraud_rate',
            color='fraud_rate', color_continuous_scale='RdYlGn_r',
            title='Fraud Rate by Provider State',
            labels={'fraud_rate': 'Fraud Rate (%)', 'provider_state': 'State'}
        )
        fig_state.update_layout(height=400)
        st.plotly_chart(fig_state, use_container_width=True)

    st.markdown("---")

    # ── Section 5: Patient Demographics & Fraud ──────────────────────────────
    st.markdown("### 👥 Patient Demographics & Fraud Behaviour")
    col1, col2, col3 = st.columns(3)

    with col1:
        age_fraud = demo_agg.groupby('age_group').agg(
            fraud_rate=('fraud_rate', 'mean'),
            total_claims=('total_claims', 'sum'),
            avg_claim_amount=('avg_claim_amount', 'mean')
        ).reset_index()
        age_order = ['Child (<18)', 'Young Adult (18-34)', 'Middle Age (35-49)', 'Senior (50-64)', 'Elderly (65+)']
        age_fraud['age_group'] = pd.Categorical(age_fraud['age_group'], categories=age_order, ordered=True)
        age_fraud = age_fraud.sort_values('age_group')

        fig_age = px.bar(
            age_fraud, x='age_group', y='fraud_rate',
            color='avg_claim_amount', color_continuous_scale='Blues',
            title='Fraud Rate by Age Group',
            labels={'fraud_rate': 'Fraud Rate (%)', 'age_group': 'Age Group', 'avg_claim_amount': 'Avg Claim ($)'}
        )
        fig_age.update_layout(height=370, xaxis_tickangle=-20)
        st.plotly_chart(fig_age, use_container_width=True)

    with col2:
        gender_ins = demo_agg.groupby(['patient_gender', 'insurance_type']).agg(
            fraud_rate=('fraud_rate', 'mean'),
            total_claims=('total_claims', 'sum')
        ).reset_index()
        fig_gender = px.bar(
            gender_ins, x='insurance_type', y='fraud_rate',
            color='patient_gender', barmode='group',
            title='Fraud Rate by Insurance Type & Gender',
            labels={'fraud_rate': 'Fraud Rate (%)', 'insurance_type': 'Insurance', 'patient_gender': 'Gender'},
            color_discrete_sequence=['#4C78A8', '#F58518']
        )
        fig_gender.update_layout(height=370, xaxis_tickangle=-15)
        st.plotly_chart(fig_gender, use_container_width=True)

    with col3:
        ins_denial = demo_agg.groupby('insurance_type').agg(
            denial_rate=('denial_rate', 'mean'),
            fraud_rate=('fraud_rate', 'mean'),
        ).reset_index()
        fig_ins = go.Figure()
        fig_ins.add_trace(go.Bar(x=ins_denial['insurance_type'], y=ins_denial['fraud_rate'],
                                 name='Fraud Rate', marker_color='#E45756'))
        fig_ins.add_trace(go.Bar(x=ins_denial['insurance_type'], y=ins_denial['denial_rate'],
                                 name='Denial Rate', marker_color='#72B7B2'))
        fig_ins.update_layout(barmode='group', title='Fraud vs Denial Rate by Insurance',
                              yaxis_title='Rate (%)', height=370, hovermode='x unified')
        st.plotly_chart(fig_ins, use_container_width=True)

    st.markdown("---")

    # ── Section 6: Procedure-Level Fraud ────────────────────────────────────
    st.markdown("### 🩺 High-Risk Procedure Analysis")
    col1, col2 = st.columns(2)

    with col1:
        proc_sorted = proc_agg.sort_values('fraud_rate', ascending=False)
        fig_proc = px.scatter(
            proc_sorted, x='avg_claim_amount', y='fraud_rate',
            size='total_claims', color='encounter_class',
            hover_name='procedure_description',
            hover_data={'procedure_code': True, 'avg_fraud_risk_score': ':.3f'},
            title='Procedure Fraud Rate vs Avg Claim Amount',
            labels={'avg_claim_amount': 'Avg Claim Amount ($)', 'fraud_rate': 'Fraud Rate (%)',
                    'encounter_class': 'Encounter Class'},
            color_discrete_sequence=px.colors.qualitative.T10
        )
        fig_proc.update_layout(height=400)
        st.plotly_chart(fig_proc, use_container_width=True)

    with col2:
        proc_top = proc_agg.nlargest(10, 'fraud_count')
        fig_proc_bar = go.Figure()
        fig_proc_bar.add_trace(go.Bar(
            x=proc_top['procedure_description'], y=proc_top['fraud_count'],
            name='Fraud Count', marker_color='#E45756'
        ))
        fig_proc_bar.add_trace(go.Bar(
            x=proc_top['procedure_description'], y=proc_top['denial_count'],
            name='Denial Count', marker_color='#72B7B2'
        ))
        fig_proc_bar.update_layout(
            barmode='group', title='Top 10 Procedures: Fraud & Denial Counts',
            xaxis_tickangle=-30, height=400, hovermode='x unified',
            yaxis_title='Count'
        )
        st.plotly_chart(fig_proc_bar, use_container_width=True)

    st.markdown("---")

    # ── Section 7: Submission Heatmap ───────────────────────────────────────
    st.markdown("### 🗓️ Fraud Submission Behaviour Heatmap")
    col1, col2 = st.columns(2)

    with col1:
        # Robustly handle different types of day_of_week (Date, String, or Integer)
        import pandas.api.types as ptypes
        import datetime
        dtype = heatmap_agg['day_of_week']
        if ptypes.is_datetime64_any_dtype(dtype) or (ptypes.is_object_dtype(dtype) and len(dtype) > 0 and isinstance(dtype.iloc[0], datetime.date)):
            heatmap_agg['day_of_week'] = pd.to_datetime(dtype).dt.day_name().str[:3]
        elif ptypes.is_numeric_dtype(dtype):
            day_map = {0: 'Sun', 1: 'Mon', 2: 'Tue', 3: 'Wed', 4: 'Thu', 5: 'Fri', 6: 'Sat', 7: 'Sun'}
            heatmap_agg['day_of_week'] = dtype.map(day_map)
        else:
            heatmap_agg['day_of_week'] = dtype.astype(str).str[:3].str.capitalize()

        day_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
        heat_pivot = heatmap_agg.groupby(['day_of_week', 'insurance_type'])['fraud_rate'].mean().reset_index()
        heat_pivot['day_of_week'] = pd.Categorical(heat_pivot['day_of_week'], categories=day_order, ordered=True)
        heat_matrix = heat_pivot.pivot(index='insurance_type', columns='day_of_week', values='fraud_rate')
        heat_matrix = heat_matrix.reindex(columns=day_order)

        fig_heat = px.imshow(
            heat_matrix,
            color_continuous_scale='YlOrRd',
            aspect='auto',
            title='Avg Fraud Rate (%) by Day × Insurance Type',
            labels=dict(color='Fraud Rate (%)')
        )
        fig_heat.update_layout(height=350)
        st.plotly_chart(fig_heat, use_container_width=True)

    with col2:
        # Submission delay distribution
        fig_delay = px.histogram(
            fact_df, x='submission_delay_days',
            color='risk_level',
            nbins=30,
            title='Claim Submission Delay Distribution by Risk Level',
            labels={'submission_delay_days': 'Submission Delay (days)', 'count': 'Count', 'risk_level': 'Risk Level'},
            color_discrete_map={'Low': '#54A24B', 'Medium': '#F58518', 'High': '#E45756', 'Critical': '#B10DC9'},
            barmode='overlay', opacity=0.7
        )
        fig_delay.update_layout(height=350, hovermode='x unified')
        st.plotly_chart(fig_delay, use_container_width=True)

    st.markdown("---")

    # ── Section 8: Fraud Risk Score Distribution ─────────────────────────────
    st.markdown("### 🎯 Fraud Risk Score Deep Dive")
    col1, col2 = st.columns(2)

    with col1:
        fig_violin = px.violin(
            fact_df, y='fraud_risk_score', x='risk_level',
            color='risk_level', box=True, points=False,
            category_orders={'risk_level': ['Low', 'Medium', 'High', 'Critical']},
            color_discrete_map={'Low': '#54A24B', 'Medium': '#F58518', 'High': '#E45756', 'Critical': '#B10DC9'},
            title='Fraud Risk Score Distribution by Risk Level'
        )
        fig_violin.update_layout(height=370, showlegend=False)
        st.plotly_chart(fig_violin, use_container_width=True)

    with col2:
        # Processing time vs fraud risk score
        sample_df = fact_df.sample(min(800, len(fact_df)), random_state=42)
        fig_scatter = px.scatter(
            sample_df, x='processing_time_days', y='fraud_risk_score',
            color='claim_status', opacity=0.6,
            title='Processing Time vs Fraud Risk Score (sampled)',
            labels={'processing_time_days': 'Processing Time (days)', 'fraud_risk_score': 'Fraud Risk Score',
                    'claim_status': 'Claim Status'},
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_scatter.update_layout(height=370)
        st.plotly_chart(fig_scatter, use_container_width=True)

    # ── Raw Data Explorer ────────────────────────────────────────────────────
    with st.expander("📋 Explore Raw Model Data"):
        tab_f, tab_p, tab_pr, tab_proc, tab_h = st.tabs([
            "fact_claims", "agg_patient_demographics", "dim_providers", "agg_procedure_fraud", "agg_submission_heatmap"
        ])
        with tab_f:
            st.markdown("**fact_claims** (sample 20 rows)")
            cols_to_show = [c for c in ['claim_id', 'provider_id', 'patient_id', 'claim_amount',
                                   'approved_amount', 'fraud_indicator', 'denial_indicator',
                                   'fraud_risk_score', 'risk_level', 'claim_status',
                                   'encounter_date'] if c in fact_df.columns]
            st.dataframe(fact_df[cols_to_show].head(20), use_container_width=True)
        with tab_p:
            st.markdown("**agg_patient_demographics**")
            st.dataframe(demo_agg.head(20), use_container_width=True)
        with tab_pr:
            st.markdown("**dim_providers** (provider dimension)")
            st.dataframe(provider_dim.sort_values('fraud_rate', ascending=False).head(20), use_container_width=True)
        with tab_proc:
            st.markdown("**agg_procedure_fraud**")
            st.dataframe(proc_agg.sort_values('fraud_rate', ascending=False), use_container_width=True)
        with tab_h:
            st.markdown("**agg_submission_heatmap**")
            st.dataframe(heatmap_agg.sort_values('fraud_rate', ascending=False).head(20), use_container_width=True)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #888; padding: 2rem;'>
    <p>Built with Streamlit &bull; Data Engineering Portfolio 2025</p>
    <p>&#128161; All data visualizations are interactive - hover, zoom, and explore!</p>
</div>
""", unsafe_allow_html=True)
