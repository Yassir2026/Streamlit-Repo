import streamlit as st
import pandas as pd
from typing import Optional
import snowflake.connector

# All database credentials and configurations are managed through Streamlit secrets.
# For more details, see: https://docs.streamlit.io/develop/concepts/connections/secrets-management

@st.cache_resource
def get_snowflake_connection():
    """建立 Snowflake 连接
    
    支持两种认证方式（在 secrets.toml 中配置其中一种）：
    1. Programmatic Access Token (PAT) — 推荐：设置 token 字段
    2. Key Pair — 旧方式：设置 private_key 字段
    """
    sf_secrets = st.secrets["snowflake"]
    
    # --- 方式 1: Programmatic Access Token (PAT) ---
    # 需要 snowflake-connector-python >= 3.12.0
    # authenticator 必须是 'PROGRAMMATIC_ACCESS_TOKEN'，不是 'oauth'
    if "token" in sf_secrets:
        return snowflake.connector.connect(
            user=sf_secrets["user"],
            account=sf_secrets["account"],
            authenticator="PROGRAMMATIC_ACCESS_TOKEN",
            token=sf_secrets["token"],
            warehouse=sf_secrets.get("warehouse"),
            database=sf_secrets.get("database"),
            schema=sf_secrets.get("schema"),
            role=sf_secrets.get("role"),
        )
    
    # --- 方式 2: Key Pair 认证（fallback）---
    from cryptography.hazmat.primitives import serialization
    private_key_str = sf_secrets["private_key"]
    p_key = serialization.load_pem_private_key(
        private_key_str.encode("utf-8"),
        password=None,
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        user=sf_secrets["user"],
        account=sf_secrets["account"],
        private_key=pkb,
        warehouse=sf_secrets.get("warehouse"),
        database=sf_secrets.get("database"),
        schema=sf_secrets.get("schema"),
        role=sf_secrets.get("role"),
    )

def execute_snowflake_query(query: str, params: Optional[tuple] = None) -> Optional[pd.DataFrame]:
    """
    Execute a SQL query on Snowflake and return results as a pandas DataFrame.
    Uses Streamlit's connection management for robustness and performance.
    
    Args:
        query (str): SQL query to execute.
        params (Optional[tuple]): Query parameters for parameterized queries.
        
    Returns:
        Optional[pd.DataFrame]: Query results as DataFrame or None if execution fails.
    """
    try:
        # Establish connection using st.connection's native Snowflake handler
        # conn = st.connection("snowflake")
        conn = get_snowflake_connection()
        
        # Execute the query and return as a DataFrame
        # df = conn.query(query, params=params)
        if params:
            cursor = conn.cursor()
            cursor.execute(query, params)
            df = cursor.fetch_pandas_all()
            cursor.close()
        else:
            df = pd.read_sql(query, conn)
        
        if df is not None and not df.empty:
            # st.info(f"✅ Query executed successfully: {len(df)} rows returned.")
            return df
        else:
            # st.warning("⚠️ Query returned no data.")
            return pd.DataFrame()  # Return an empty DataFrame for consistency
            
    except Exception as e:
        st.error(f"❌ Query execution failed on Snowflake: {str(e)}")
        return None

def load_executive_dashboard_data() -> Optional[pd.DataFrame]:
    """
    Load executive dashboard data from Snowflake.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing executive dashboard data or None if loading fails
    """
    query = """
    SELECT * FROM HEALTHCARE_INSURANCE_DB.ANALYTICS.VW_EXECUTIVE_DASHBOARD
    ORDER BY MONTH_DATE DESC
    LIMIT 1000
    """
    
    return execute_snowflake_query(query)

def load_patient_demographics_data() -> Optional[pd.DataFrame]:
    """
    Load patient demographics data from Snowflake.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing patient demographics data or None if loading fails
    """
    query = """
    SELECT * FROM HEALTHCARE_INSURANCE_DB.ANALYTICS.VW_PATIENT_DEMOGRAPHICS
    LIMIT 5000
    """
    
    return execute_snowflake_query(query)

def load_encounter_analysis_data() -> Optional[pd.DataFrame]:
    """
    Load encounter analysis data from Snowflake.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing encounter analysis data or None if loading fails
    """
    query = """
    SELECT * FROM HEALTHCARE_INSURANCE_DB.ANALYTICS.VW_ENCOUNTER_ANALYSIS
    WHERE ENCOUNTER_DATE >= DATEADD(year, -2, CURRENT_DATE())
    LIMIT 10000
    """
    
    return execute_snowflake_query(query)

def load_claims_management_data() -> Optional[pd.DataFrame]:
    """
    Load claims management data from Snowflake.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing claims management data or None if loading fails
    """
    query = """
    SELECT * FROM HEALTHCARE_INSURANCE_DB.ANALYTICS.VW_CLAIMS_MANAGEMENT
    WHERE SERVICE_DATE >= DATEADD(year, -2, CURRENT_DATE())
    LIMIT 5000
    """
    
    return execute_snowflake_query(query)

def load_financial_insights_data() -> Optional[pd.DataFrame]:
    """
    Load financial insights data from Snowflake.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing financial insights data or None if loading fails
    """
    query = """
    SELECT * FROM HEALTHCARE_INSURANCE_DB.ANALYTICS.VW_FINANCIAL_INSIGHTS
    LIMIT 5000
    """
    
    return execute_snowflake_query(query)

def load_geographic_analysis_data() -> Optional[pd.DataFrame]:
    """
    Load geographic analysis data from Snowflake.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing geographic analysis data or None if loading fails
    """
    query = """
    SELECT * FROM HEALTHCARE_INSURANCE_DB.ANALYTICS.VW_GEOGRAPHIC_ANALYSIS
    ORDER BY PATIENT_COUNT DESC
    LIMIT 1000
    """
    
    return execute_snowflake_query(query)

def load_fraud_fact_claims() -> Optional[pd.DataFrame]:
    print("🚀 [Claims Fraud ETL] Fetching FACT_CLAIMS from Snowflake...")
    query = "SELECT * FROM CLAIMS_PROCESSING_DB.FACTS.FACT_CLAIMS ORDER BY ENCOUNTER_DATE DESC LIMIT 5000"
    return execute_snowflake_query(query)

def load_fraud_daily_summary() -> Optional[pd.DataFrame]:
    print("🚀 [Claims Fraud ETL] Fetching AGG_DAILY_CLAIMS_SUMMARY from Snowflake...")
    query = "SELECT * FROM CLAIMS_PROCESSING_DB.AGGREGATES.AGG_DAILY_CLAIMS_SUMMARY ORDER BY SUMMARY_DATE DESC"
    return execute_snowflake_query(query)

def load_fraud_monthly_trend() -> Optional[pd.DataFrame]:
    print("🚀 [Claims Fraud ETL] Fetching AGG_FRAUD_TYPE_MONTHLY from Snowflake...")
    query = "SELECT * FROM CLAIMS_PROCESSING_DB.AGGREGATES.AGG_FRAUD_TYPE_MONTHLY"
    return execute_snowflake_query(query)

def load_fraud_denial_reason() -> Optional[pd.DataFrame]:
    print("🚀 [Claims Fraud ETL] Fetching AGG_DENIAL_REASON from Snowflake...")
    query = "SELECT * FROM CLAIMS_PROCESSING_DB.AGGREGATES.AGG_DENIAL_REASON"
    return execute_snowflake_query(query)

def load_fraud_providers() -> Optional[pd.DataFrame]:
    print("🚀 [Claims Fraud ETL] Fetching DIM_PROVIDERS from Snowflake...")
    query = "SELECT * FROM CLAIMS_PROCESSING_DB.DIMENSIONS.DIM_PROVIDERS"
    return execute_snowflake_query(query)

def load_fraud_demographics() -> Optional[pd.DataFrame]:
    print("🚀 [Claims Fraud ETL] Fetching AGG_PATIENT_DEMOGRAPHICS from Snowflake...")
    query = "SELECT * FROM CLAIMS_PROCESSING_DB.AGGREGATES.AGG_PATIENT_DEMOGRAPHICS"
    return execute_snowflake_query(query)

def load_fraud_procedures() -> Optional[pd.DataFrame]:
    print("🚀 [Claims Fraud ETL] Fetching AGG_PROCEDURE_FRAUD from Snowflake...")
    query = "SELECT * FROM CLAIMS_PROCESSING_DB.AGGREGATES.AGG_PROCEDURE_FRAUD"
    return execute_snowflake_query(query)

def load_fraud_heatmap() -> Optional[pd.DataFrame]:
    print("🚀 [Claims Fraud ETL] Fetching AGG_SUBMISSION_HEATMAP from Snowflake...")
    query = "SELECT * FROM CLAIMS_PROCESSING_DB.AGGREGATES.AGG_SUBMISSION_HEATMAP"
    return execute_snowflake_query(query)