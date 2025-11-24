import streamlit as st
import pandas as pd
from typing import Optional

# All database credentials and configurations are managed through Streamlit secrets.
# For more details, see: https://docs.streamlit.io/develop/concepts/connections/secrets-management

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
        conn = st.connection("snowflake")
        
        # Execute the query and return as a DataFrame
        df = conn.query(query, params=params)
        
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