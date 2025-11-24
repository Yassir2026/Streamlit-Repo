import streamlit as st
import pandas as pd
from typing import Optional

# All database credentials and configurations are managed through Streamlit secrets.
# For more details, see: https://docs.streamlit.io/develop/concepts/connections/secrets-management

def execute_redshift_query(query: str, params: Optional[tuple] = None) -> Optional[pd.DataFrame]:
    """
    Execute a SQL query on Redshift and return results as a pandas DataFrame.
    Uses Streamlit's connection management for robustness and performance.
    
    Args:
        query (str): SQL query to execute.
        params (Optional[tuple]): Query parameters for parameterized queries.
        
    Returns:
        Optional[pd.DataFrame]: Query results as DataFrame or None if execution fails.
    """
    try:
        # Establish connection using st.connection
        conn = st.connection("redshift", type="sql")
        
        # Execute the query and return as a DataFrame
        df = conn.query(query, params=params)
        
        if df is not None and not df.empty:
            # st.info(f"✅ Query executed successfully: {len(df)} rows returned.")
            return df
        else:
            # st.warning("⚠️ Query returned no data.")
            return pd.DataFrame()  # Return an empty DataFrame for consistency
            
    except Exception as e:
        st.error(f"❌ Query execution failed on Redshift: {str(e)}")
        return None

def load_fda_volume_analysis() -> Optional[pd.DataFrame]:
    """
    Load FDA volume analysis data for dashboard.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing volume analysis data or None if loading fails
    """
    query = """
        select
            COUNT(*) as total_reports,
            SUM(case when serious = '1' then 1 else 0 end) as serious_reports,
            SUM(case when seriousnessdeath = '1' then 1 else 0 end) as fatal_reports,
            COUNT(distinct occurcountry) as countries_affected,
            DATE_TRUNC('month', receivedate) as report_month
        from
            public.dim_reports
        group by
            DATE_TRUNC('month', receivedate)
        order by
            report_month
        LIMIT 1000
    """
    
    return execute_redshift_query(query)

def load_top_drugs_analysis() -> Optional[pd.DataFrame]:
    """
    Load top drugs analysis data for dashboard.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing top drugs analysis data or None if loading fails
    """
    query = """
        SELECT
            d.medicinalproduct,
            d.generic_name,
            COUNT(*) as report_count,
            SUM(CASE WHEN fr.reactionoutcome = '1' THEN 1 ELSE 0 END) as recovered_count,
            SUM(CASE WHEN fr.reactionoutcome = '5' THEN 1 ELSE 0 END) as fatal_count,
            AVG(p.patient_age) as avg_age,
            SUM(CASE WHEN p.patient_sex = '1' THEN 1 ELSE 0 END) as male_count,
            SUM(CASE WHEN p.patient_sex = '2' THEN 1 ELSE 0 END) as female_count
        FROM
            public.fact_drugs d
        JOIN
            public.dim_reports p ON d.safetyreportid = p.safetyreportid
        LEFT JOIN
            public.fact_reactions fr ON d.safetyreportid = fr.safetyreportid
        WHERE
            d.medicinalproduct IS NOT NULL
            AND d.medicinalproduct != ''
        GROUP BY
            d.medicinalproduct, d.generic_name
        ORDER BY
            report_count DESC
        LIMIT 10
    """
    
    return execute_redshift_query(query)

def load_global_adverse_distribution() -> Optional[pd.DataFrame]:
    """
    Load global adverse event distribution data for dashboard.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing global distribution data or None if loading fails
    """
    query = """
        SELECT
            occurcountry,
            COUNT(*) as report_count,
            SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_reports,
            SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) as fatal_reports,
            COUNT(DISTINCT CASE WHEN serious = '1' THEN safetyreportid END) as unique_serious_reports
        FROM
            public.dim_reports
        WHERE
            occurcountry IS NOT NULL
            AND occurcountry != ''
        GROUP BY
            occurcountry
        ORDER BY
            report_count DESC
        LIMIT 20
    """
    
    return execute_redshift_query(query)

def load_fda_safety_trends() -> Optional[pd.DataFrame]:
    """
    Load FDA safety report trends data for dashboard.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing safety trends data or None if loading fails
    """
    query = """
        select
            receivedate,
            COUNT(*) as total_reports,
            SUM(case when serious = '1' then 1 else 0 end) as serious_reports,
            SUM(case when seriousnessdeath = '1' then 1 else 0 end) as fatal_reports,
            SUM(case when seriousnesshospitalization = '1' then 1 else 0 end) as hospitalization_reports
        from
            public.dim_reports
        group by
            receivedate
        ORDER BY
            receivedate DESC
        LIMIT 500
    """
    
    return execute_redshift_query(query)

def load_top_suspect_drugs() -> Optional[pd.DataFrame]:
    """
    Load top suspect drugs data for dashboard.
    
    Returns:
        Optional[pd.DataFrame]: DataFrame containing top suspect drugs data or None if loading fails
    """
    query = """
        SELECT
            d.medicinalproduct,
            d.generic_name,
            d.drugcharacterization,
            COUNT(*) as report_count,
            SUM(CASE WHEN fr.reactionoutcome = '1' THEN 1 ELSE 0 END) as recovered_count,
            SUM(CASE WHEN fr.reactionoutcome = '5' THEN 1 ELSE 0 END) as fatal_count,
            COUNT(DISTINCT d.safetyreportid) as unique_reports
        FROM
            public.fact_drugs d
        LEFT JOIN
            public.fact_reactions fr ON d.safetyreportid = fr.safetyreportid
        WHERE
            d.drugcharacterization = '1'  -- Suspect drugs only
            AND d.medicinalproduct IS NOT NULL
            AND d.medicinalproduct != ''
        GROUP BY
            d.medicinalproduct, d.generic_name, d.drugcharacterization
        ORDER BY
            report_count DESC
        LIMIT 10
    """
    
    return execute_redshift_query(query)