import streamlit as st
import pandas as pd
from typing import Optional
import psycopg2
import psycopg2.extras

# All database credentials and configurations are managed through Streamlit secrets.
# For more details, see: https://docs.streamlit.io/develop/concepts/connections/secrets-management


@st.cache_resource
def get_supabase_connection():
    """
    Create and cache a single Supabase (PostgreSQL) connection.
    Connection will be reused across queries.

    Returns:
        psycopg2.Connection: Cached connection
    """
    try:
        sb = st.secrets["supabase"]
        conn = psycopg2.connect(
            host=sb["db_host"],
            port=int(sb["db_port"]),
            dbname=sb["db_name"],
            user=sb["db_user"],
            password=sb["db_password"],
            connect_timeout=30,
            sslmode="require",
        )
        conn.autocommit = True  # read-only dashboard; no transaction management needed
        return conn
    except Exception as e:
        st.error(f"❌ Failed to connect to Supabase: {str(e)}")
        return None


def execute_supabase_query(
    query: str, params: Optional[tuple] = None
) -> Optional[pd.DataFrame]:
    """
    Execute a SQL query on Supabase and return results as a pandas DataFrame.
    Uses a cached connection for better performance.

    Args:
        query (str): SQL query to execute.
        params (Optional[tuple]): Query parameters for parameterized queries.

    Returns:
        Optional[pd.DataFrame]: Query results as DataFrame or None if execution fails.
    """
    conn = get_supabase_connection()
    if conn is None:
        return None

    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)

        return df if not df.empty else pd.DataFrame()

    except Exception as e:
        st.error(f"❌ Query execution failed on Supabase: {str(e)}")
        # Clear cache to force reconnection on next call
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        get_supabase_connection.clear()
        return None

    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


# ── Query functions — identical signatures to redshift_connection.py ──────────


def load_fda_volume_analysis() -> Optional[pd.DataFrame]:
    """Load FDA volume analysis data for dashboard."""
    query = """
        SELECT
            COUNT(*) AS total_reports,
            SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) AS serious_reports,
            SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) AS fatal_reports,
            COUNT(DISTINCT occurcountry) AS countries_affected,
            DATE_TRUNC('month', receivedate) AS report_month
        FROM public.dim_reports
        GROUP BY DATE_TRUNC('month', receivedate)
        ORDER BY report_month
        LIMIT 1000
    """
    return execute_supabase_query(query)


def load_top_drugs_analysis() -> Optional[pd.DataFrame]:
    """Load top drugs analysis data for dashboard."""
    query = """
        SELECT
            d.medicinalproduct,
            d.generic_name,
            COUNT(*) AS report_count,
            SUM(CASE WHEN fr.reactionoutcome = '1' THEN 1 ELSE 0 END) AS recovered_count,
            SUM(CASE WHEN fr.reactionoutcome = '5' THEN 1 ELSE 0 END) AS fatal_count,
            AVG(p.patient_age) AS avg_age,
            SUM(CASE WHEN p.patient_sex = '1' THEN 1 ELSE 0 END) AS male_count,
            SUM(CASE WHEN p.patient_sex = '2' THEN 1 ELSE 0 END) AS female_count
        FROM public.fact_drugs d
        JOIN public.dim_reports p ON d.safetyreportid = p.safetyreportid
        LEFT JOIN public.fact_reactions fr ON d.safetyreportid = fr.safetyreportid
        WHERE d.medicinalproduct IS NOT NULL
          AND d.medicinalproduct != ''
        GROUP BY d.medicinalproduct, d.generic_name
        ORDER BY report_count DESC
        LIMIT 10
    """
    return execute_supabase_query(query)


def load_global_adverse_distribution() -> Optional[pd.DataFrame]:
    """Load global adverse event distribution data for dashboard."""
    query = """
        SELECT
            occurcountry,
            COUNT(*) AS report_count,
            SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) AS serious_reports,
            SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) AS fatal_reports,
            COUNT(DISTINCT CASE WHEN serious = '1' THEN safetyreportid END) AS unique_serious_reports
        FROM public.dim_reports
        WHERE occurcountry IS NOT NULL
          AND occurcountry != ''
        GROUP BY occurcountry
        ORDER BY report_count DESC
        LIMIT 20
    """
    return execute_supabase_query(query)


def load_fda_safety_trends() -> Optional[pd.DataFrame]:
    """Load FDA safety report trends data for dashboard."""
    query = """
        SELECT
            receivedate,
            COUNT(*) AS total_reports,
            SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) AS serious_reports,
            SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) AS fatal_reports,
            SUM(CASE WHEN seriousnesshospitalization = '1' THEN 1 ELSE 0 END) AS hospitalization_reports
        FROM public.dim_reports
        GROUP BY receivedate
        ORDER BY receivedate DESC
        LIMIT 500
    """
    return execute_supabase_query(query)


def load_top_suspect_drugs() -> Optional[pd.DataFrame]:
    """Load top suspect drugs data for dashboard."""
    query = """
        SELECT
            d.medicinalproduct,
            d.generic_name,
            d.drugcharacterization,
            COUNT(*) AS report_count,
            SUM(CASE WHEN fr.reactionoutcome = '1' THEN 1 ELSE 0 END) AS recovered_count,
            SUM(CASE WHEN fr.reactionoutcome = '5' THEN 1 ELSE 0 END) AS fatal_count,
            COUNT(DISTINCT d.safetyreportid) AS unique_reports
        FROM public.fact_drugs d
        LEFT JOIN public.fact_reactions fr ON d.safetyreportid = fr.safetyreportid
        WHERE d.drugcharacterization = '1'
          AND d.medicinalproduct IS NOT NULL
          AND d.medicinalproduct != ''
        GROUP BY d.medicinalproduct, d.generic_name, d.drugcharacterization
        ORDER BY report_count DESC
        LIMIT 10
    """
    return execute_supabase_query(query)
