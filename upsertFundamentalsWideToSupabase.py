"""
Upsert wide-format fundamentals data into Supabase.

Usage:
    1. Ensure you have a Supabase client helper like:

        from supabase import create_client, Client

        def get_supabase_client() -> Client:
            url = os.environ["SUPABASE_URL"]
            key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
            return create_client(url, key)

       (Or import your existing get_supabase_client from another module.)

    2. Load your wide CSV and call:

        import pandas as pd
        from upsertFundamentalsWideToSupabase import upsert_fundamentals_wide_to_supabase

        df_wide = pd.read_csv("fundamentals_wide.csv")
        upsert_fundamentals_wide_to_supabase(df_wide)
"""

from __future__ import annotations

import os
import warnings

# Disable SSL verification for this script (unsafe on untrusted networks)
os.environ["PYTHONHTTPSVERIFY"] = "0"
# Suppress SSL warnings from httpx/urllib3
warnings.filterwarnings("ignore", message="Unverified HTTPS request")
warnings.filterwarnings("ignore", category=UserWarning, module="urllib3")
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

# --------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------

TABLE = "fundamentals_wide"  # Supabase table name


# --------------------------------------------------------------------
# ENV LOADER (.env)
# --------------------------------------------------------------------

def load_env_from_dotenv() -> None:
    """
    Load SUPABASE_URL and keys from a local .env file if present.
    Does NOT override variables already set in the environment.
    """
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return

    try:
        with env_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except OSError:
        # If .env can't be read, just skip; env vars may still be set externally.
        pass


# Load .env as soon as the module is imported
load_env_from_dotenv()


# --------------------------------------------------------------------
# SUPABASE CLIENT
# --------------------------------------------------------------------

def get_supabase_client():
    """
    Return a configured Supabase client with SSL verification disabled.

    NOTE:
    - If you already have get_supabase_client defined elsewhere,
      delete this function and import yours instead, e.g.:

        from my_supabase_client import get_supabase_client
    """
    try:
        from supabase import create_client, ClientOptions  # type: ignore
        import httpx  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "supabase-py is not installed. Install with:\n"
            "  pip install supabase-py"
        ) from exc

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) "
            "must be set in environment variables."
        )

    # Create httpx client with SSL verification disabled (for corporate proxy)
    # WARNING: This is insecure on untrusted networks
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass
    
    http_client = httpx.Client(verify=False)
    
    # Create ClientOptions with custom httpx client
    # Note: parameter name is 'httpx_client', not 'http_client'
    options = ClientOptions(httpx_client=http_client)
    
    # Create Supabase client with custom httpx client
    return create_client(url, key, options=options)


# --------------------------------------------------------------------
# SCHEMA GENERATION (Helper)
# --------------------------------------------------------------------

def generate_table_sql(table_name: str, columns: list[str]) -> str:
    """
    Generate SQL CREATE TABLE statement for Supabase.
    
    Args:
        table_name: Name of the table
        columns: List of column names (normalized)
    
    Returns:
        SQL CREATE TABLE statement
    """
    sql_lines = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
    
    # Primary key columns first
    pk_cols = ['ticker', 'fiscal_year', 'period']
    for col in pk_cols:
        if col in columns:
            if col == 'fiscal_year':
                sql_lines.append(f"  {col} INTEGER NOT NULL,")
            else:
                sql_lines.append(f"  {col} TEXT NOT NULL,")
    
    # Filing date
    if 'filing_date' in columns:
        sql_lines.append(f"  filing_date DATE,")
    
    # Metric columns (all numeric)
    metric_cols = [c for c in columns if c not in pk_cols + ['filing_date']]
    for col in metric_cols:
        sql_lines.append(f"  {col} NUMERIC,")
    
    # Primary key constraint
    sql_lines.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")
    sql_lines.append(");")
    
    return "\n".join(sql_lines)


# --------------------------------------------------------------------
# UPSERT LOGIC
# --------------------------------------------------------------------

def _clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure all values are JSON-serializable and NaNs/inf are converted to None."""
    cleaned: Dict[str, Any] = {}
    for key, value in record.items():
        if pd.isna(value) or value is None:
            cleaned[key] = None
        elif isinstance(value, (float, np.floating)) and (np.isnan(value) or np.isinf(value)):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def upsert_fundamentals_wide_to_supabase(wide_df: pd.DataFrame) -> None:
    """
    Upsert wide-format fundamentals into Supabase.

    Assumes Supabase table `fundamentals_wide` with primary key:
      (ticker, fiscal_year, period)

    Column names are automatically normalized:
      - "Ticker" -> "ticker"
      - "Fiscal Year" -> "fiscal_year"
      - "Period" -> "period"
      - "Filing Date" -> "filing_date"
      - Metric columns: spaces/& replaced with underscores, lowercase
        (e.g., "Cash & Cash Equivalents" -> "cash_and_cash_equivalents")

    IMPORTANT: Your Supabase table schema must use these normalized column names.
    """
    sb = get_supabase_client()

    df = wide_df.copy()

    # --- Normalize ALL column names from CSV to expected schema ---
    # Convert to lowercase, replace spaces with underscores, remove special chars
    def normalize_column_name(col: str) -> str:
        """Normalize column name: lowercase, spaces to underscores, remove special chars."""
        # First handle known mappings
        if col == "Ticker":
            return "ticker"
        elif col == "Fiscal Year":
            return "fiscal_year"
        elif col == "Period":
            return "period"
        elif col == "Filing Date":
            return "filing_date"
        else:
            # For metric columns: lowercase, replace spaces/& with underscores
            normalized = col.lower().replace(" ", "_").replace("&", "and").replace("/", "_")
            # Remove any remaining special characters except underscores
            normalized = "".join(c if c.isalnum() or c == "_" else "" for c in normalized)
            # Collapse multiple underscores
            while "__" in normalized:
                normalized = normalized.replace("__", "_")
            return normalized.strip("_")

    # Rename all columns
    column_mapping = {col: normalize_column_name(col) for col in df.columns}
    df = df.rename(columns=column_mapping)
    
    # Log column normalization for verification
    changed_cols = {k: v for k, v in column_mapping.items() if k != v}
    if changed_cols:
        print(f"Normalized {len(changed_cols)} column names (e.g., {list(changed_cols.items())[:3]})")
    
    # Print all column names that will be sent to Supabase (for schema verification)
    print(f"\nColumns being sent to Supabase ({len(df.columns)} total):")
    print(f"  Primary keys: ticker, fiscal_year, period, filing_date")
    print(f"  Metric columns: {', '.join([c for c in df.columns if c not in ['ticker', 'fiscal_year', 'period', 'filing_date']][:10])}...")
    print(f"\n⚠️  Make sure your Supabase table '{TABLE}' has all these columns!")

    # --- Key normalization (for consistent PK matching) ---

    # ticker as stripped string
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip()

    # fiscal_year as int where possible
    if "fiscal_year" in df.columns:
        df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")

    # period as stripped string
    if "period" in df.columns:
        df["period"] = df["period"].astype(str).str.strip()

    # Drop rows with null PK parts
    initial_count = len(df)
    df = df[df["ticker"].notna() & df["fiscal_year"].notna() & df["period"].notna()].copy()
    if len(df) < initial_count:
        print(
            f"Warning: Removed {initial_count - len(df)} records with NULL "
            "ticker/fiscal_year/period values"
        )

    # --- Clean metric columns to be JSON-serializable ---

    records = [_clean_record(r) for r in df.to_dict(orient="records")]

    # --- Batch upsert ---

    batch_size = 500
    total_records = len(records)
    total_batches = (total_records + batch_size - 1) // batch_size
    print(f"Upserting {total_records} records in {total_batches} batch(es) into {TABLE}...")

    for i in range(0, total_records, batch_size):
        chunk = records[i : i + batch_size]
        batch_num = (i // batch_size) + 1

        try:
            # Composite PK: (ticker, fiscal_year, period)
            sb.table(TABLE).upsert(
                chunk,
                on_conflict="ticker,fiscal_year,period",
            ).execute()

            if batch_num % 10 == 0 or batch_num == total_batches:
                print(f"Upserted batch {batch_num}/{total_batches} ({len(chunk)} records)")
        except Exception as exc:  # pragma: no cover
            print(f"\n❌ Error upserting batch {batch_num}: {exc}")
            if "Could not find" in str(exc) and "column" in str(exc):
                print(f"\n💡 SCHEMA MISMATCH DETECTED!")
                print(f"   The Supabase table '{TABLE}' is missing some columns.")
                print(f"   Expected columns: {', '.join(df.columns[:10])}...")
                print(f"\n   To fix this:")
                print(f"   1. Go to your Supabase dashboard → SQL Editor")
                print(f"   2. Run this SQL to create the table:")
                print(f"\n{generate_table_sql(TABLE, list(df.columns))}")
            raise

    print(f"Successfully upserted {total_records} records to {TABLE}")


if __name__ == "__main__":
    import sys
    import pandas as pd

    # Hardcoded path (relative to this script)
    csv_path = "fundamentals_wide.csv"

    print(f"Loading wide fundamentals from: {csv_path}")
    df_wide = pd.read_csv(csv_path)
    
    # Check for --generate-sql flag
    if len(sys.argv) > 1 and sys.argv[1] == "--generate-sql":
        print("\n" + "=" * 80)
        print("SQL CREATE TABLE STATEMENT FOR SUPABASE")
        print("=" * 80)
        print("\nCopy and paste this into your Supabase SQL Editor:\n")
        
        # Normalize columns (same logic as in upsert function)
        def normalize_column_name(col: str) -> str:
            if col == "Ticker":
                return "ticker"
            elif col == "Fiscal Year":
                return "fiscal_year"
            elif col == "Period":
                return "period"
            elif col == "Filing Date":
                return "filing_date"
            else:
                normalized = col.lower().replace(" ", "_").replace("&", "and").replace("/", "_")
                normalized = "".join(c if c.isalnum() or c == "_" else "" for c in normalized)
                while "__" in normalized:
                    normalized = normalized.replace("__", "_")
                return normalized.strip("_")
        
        normalized_cols = [normalize_column_name(col) for col in df_wide.columns]
        print(generate_table_sql(TABLE, normalized_cols))
        print("\n" + "=" * 80)
        sys.exit(0)
    
    upsert_fundamentals_wide_to_supabase(df_wide)

