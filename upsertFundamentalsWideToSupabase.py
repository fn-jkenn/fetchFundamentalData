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

# Disable SSL verification for this script (unsafe on untrusted networks)
os.environ["PYTHONHTTPSVERIFY"] = "0"
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
    Return a configured Supabase client.

    NOTE:
    - If you already have get_supabase_client defined elsewhere,
      delete this function and import yours instead, e.g.:

        from my_supabase_client import get_supabase_client
    """
    try:
        from supabase import create_client  # type: ignore
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

    return create_client(url, key)


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

    Column names in Supabase should match DataFrame columns exactly:
      - ticker (text)
      - fiscal_year (int)
      - period (text)
      - plus any metric columns (numeric)
    """
    sb = get_supabase_client()

    df = wide_df.copy()

    # --- Normalize column names from CSV to expected schema ---
    # Expecting: ticker, fiscal_year, period
    df = df.rename(
        columns={
            "Ticker": "ticker",
            "Fiscal Year": "fiscal_year",
            "Period": "period",
            "Filing Date": "filing_date",  # Add filing date column
        }
    )

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
            print(f"Error upserting batch {batch_num}: {exc}")
            raise

    print(f"Successfully upserted {total_records} records to {TABLE}")


if __name__ == "__main__":
    import pandas as pd

    # Hardcoded path (relative to this script)
    csv_path = "fundamentals_wide.csv"

    print(f"Loading wide fundamentals from: {csv_path}")
    df_wide = pd.read_csv(csv_path)
    upsert_fundamentals_wide_to_supabase(df_wide)

