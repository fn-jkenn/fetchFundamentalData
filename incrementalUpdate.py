from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable

# Set SSL verification to false by default (can be overridden by environment variable)
if "SEC_API_VERIFY_SSL" not in os.environ:
    os.environ["SEC_API_VERIFY_SSL"] = "false"

import pandas as pd

from fetchAllData import COMPANIES, get_all_fundamentals

FUNDAMENTALS_CSV = Path("fundamentals_long.csv")
FUNDAMENTALS_WIDE_CSV = Path("fundamentals_wide.csv")
# Primary key for deduplication: Ticker + Fiscal Year + Period
# This ensures one row per company/period combination (keeps latest filing)
PRIMARY_KEY_COLUMNS: Iterable[str] = (
    "Ticker",
    "Fiscal Year",
    "Period",
)

# Full key columns for detailed deduplication (includes metric-specific info)
KEY_COLUMNS: Iterable[str] = (
    "Ticker",
    "GAAPTag",
    "Fiscal Year",
    "Period",
    "Filing Date",
    "Form",
    "Unit",
)


def load_existing() -> pd.DataFrame:
    """Load existing CSV and remove any duplicates found in it using primary key."""
    if FUNDAMENTALS_CSV.exists():
        df = pd.read_csv(FUNDAMENTALS_CSV)
        if not df.empty:
            # Remove duplicates using primary key (keeps latest filing for each period)
            before = len(df)
            df = deduplicate_by_primary_key(df)
            after = len(df)
            if before != after:
                print(f"⚠ Found and removed {before - after} duplicate periods from existing CSV (kept latest filings)")
                # Save the cleaned CSV
                df.to_csv(FUNDAMENTALS_CSV, index=False)
        return df
    return pd.DataFrame()


def build_primary_key(frame: pd.DataFrame) -> pd.Series:
    """
    Build primary key using Ticker + Fiscal Year + Period.
    This identifies unique reporting periods.
    """
    if frame.empty:
        return pd.Series(dtype="string")
    
    subset = frame.loc[:, PRIMARY_KEY_COLUMNS].copy()
    
    # Normalize Fiscal Year: convert to int if numeric, then to string
    if "Fiscal Year" in subset.columns:
        fiscal_numeric = pd.to_numeric(subset["Fiscal Year"], errors="coerce")
        fiscal_str = fiscal_numeric.astype(object)
        mask_valid = fiscal_numeric.notna()
        fiscal_str.loc[mask_valid] = fiscal_numeric.loc[mask_valid].astype(int).astype(str)
        fiscal_str.loc[~mask_valid] = "<NA>"
        subset["Fiscal Year"] = fiscal_str
    
    # Fill NaN and convert all other columns to string, then strip whitespace
    for col in subset.columns:
        if col != "Fiscal Year":
            subset[col] = subset[col].fillna("<NA>").astype(str).str.strip()
    
    return subset.agg("||".join, axis=1)


def build_keys(frame: pd.DataFrame) -> pd.Series:
    """
    Create a stable key per row for deduplication.
    Uses full key including metric (GAAPTag) to identify unique metric/period combinations.
    """
    if frame.empty:
        return pd.Series(dtype="string")
    
    subset = frame.loc[:, KEY_COLUMNS].copy()
    
    # Normalize Fiscal Year: convert to int if numeric, then to string
    # This ensures CSV (float) and API (int) both become the same string
    if "Fiscal Year" in subset.columns:
        # Convert to numeric, handling both float and int
        fiscal_numeric = pd.to_numeric(subset["Fiscal Year"], errors="coerce")
        # Convert to int where possible, then to string
        fiscal_str = fiscal_numeric.astype(object)  # Convert to object first to avoid dtype warning
        mask_valid = fiscal_numeric.notna()
        fiscal_str.loc[mask_valid] = fiscal_numeric.loc[mask_valid].astype(int).astype(str)
        fiscal_str.loc[~mask_valid] = "<NA>"
        subset["Fiscal Year"] = fiscal_str
    
    # Fill NaN and convert all other columns to string, then strip whitespace
    for col in subset.columns:
        if col != "Fiscal Year":  # Already handled above
            subset[col] = subset[col].fillna("<NA>").astype(str).str.strip()
    
    return subset.agg("||".join, axis=1)


def get_latest_filing_dates(existing: pd.DataFrame) -> dict[str, str]:
    """
    Get the latest filing date for each ticker from existing data.
    Returns dict mapping ticker to latest filing date (YYYY-MM-DD format).
    """
    if existing.empty or "Filing Date" not in existing.columns or "Ticker" not in existing.columns:
        return {}
    
    # Convert Filing Date to datetime for comparison
    existing_copy = existing.copy()
    existing_copy["Filing Date"] = pd.to_datetime(existing_copy["Filing Date"], errors="coerce")
    
    # Get latest filing date per ticker
    latest_dates = existing_copy.groupby("Ticker")["Filing Date"].max()
    
    # Convert back to string format (YYYY-MM-DD)
    result = {}
    for ticker, date in latest_dates.items():
        if pd.notna(date):
            result[ticker] = date.strftime("%Y-%m-%d")
    
    return result


def filter_to_new_filings(fresh: pd.DataFrame, latest_dates: dict[str, str]) -> pd.DataFrame:
    """
    Filter fresh data to only include filings newer than what's already in CSV.
    Only keeps rows with Filing Date > latest date for that ticker.
    """
    if fresh.empty or "Filing Date" not in fresh.columns or "Ticker" not in fresh.columns:
        return fresh
    
    if not latest_dates:
        # No existing data, return all fresh data
        return fresh
    
    fresh_copy = fresh.copy()
    filing_date_dt = pd.to_datetime(fresh_copy["Filing Date"], errors="coerce")
    
    # Create mask for rows that are newer than latest date for that ticker
    mask = pd.Series(False, index=fresh_copy.index)
    
    for ticker, latest_date_str in latest_dates.items():
        latest_date = pd.to_datetime(latest_date_str)
        ticker_mask = (fresh_copy["Ticker"] == ticker) & (filing_date_dt > latest_date)
        mask |= ticker_mask
    
    # Also include tickers that don't exist in existing data
    new_tickers = ~fresh_copy["Ticker"].isin(latest_dates.keys())
    mask |= new_tickers
    
    # Filter and return (Filing Date remains as original string format)
    return fresh_copy.loc[mask].reset_index(drop=True)


def get_new_rows(existing: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out rows that already exist in the CSV.
    First filters to only newer filings, then removes duplicates by key.
    Returns only rows that are truly new (not duplicates).
    """
    if fresh.empty:
        return fresh

    # Step 1: Filter to only filings newer than what's in CSV
    latest_dates = get_latest_filing_dates(existing)
    if latest_dates:
        print(f"\nFiltering to only newer filings (latest dates per ticker in CSV):")
        for ticker, date in sorted(latest_dates.items())[:10]:
            print(f"  {ticker}: {date}")
        if len(latest_dates) > 10:
            print(f"  ... and {len(latest_dates) - 10} more")
        
        before_filter = len(fresh)
        fresh = filter_to_new_filings(fresh, latest_dates)
        after_filter = len(fresh)
        print(f"\nDate filtering: {before_filter} rows → {after_filter} rows "
              f"({before_filter - after_filter} older filings filtered out)")
    
    if fresh.empty:
        return fresh

    fresh = fresh.copy()
    fresh["_key"] = build_keys(fresh)

    if existing.empty:
        return fresh.drop(columns="_key")

    # Step 2: Build keys from existing data and filter duplicates
    existing_keys = set(build_keys(existing))
    
    # Only keep rows whose keys are NOT in existing_keys
    mask = ~fresh["_key"].isin(existing_keys)
    new_rows = fresh.loc[mask].drop(columns="_key")
    
    # Double-check: verify no duplicates slipped through
    if not new_rows.empty:
        new_keys = build_keys(new_rows)
        if new_keys.duplicated().any():
            # Remove duplicates from new_rows itself
            new_rows["_key"] = new_keys
            new_rows = new_rows.drop_duplicates(subset="_key", keep="last")
            new_rows = new_rows.drop(columns="_key")
    
    return new_rows


def log_new_data(new_rows: pd.DataFrame) -> None:
    """Log detailed information about newly added data."""
    if new_rows.empty:
        return
    
    print("\n" + "=" * 80)
    print("NEW DATA SUMMARY")
    print("=" * 80)
    
    # Summary by ticker
    ticker_counts = new_rows["Ticker"].value_counts().sort_index()
    print(f"\nNew rows by Ticker ({len(ticker_counts)} companies):")
    for ticker, count in ticker_counts.items():
        print(f"  {ticker}: {count} rows")
    
    # Summary by metric
    metric_counts = new_rows["Metric"].value_counts()
    print(f"\nNew rows by Metric ({len(metric_counts)} metrics):")
    for metric, count in metric_counts.items():
        print(f"  {metric}: {count} rows")
    
    # Summary by fiscal year
    if "Fiscal Year" in new_rows.columns:
        fy_counts = new_rows["Fiscal Year"].value_counts().sort_index(ascending=False)
        print(f"\nNew rows by Fiscal Year ({len(fy_counts)} fiscal years):")
        for fy, count in fy_counts.items():
            print(f"  {fy}: {count} rows")
    
    # Unique filings summary
    filing_cols = ["Ticker", "Fiscal Year", "Period", "Filing Date", "Form"]
    if all(col in new_rows.columns for col in filing_cols):
        unique_filings = new_rows[filing_cols].drop_duplicates()
        print(f"\nUnique filings added: {len(unique_filings)}")
        print("\nAll filing details:")
        for _, row in unique_filings.iterrows():
            print(f"  {row['Ticker']} | FY{row['Fiscal Year']} {row['Period']} | "
                  f"{row['Filing Date']} | {row['Form']}")
    
    # All new rows
    print(f"\nAll new rows ({len(new_rows)} total):")
    sample_cols = ["Ticker", "Metric", "Fiscal Year", "Period", "Filing Date", "Value"]
    display_cols = [col for col in sample_cols if col in new_rows.columns]
    for idx, row in new_rows[display_cols].iterrows():
        values_str = " | ".join([f"{col}: {row[col]}" for col in display_cols])
        print(f"  {values_str}")
    
    print("=" * 80 + "\n")


def deduplicate_by_primary_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate using primary key (Ticker + Fiscal Year + Period).
    For each unique period, keeps only the latest filing (by Filing Date).
    This ensures no duplicate reporting periods while preserving all metrics.
    """
    if df.empty or "Filing Date" not in df.columns:
        return df
    
    df = df.copy()
    
    # Build primary key
    df["_primary_key"] = build_primary_key(df)
    
    # Convert Filing Date to datetime for comparison
    df["Filing Date"] = pd.to_datetime(df["Filing Date"], errors="coerce")
    
    # For each primary key (ticker/fiscal year/period), keep only rows with latest filing date
    # Sort by Filing Date descending, then drop duplicates keeping first (latest)
    df = df.sort_values(["Filing Date", "GAAPTag"], ascending=[False, True], na_position="last")
    df = df.drop_duplicates(subset=["_primary_key", "GAAPTag"], keep="first")
    
    # Convert Filing Date back to string
    df["Filing Date"] = df["Filing Date"].dt.strftime("%Y-%m-%d")
    
    # Drop temporary column
    df = df.drop(columns=["_primary_key"])
    
    return df.reset_index(drop=True)


def upsert_data(existing: pd.DataFrame, new_rows: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Upsert functionality: Insert new rows or update existing ones.
    
    For each row in new_rows:
    - If it's a new ticker/fiscal year/period/metric combination → INSERT
    - If it exists but has a newer filing date → UPDATE (replace old with new)
    - If it exists with same or older filing date → SKIP (keep existing)
    
    Returns:
        - Updated DataFrame
        - Dictionary with stats: {'inserted': count, 'updated': count, 'skipped': count}
    """
    if new_rows.empty:
        return existing, {'inserted': 0, 'updated': 0, 'skipped': 0}
    
    if existing.empty:
        return new_rows, {'inserted': len(new_rows), 'updated': 0, 'skipped': 0}
    
    # Prepare dataframes for comparison
    existing_copy = existing.copy()
    new_copy = new_rows.copy()
    
    # Build keys for both dataframes
    existing_keys = build_keys(existing_copy)
    new_keys = build_keys(new_copy)
    existing_copy["_key"] = existing_keys
    new_copy["_key"] = new_keys
    
    # Convert Filing Date to datetime for comparison
    existing_copy["_filing_date_dt"] = pd.to_datetime(existing_copy["Filing Date"], errors="coerce")
    new_copy["_filing_date_dt"] = pd.to_datetime(new_copy["Filing Date"], errors="coerce")
    
    # Create a lookup: for each key, get the latest filing date in existing
    existing_lookup = existing_copy.groupby("_key")["_filing_date_dt"].max().to_dict()
    
    # Categorize new rows
    inserted_mask = ~new_copy["_key"].isin(existing_lookup.keys())
    existing_mask = new_copy["_key"].isin(existing_lookup.keys())
    
    # For existing keys, check if new filing date is newer
    update_mask = pd.Series(False, index=new_copy.index)
    skip_mask = pd.Series(False, index=new_copy.index)
    
    for idx in new_copy[existing_mask].index:
        key = new_copy.loc[idx, "_key"]
        new_filing_date = new_copy.loc[idx, "_filing_date_dt"]
        existing_filing_date = existing_lookup[key]
        
        if pd.notna(new_filing_date) and pd.notna(existing_filing_date):
            if new_filing_date > existing_filing_date:
                update_mask.loc[idx] = True
            else:
                skip_mask.loc[idx] = True
        elif pd.notna(new_filing_date):
            # New has date, existing doesn't → update
            update_mask.loc[idx] = True
        else:
            # No date or both missing → skip
            skip_mask.loc[idx] = True
    
    # Build stats
    stats = {
        'inserted': inserted_mask.sum(),
        'updated': update_mask.sum(),
        'skipped': skip_mask.sum()
    }
    
    # Remove rows that will be updated from existing
    if update_mask.any():
        updated_keys = set(new_copy.loc[update_mask, "_key"])
        existing_copy = existing_copy[~existing_copy["_key"].isin(updated_keys)]
    
    # Combine: existing (minus updated) + inserted + updated
    result_parts = []
    
    # Add existing rows (excluding those that will be updated)
    if not existing_copy.empty:
        existing_final = existing_copy.drop(columns=["_key", "_filing_date_dt"])
        result_parts.append(existing_final)
    
    # Add inserted rows
    if inserted_mask.any():
        inserted_df = new_copy.loc[inserted_mask].drop(columns=["_key", "_filing_date_dt"])
        result_parts.append(inserted_df)
    
    # Add updated rows
    if update_mask.any():
        updated_df = new_copy.loc[update_mask].drop(columns=["_key", "_filing_date_dt"])
        result_parts.append(updated_df)
    
    # Combine all
    if result_parts:
        result = pd.concat(result_parts, ignore_index=True)
    else:
        result = existing
    
    # Final deduplication by primary key (in case of edge cases)
    result = deduplicate_by_primary_key(result)
    
    return result, stats


def append_and_save(existing: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Upsert new rows: Insert new data or update existing data if filing date is newer.
    Uses primary key (Ticker + Fiscal Year + Period + Metric) for upsert logic.
    """
    if new_rows.empty:
        return existing
    
    # Use upsert functionality
    updated, stats = upsert_data(existing, new_rows)
    
    # Log upsert statistics
    print(f"\n📊 Upsert Summary:")
    if stats['inserted'] > 0:
        print(f"  ✓ Inserted: {stats['inserted']} new rows")
    if stats['updated'] > 0:
        print(f"  🔄 Updated: {stats['updated']} rows with newer filing data (amended filings)")
    if stats['skipped'] > 0:
        print(f"  ⏭️  Skipped: {stats['skipped']} rows (same/older filing dates, keeping existing)")
    
    total_processed = stats['inserted'] + stats['updated'] + stats['skipped']
    if total_processed > 0:
        print(f"  📈 Total processed: {total_processed} rows")

    updated = updated.sort_values(
        ["Ticker", "Fiscal Year", "Period", "Filing Date"],
        na_position="last",
    )
    updated.to_csv(FUNDAMENTALS_CSV, index=False)
    return updated


def rebuild_wide(long_df: pd.DataFrame) -> None:
    if long_df.empty:
        print("No long-format data available; wide CSV not regenerated.")
        return

    df_wide = long_df.pivot_table(
        index=["Ticker", "Fiscal Year", "Period"],
        columns="Metric",
        values="Value",
        aggfunc="last",
    )
    df_wide.to_csv(FUNDAMENTALS_WIDE_CSV)
    print(f"Rebuilt wide-format dataset -> {FUNDAMENTALS_WIDE_CSV}")


def main():
    existing = load_existing()
    print(f"Loaded {len(existing)} existing rows from {FUNDAMENTALS_CSV}")
    
    # Show latest data in CSV
    if not existing.empty and "Filing Date" in existing.columns:
        latest_date = pd.to_datetime(existing["Filing Date"], errors="coerce").max()
        if pd.notna(latest_date):
            print(f"Latest filing date in CSV: {latest_date.strftime('%Y-%m-%d')}")
    
    fresh = get_all_fundamentals(COMPANIES)
    print(f"Fetched {len(fresh)} total rows from SEC API (all historical data)")

    new_rows = get_new_rows(existing, fresh)

    if new_rows.empty:
        print("\n✓ No new fundamentals to append. CSV remains unchanged.")
        print("  (All fetched data already exists in the CSV)")
        updated_long = existing
    else:
        print(f"\n✓ Found {len(new_rows)} new rows to append (duplicates filtered out)")
        log_new_data(new_rows)
        updated_long = append_and_save(existing, new_rows)
        print(f"✓ Successfully appended {len(new_rows)} new fundamental rows to {FUNDAMENTALS_CSV}.")
        print(f"✓ Total rows in CSV: {len(updated_long)}")

    rebuild_wide(updated_long)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Update cancelled by user.")

