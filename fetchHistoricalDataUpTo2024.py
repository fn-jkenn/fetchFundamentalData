"""
Fetch historical fundamental data up to 2024 only.
Use this to populate the CSV with historical data for testing incrementalUpdate.py.
"""
import os
import time
from typing import Dict, List
from datetime import datetime

# Set SSL verification to false by default (can be overridden by environment variable)
if "SEC_API_VERIFY_SSL" not in os.environ:
    os.environ["SEC_API_VERIFY_SSL"] = "false"

import pandas as pd
import requests
from requests import Response
from requests.exceptions import RequestException

HEADERS = {
    "User-Agent": os.getenv(
        "SEC_API_USER_AGENT",
        "contact@example.com"  # Replace with your email per SEC guidance
    )
}

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3
REQUEST_TIMEOUT = 30  # seconds

VERIFY_SSL = os.getenv("SEC_API_VERIFY_SSL", "false").lower() not in {"0", "false", "no"}
CUSTOM_CA_BUNDLE = os.getenv("SEC_API_CA_BUNDLE")
VERIFY_PARAM = CUSTOM_CA_BUNDLE if (CUSTOM_CA_BUNDLE and VERIFY_SSL) else VERIFY_SSL

PREFERRED_UNITS = {
    "EarningsPerShareBasic": ["USD/shares"],
    "EarningsPerShareDiluted": ["USD/shares"],
    "CommonStockSharesOutstanding": ["shares", "pure"],
}

# --------------------------------------------------------
# 1. YOUR COMPANIES (CIK ALREADY PROVIDED)
# --------------------------------------------------------

COMPANIES = {
    "AXON": "0001069183",
    "GE": "0000040545",
    "GD": "0000040533",
    "HWM": "0000004281",
    "LHX": "0000202058",
    "LMT": "0000936468",
    "NOC": "0001133421",
    "RTX": "0000101829",
    "BA": "0000012927",
    "TDG": "0001260221"
}

# --------------------------------------------------------
# 2. EXPANDED FUNDAMENTAL METRICS (US-GAAP TAGS)
# --------------------------------------------------------

GAAP_TAGS = {
    # ========= Income Statement =========
    "Revenues": "Revenue",
    "CostOfRevenue": "Cost of Revenue",
    "GrossProfit": "Gross Profit",
    "OperatingExpenses": "Operating Expenses",
    "OperatingIncomeLoss": "Operating Income",
    "NonoperatingIncomeExpense": "Nonoperating Income",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxes": "Income Before Taxes",
    "NetIncomeLoss": "Net Income",
    "EBIT": "EBIT",
    "EBITDA": "EBITDA",   # Sometimes custom — may not always appear
    "IncomeLossFromContinuingOperations": "Income from Continuing Ops",

    # ========= Balance Sheet =========
    "Assets": "Total Assets",
    "Liabilities": "Total Liabilities",
    "StockholdersEquity": "Shareholder Equity",
    "LiabilitiesAndStockholdersEquity": "Liabilities & Equity",
    "CashAndCashEquivalentsAtCarryingValue": "Cash & Cash Equivalents",
    "AccountsReceivableNetCurrent": "Accounts Receivable",
    "InventoryNet": "Inventory",
    "AccountsPayableCurrent": "Accounts Payable",
    "LongTermDebt": "Long Term Debt",
    "ShortTermBorrowings": "Short Term Borrowings",

    # ========= Cash Flow =========
    "NetCashProvidedByUsedInOperatingActivities": "Cash From Operations",
    "NetCashProvidedByUsedInInvestingActivities": "Cash From Investing",
    "NetCashProvidedByUsedInFinancingActivities": "Cash From Financing",
    "PaymentsForProceedsFromLongTermDebt": "Long Term Debt Changes",
    "PaymentsOfDividends": "Dividends Paid",
    "RepurchaseOfCommonStock": "Share Buybacks",

    # ========= Per Share =========
    "EarningsPerShareBasic": "EPS Basic",
    "EarningsPerShareDiluted": "EPS Diluted",
    "CommonStockSharesOutstanding": "Shares Outstanding",

    # ========= Operating Metrics =========
    "OperatingIncomeLoss": "Operating Income",
    "ResearchAndDevelopmentExpense": "R&D Expense",
    "SellingGeneralAndAdministrativeExpense": "SG&A Expense",
}

# Cutoff date: only include data up to 2024-12-31
CUTOFF_DATE = datetime(2024, 12, 31)

# --------------------------------------------------------
# 3. FETCH COMPANY FUNDAMENTALS (FILTERED TO 2024)
# --------------------------------------------------------

def get_company_fundamentals(cik: str, ticker: str) -> List[Dict]:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik.zfill(10)}.json"
    response = fetch_with_retry(url)

    if response is None:
        print(f"[ERROR] Giving up on {ticker} after retries\n")
        return []

    data = response.json()
    facts = data.get("facts", {}).get("us-gaap", {})

    rows = []

    for tag, metric_name in GAAP_TAGS.items():
        if tag not in facts:
            continue

        units = facts[tag].get("units", {})
        unit_key = pick_unit(tag, units)
        if not unit_key:
            continue

        for entry in units[unit_key]:
            filing_date_str = entry.get("end")
            fiscal_year = entry.get("fy")
            
            # Filter: only include entries with filing date on or before 2024-12-31
            # Skip entries without a valid date or with dates after cutoff
            if not filing_date_str:
                continue  # Skip entries without filing date
            
            try:
                filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
                if filing_date > CUTOFF_DATE:
                    continue  # Skip entries after 2024-12-31
            except (ValueError, TypeError):
                # If date parsing fails, skip it (be strict)
                continue
            
            # Also filter out Fiscal Year 2025 (even if filing date is in 2024)
            if fiscal_year is not None:
                try:
                    fy_int = int(fiscal_year)
                    if fy_int >= 2025:
                        continue  # Skip fiscal year 2025 and later
                except (ValueError, TypeError):
                    pass  # If fiscal year can't be parsed, include it
            
            rows.append({
                "Ticker": ticker,
                "CIK": cik,
                "Metric": metric_name,
                "GAAPTag": tag,
                "Value": entry.get("val"),
                "Fiscal Year": entry.get("fy"),
                "Period": entry.get("fp"),
                "Filing Date": filing_date_str,
                "Form": entry.get("form"),
                "Unit": unit_key
            })

    return rows


def fetch_with_retry(url: str) -> Response | None:
    """Fetch SEC endpoint with retries and configurable SSL handling."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
                verify=VERIFY_PARAM,
            )
            if response.status_code == 200:
                return response

            print(f"[WARN] {url} returned {response.status_code} (attempt {attempt})")
        except RequestException as err:
            print(f"[WARN] Request error on attempt {attempt}: {err}")

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_SECONDS)
    return None


def pick_unit(tag: str, units: Dict) -> str | None:
    """Return the best unit key for a GAAP tag."""
    preferred_units = PREFERRED_UNITS.get(tag, [])
    for preferred in preferred_units:
        if preferred in units:
            return preferred

    if "USD" in units:
        return "USD"

    if units:
        return next(iter(units))

    return None


# --------------------------------------------------------
# 4. PROCESS ALL COMPANIES
# --------------------------------------------------------

def get_all_fundamentals(companies):
    all_data = []
    for ticker, cik in companies.items():
        print(f"Fetching: {ticker} ({cik})")
        rows = get_company_fundamentals(cik, ticker)
        all_data.extend(rows)
        print(f"  → Found {len(rows)} rows (filtered to ≤ 2024-12-31)")
    
    # Convert to DataFrame and apply additional filter to be absolutely sure
    df = pd.DataFrame(all_data)
    
    if not df.empty:
        before_filter = len(df)
        
        # Filter by Filing Date
        if "Filing Date" in df.columns:
            df["Filing Date"] = pd.to_datetime(df["Filing Date"], errors="coerce")
            df = df[df["Filing Date"] <= CUTOFF_DATE]
        
        # Filter by Fiscal Year (exclude 2025 and later)
        if "Fiscal Year" in df.columns:
            # Convert Fiscal Year to numeric
            df["Fiscal Year"] = pd.to_numeric(df["Fiscal Year"], errors="coerce")
            df = df[(df["Fiscal Year"].isna()) | (df["Fiscal Year"] < 2025)]
        
        after_filter = len(df)
        
        if before_filter != after_filter:
            print(f"\n⚠ Post-filter: Removed {before_filter - after_filter} rows with dates/FY > 2024")
        
        # Convert back to string format
        if "Filing Date" in df.columns:
            df["Filing Date"] = df["Filing Date"].dt.strftime("%Y-%m-%d")
    
    return df


# --------------------------------------------------------
# 5. RUN + EXPORT
# --------------------------------------------------------

def main():
    print("=" * 80)
    print("FETCHING HISTORICAL DATA UP TO 2024-12-31")
    print("=" * 80)
    print(f"Cutoff date: {CUTOFF_DATE.strftime('%Y-%m-%d')}")
    print("This will populate fundamentals_long.csv with historical data.")
    print("After running this, you can test incrementalUpdate.py to see")
    print("if it only adds 2025+ data.\n")
    
    df_long = get_all_fundamentals(COMPANIES)
    
    if df_long.empty:
        print("\n⚠ No data found. Check your connection or SSL settings.")
        return
    
    # Show date range of fetched data and verify no 2025 data
    if "Filing Date" in df_long.columns:
        df_long["Filing Date"] = pd.to_datetime(df_long["Filing Date"], errors="coerce")
        min_date = df_long["Filing Date"].min()
        max_date = df_long["Filing Date"].max()
        print(f"\nFetched data date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
        
        # Verify all dates are <= 2024-12-31
        after_cutoff = df_long[df_long["Filing Date"] > CUTOFF_DATE]
        if len(after_cutoff) > 0:
            print(f"\n⚠ ERROR: {len(after_cutoff)} rows have dates after {CUTOFF_DATE.strftime('%Y-%m-%d')}")
            print("Sample of problematic rows:")
            print(after_cutoff[["Ticker", "Filing Date", "Fiscal Year", "Period"]].head(10))
            # Remove them
            df_long = df_long[df_long["Filing Date"] <= CUTOFF_DATE]
            print(f"✓ Removed problematic rows. Final count: {len(df_long)} rows")
        else:
            print(f"✓ All {len(df_long)} rows are on or before {CUTOFF_DATE.strftime('%Y-%m-%d')}")
        
        # Check for 2025 Filing Dates specifically
        df_2025_fd = df_long[df_long["Filing Date"].dt.year == 2025]
        if len(df_2025_fd) > 0:
            print(f"\n⚠ ERROR: Found {len(df_2025_fd)} rows with 2025 filing dates! Removing them...")
            df_long = df_long[df_long["Filing Date"].dt.year < 2025]
            print(f"✓ Removed 2025 filing date rows. Final count: {len(df_long)} rows")
        else:
            print(f"✓ Verified: No 2025 filing dates found")
        
        # Check for Fiscal Year 2025
        if "Fiscal Year" in df_long.columns:
            df_long["Fiscal Year"] = pd.to_numeric(df_long["Fiscal Year"], errors="coerce")
            df_2025_fy = df_long[df_long["Fiscal Year"] >= 2025]
            if len(df_2025_fy) > 0:
                print(f"\n⚠ ERROR: Found {len(df_2025_fy)} rows with Fiscal Year >= 2025! Removing them...")
                df_long = df_long[(df_long["Fiscal Year"].isna()) | (df_long["Fiscal Year"] < 2025)]
                print(f"✓ Removed Fiscal Year 2025+ rows. Final count: {len(df_long)} rows")
            else:
                print(f"✓ Verified: No Fiscal Year 2025+ found")
        
        # Convert back to string format for CSV
        df_long["Filing Date"] = df_long["Filing Date"].dt.strftime("%Y-%m-%d")
    
    df_long.to_csv("fundamentals_long.csv", index=False)

    print(f"\n✓ Saved long-format dataset -> fundamentals_long.csv ({len(df_long)} rows)")
    print(df_long.head())

    # Optional wide pivot table
    df_wide = df_long.pivot_table(
        index=["Ticker", "Fiscal Year", "Period"],
        columns="Metric",
        values="Value",
        aggfunc="last"
    )
    
    # Add Filing Date as a column (get the latest filing date for each period)
    if "Filing Date" in df_long.columns:
        filing_dates = df_long.groupby(["Ticker", "Fiscal Year", "Period"])["Filing Date"].max()
        df_wide["Filing Date"] = df_wide.index.map(lambda x: filing_dates.get(x, None))
    
    # Reset index to make Ticker, Fiscal Year, Period regular columns
    df_wide = df_wide.reset_index()

    df_wide.to_csv("fundamentals_wide.csv", index=False)

    print(f"\n✓ Saved wide-format dataset -> fundamentals_wide.csv")
    print(df_wide.head())
    
    print("\n" + "=" * 80)
    print("NEXT STEP: Run incrementalUpdate.py to test if it only adds 2025+ data")
    print("=" * 80)


if __name__ == "__main__":
    main()

