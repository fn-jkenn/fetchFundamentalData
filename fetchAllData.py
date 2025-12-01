import os
import time
from typing import Dict, List

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

# --------------------------------------------------------
# 3. FETCH COMPANY FUNDAMENTALS
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
            rows.append({
                "Ticker": ticker,
                "CIK": cik,
                "Metric": metric_name,
                "GAAPTag": tag,
                "Value": entry.get("val"),
                "Fiscal Year": entry.get("fy"),
                "Period": entry.get("fp"),
                "Filing Date": entry.get("end"),
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
    return pd.DataFrame(all_data)


# --------------------------------------------------------
# 5. DEDUPLICATION USING PRIMARY KEY
# --------------------------------------------------------

def deduplicate_by_primary_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate using primary key (Ticker + Fiscal Year + Period).
    For each unique period, keeps only the latest filing (by Filing Date).
    This ensures no duplicate reporting periods while preserving all metrics.
    """
    if df.empty or "Filing Date" not in df.columns:
        return df
    
    df = df.copy()
    
    # Build primary key: Ticker + Fiscal Year + Period
    def build_primary_key(frame):
        subset = frame[["Ticker", "Fiscal Year", "Period"]].copy()
        # Normalize Fiscal Year
        if "Fiscal Year" in subset.columns:
            fiscal_numeric = pd.to_numeric(subset["Fiscal Year"], errors="coerce")
            fiscal_str = fiscal_numeric.astype(object)
            mask_valid = fiscal_numeric.notna()
            fiscal_str.loc[mask_valid] = fiscal_numeric.loc[mask_valid].astype(int).astype(str)
            fiscal_str.loc[~mask_valid] = "<NA>"
            subset["Fiscal Year"] = fiscal_str
        # Fill NaN and convert to string
        for col in subset.columns:
            if col != "Fiscal Year":
                subset[col] = subset[col].fillna("<NA>").astype(str).str.strip()
        return subset.agg("||".join, axis=1)
    
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


# --------------------------------------------------------
# 6. RUN + EXPORT
# --------------------------------------------------------

def main():
    df_long = get_all_fundamentals(COMPANIES)
    
    # Deduplicate using primary key (keeps latest filing for each period)
    before_count = len(df_long)
    df_long = deduplicate_by_primary_key(df_long)
    after_count = len(df_long)
    
    if before_count != after_count:
        print(f"\n✓ Removed {before_count - after_count} duplicate periods (kept latest filing for each period)")
    
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

    df_wide.to_csv("fundamentals_wide.csv")

    print(f"\n✓ Saved wide-format dataset -> fundamentals_wide.csv")
    print(df_wide.head())


if __name__ == "__main__":
    main()
