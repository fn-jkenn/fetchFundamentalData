# SEC EDGAR Financial Data Fetcher

A Python script that fetches company financial fundamentals from the SEC EDGAR API and exports them to CSV files in both long and wide formats.

## 📋 Overview

This script retrieves US-GAAP financial metrics for multiple companies from the SEC's public API and organizes the data into structured CSV files. It supports:

- **27+ financial metrics** (Revenue, Net Income, Assets, Cash Flow, EPS, etc.)
- **Multiple companies** processed in batch
- **Automatic retry logic** for network reliability
- **SSL certificate handling** for corporate proxy environments
- **Two output formats**: Long (database-friendly) and Wide (Excel-friendly)

## 🚀 Quick Start

### Prerequisites

- Python 3.7+
- Required packages: `requests`, `pandas`

### Installation

1. Install dependencies:
```bash
py -m pip install requests pandas
```

2. Run the script:
```bash
# Run directly with Python (SSL verification is disabled by default)
py fetchAllData.py

# For incremental updates (only fetches new data):
py incrementalUpdate.py
```

## 📁 Output Files

The script generates two CSV files:

### `fundamentals_long.csv`
- **Format**: One row per metric per filing period
- **Use case**: Database storage, filtering, time-series analysis
- **Columns**: Ticker, CIK, Metric, GAAPTag, Value, Fiscal Year, Period, Filing Date, Form, Unit
- **Size**: ~34,000 rows (varies by companies and time periods)

### `fundamentals_wide.csv`
- **Format**: One row per company/period, metrics as columns
- **Use case**: Excel analysis, side-by-side comparisons, financial modeling
- **Columns**: Ticker, Fiscal Year, Period, then one column per metric (Revenue, Net Income, etc.)
- **Size**: ~650 rows (one per unique company/period combination)

## 🔧 Configuration

### Companies List

Edit the `COMPANIES` dictionary to add/remove companies:

```python
COMPANIES = {
     "Axon Enterprise Inc": "0001069183",
      "GE Aerospace": "0000040545",
      "General Dynamics Corporation": "",
      "Howmet Aerospace Inc": "0000004281",
      "L3Harris Technologies, Inc": "0000202058",
      "Lockheed Martin Corporation": "0000936468",
      "Northrop Grumman Corporation": "0001133421",
      "RTX Corporation": "0000101829",
      "The Boeing Company": "0000012927",
      "TransDigm Group Incorporated": "0001260221"
  
}
```

*Finding a CIK**: Search for a company on [SEC.gov](https://www.sec.gov/edgar/searchedgar/companysearch.html) and use the CIK number.

### Financial Metrics

The `GAAP_TAGS` dictionary defines which metrics to extract. Currently includes:

- **Income Statement**: Revenue, Cost of Revenue, Gross Profit, Operating Income, Net Income, EBIT, EBITDA
- **Balance Sheet**: Total Assets, Liabilities, Shareholder Equity, Cash, Accounts Receivable, Inventory, Debt
- **Cash Flow**: Operating, Investing, Financing activities, Dividends, Share Buybacks
- **Per Share**: EPS Basic, EPS Diluted, Shares Outstanding
- **Operating**: R&D Expense, SG&A Expense

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SEC_API_USER_AGENT` | Your email address (SEC requirement) | `contact@example.com` |
| `SEC_API_VERIFY_SSL` | Enable/disable SSL verification | `true` |
| `SEC_API_CA_BUNDLE` | Path to custom CA certificate file | None |

**Example**:
```bash
# Set your email
$env:SEC_API_USER_AGENT="your.email@example.com"

# Disable SSL verification (for corporate proxies)
$env:SEC_API_VERIFY_SSL="false"
```

## 📖 Code Structure

### Main Functions

#### `pick_unit(tag, units)`
**Purpose**: Selects the appropriate unit type for a GAAP tag.

**Logic**:
1. Checks for preferred units (e.g., EPS → "USD/shares")
2. Falls back to "USD" if available
3. Returns first available unit if no preference
4. Returns `None` if no units exist

**Why it exists**: Different metrics use different units (USD, USD/shares, shares, pure), and we need to pick the right one.

#### `fetch_with_retry(url)`
**Purpose**: Fetches data from SEC API with automatic retry logic.

**Features**:
- Retries up to 3 times on failure
- 30-second timeout per request
- 3-second delay between retries
- Handles SSL verification errors
- Returns `None` if all retries fail

**Why it exists**: Network requests can fail temporarily; retries improve reliability.

#### `get_company_fundamentals(cik, ticker)`
**Purpose**: Extracts financial data for a single company.

**Process**:
1. Builds SEC API URL (e.g., `https://data.sec.gov/api/xbrl/companyfacts/CIK0001069183.json`)
2. Fetches JSON data using `fetch_with_retry()`
3. Parses JSON to extract US-GAAP facts
4. Loops through each metric in `GAAP_TAGS`:
   - Checks if metric exists in SEC data
   - Selects appropriate unit using `pick_unit()`
   - Extracts all historical entries for that metric
   - Creates dictionary with metadata (ticker, CIK, fiscal year, period, etc.)
5. Returns list of dictionaries (one per data point)

**Returns**: List of dictionaries, each containing:
```python
{
    "Ticker": "AXON",
    "CIK": "0001069183",
    "Metric": "Revenue",
    "GAAPTag": "Revenues",
    "Value": 9989219.0,
    "Fiscal Year": 2011.0,
    "Period": "FY",
    "Filing Date": "2010-12-31",
    "Form": "10-K",
    "Unit": "USD"
}
```

#### `get_all_fundamentals(companies)`
**Purpose**: Processes all companies and combines their data.

**Process**:
1. Creates empty list to store all data
2. Loops through each company in `COMPANIES` dictionary:
   - Prints progress message
   - Calls `get_company_fundamentals()` for that company
   - Adds returned rows to master list
3. Converts list of dictionaries to pandas DataFrame
4. Returns DataFrame

**Returns**: pandas DataFrame with all companies' data combined.

### Execution Flow

```
1. Import libraries and set configuration
   ↓
2. get_all_fundamentals(COMPANIES)
   ↓
   For each company:
     ↓
     get_company_fundamentals(cik, ticker)
       ↓
       fetch_with_retry(url) → Get JSON from SEC
       ↓
       Parse JSON → Extract US-GAAP facts
       ↓
       For each metric:
         pick_unit() → Select unit
         ↓
         Extract data entries
       ↓
     Return list of dictionaries
   ↓
3. Combine all data → Convert to DataFrame
   ↓
4. Save to fundamentals_long.csv
   ↓
5. Pivot DataFrame to wide format
   ↓
6. Save to fundamentals_wide.csv
```

## 🐛 Troubleshooting

### SSL Certificate Verification Error

**Error**: `SSLCertVerificationError: certificate verify failed`

**Cause**: Corporate proxy/firewall is intercepting HTTPS connections.

**Solution 1** (Temporary - per session):
```powershell
$env:SEC_API_VERIFY_SSL="false"
py fetchAllData.py
```

**Solution 2** (Permanent - recommended):
1. Export your corporate CA certificate to a `.pem` file
2. Set environment variable:
```powershell
setx SEC_API_CA_BUNDLE "C:\path\to\certificate.pem"
```
3. Restart terminal and run script normally

**Solution 3**: Use the provided batch file:
```bash
run_try.bat
```

### No Data Retrieved

**Possible causes**:
- Network connectivity issues
- SEC API rate limiting (wait a few minutes)
- Invalid CIK numbers
- Company doesn't have data for requested metrics

**Check**: Look for error messages in console output. The script prints warnings for failed requests.

### Missing Columns in Output

**Cause**: Some companies may not report all metrics, or metrics may use unexpected unit types.

**Solution**: The script automatically skips metrics that aren't available. Check the SEC website directly if you need specific data.

## 📊 Example Output

### Long Format Sample
```
Ticker,CIK,Metric,GAAPTag,Value,Fiscal Year,Period,Filing Date,Form,Unit
AXON,0001069183,Revenue,Revenues,9989219.0,2011.0,FY,2010-12-31,10-K,USD
AXON,0001069183,Gross Profit,GrossProfit,44529237.0,2011.0,FY,2010-12-31,10-K,USD
```

### Wide Format Sample
```
Ticker,Fiscal Year,Period,Revenue,Gross Profit,Net Income,Total Assets,...
AXON,2011.0,FY,9989219.0,44529237.0,-7039866.0,104962751.0,...
```

## 📝 Notes

- **SEC Rate Limiting**: The SEC API has rate limits. If you're processing many companies, add delays between requests.
- **Data Freshness**: Data is pulled from SEC's latest filings. Historical data may be updated as companies refile.
- **Missing Metrics**: Not all companies report all metrics. The script skips unavailable metrics automatically.
- **User-Agent**: SEC requires a valid email in the User-Agent header. Update `SEC_API_USER_AGENT` with your contact email.

## 🔗 Resources

- [SEC EDGAR API Documentation](https://www.sec.gov/edgar/sec-api-documentation)
- [SEC Company Search](https://www.sec.gov/edgar/searchedgar/companysearch.html)
- [US-GAAP Taxonomy](https://www.fasb.org/jsp/FASB/Page/SectionPage&cid=1176156316498)

## 📄 License

This script is provided as-is for educational and research purposes.

## 🤝 Contributing

Feel free to:
- Add more GAAP tags to `GAAP_TAGS`
- Improve error handling
- Add data validation
- Support additional output formats

---

**Last Updated**: 2025

