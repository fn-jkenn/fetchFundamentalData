# Changelog - Today's Updates

## Summary
Major enhancements to SEC EDGAR data fetching system with incremental updates, deduplication, and upsert functionality.

---

## New Files Created

### `incrementalUpdate.py` (formerly `updateFundamentals.py`)
- **Purpose**: Incremental data updates - only fetches and adds new filings
- **Features**:
  - Filters to only fetch filings newer than what's in CSV
  - Upsert functionality (insert/update/skip based on filing dates)
  - Primary key deduplication (Ticker + Fiscal Year + Period)
  - Detailed logging of new/updated data
  - Automatic wide-format CSV regeneration

### `fetchHistoricalDataUpTo2024.py`
- **Purpose**: Test utility to populate CSV with historical data up to 2024
- **Features**:
  - Filters data by Filing Date ≤ 2024-12-31
  - Filters Fiscal Year < 2025
  - Used for testing incremental update functionality

---

## File Renames

- `tryFetchUsingEdgarAPI.py` → `fetchAllData.py`
- `updateFundamentals.py` → `incrementalUpdate.py`
- All imports and references updated across codebase

---

## Core Functionality Changes

### Primary Key Implementation
- **Added**: Primary key concept using `Ticker + Fiscal Year + Period`
- **Purpose**: Ensures one row per company/period combination
- **Implementation**: 
  - Keeps latest filing when multiple filings exist for same period
  - Prevents duplicate reporting periods
  - Applied in both `fetchAllData.py` and `incrementalUpdate.py`

### Deduplication Logic
- **Fixed**: Fiscal Year normalization (float vs int handling)
- **Added**: `deduplicate_by_primary_key()` function
- **Added**: `build_primary_key()` function for key generation
- **Result**: Consistent key matching between CSV and API data

### Upsert Functionality
- **Added**: `upsert_data()` function
- **Logic**:
  - INSERT: New ticker/fiscal year/period/metric combinations
  - UPDATE: Existing combinations with newer filing dates (amended filings)
  - SKIP: Existing combinations with same/older filing dates
- **Benefits**: Automatically handles amended filings and data corrections

### Date Filtering
- **Added**: `get_latest_filing_dates()` function
- **Added**: `filter_to_new_filings()` function
- **Purpose**: Only fetch filings newer than latest date in CSV per ticker
- **Result**: Significantly reduces API calls and processing time

### Enhanced Logging
- **Added**: `log_new_data()` function with detailed breakdown
- **Shows**:
  - New rows by ticker, metric, fiscal year
  - Unique filings added
  - All new rows with full details
- **Added**: Upsert summary (inserted/updated/skipped counts)

---

## Configuration Changes

### SSL Verification
- **Changed**: SSL verification now set directly in Python files
- **Default**: `SEC_API_VERIFY_SSL=false` (can be overridden)
- **Files**: `fetchAllData.py`, `incrementalUpdate.py`, `fetchHistoricalDataUpTo2024.py`
- **Result**: Can run scripts directly without batch files

---

## Bug Fixes

### Fiscal Year Data Type Mismatch
- **Issue**: CSV stored Fiscal Year as float (2025.0), API returns int (2025)
- **Fix**: Normalized Fiscal Year to int before string conversion in key building
- **Result**: Keys now match correctly, preventing false duplicates

### Duplicate Detection
- **Issue**: Same data being added repeatedly
- **Fix**: Improved key normalization and deduplication logic
- **Result**: Proper duplicate detection and prevention

### Date Filtering Edge Cases
- **Issue**: Fiscal Year 2025 appearing with 2024 filing dates
- **Fix**: Added dual filtering (Filing Date AND Fiscal Year)
- **Result**: Accurate historical data cutoff

---

## Code Quality Improvements

- **Added**: Type hints throughout
- **Added**: Comprehensive docstrings
- **Improved**: Error handling and edge case coverage
- **Standardized**: Consistent naming conventions
- **Cleaned**: Removed redundant code

---

## Documentation Updates

- **Updated**: `README.md` with new file names
- **Updated**: Function documentation strings
- **Added**: Usage examples for new scripts

---

## Testing Utilities

- **Created**: `check_csv_dates.py` - utility to verify CSV date ranges
- **Created**: `fetchHistoricalDataUpTo2024.py` - test data generator

---

## Key Benefits

✅ **Efficiency**: Only fetches new data, not entire history  
✅ **Accuracy**: Handles amended filings automatically  
✅ **Reliability**: Primary key prevents duplicates  
✅ **Transparency**: Detailed logging of all changes  
✅ **Maintainability**: Clean code structure with clear naming  

---

**Date**: Today  
**Status**: Production Ready

