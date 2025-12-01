import pandas as pd
from datetime import datetime

df = pd.read_csv('fundamentals_long.csv')
print(f"Total rows: {len(df)}")

# Check Filing Date
if "Filing Date" in df.columns:
    df["Filing Date"] = pd.to_datetime(df["Filing Date"], errors="coerce")
    min_date = df["Filing Date"].min()
    max_date = df["Filing Date"].max()
    print(f"\nFiling Date range: {min_date} to {max_date}")
    
    # Check for 2025 dates
    df_2025 = df[df["Filing Date"].dt.year == 2025]
    print(f"Rows with 2025 Filing Date: {len(df_2025)}")
    
    if len(df_2025) > 0:
        print("\nSample 2025 Filing Date rows:")
        print(df_2025[["Ticker", "Filing Date", "Fiscal Year", "Period", "Form", "Metric"]].head(20))
else:
    print("No 'Filing Date' column found")

# Check Fiscal Year
if "Fiscal Year" in df.columns:
    print(f"\nFiscal Year column check:")
    unique_fy = sorted([int(x) for x in df["Fiscal Year"].dropna().unique() if pd.notna(x)])
    print(f"Fiscal Year range: {min(unique_fy)} to {max(unique_fy)}")
    print(f"Latest 10 Fiscal Years: {unique_fy[-10:]}")
    
    df_2025_fy = df[df["Fiscal Year"] == 2025]
    print(f"\nRows with Fiscal Year 2025: {len(df_2025_fy)}")
    
    if len(df_2025_fy) > 0:
        print("\nSample rows with Fiscal Year 2025:")
        print(df_2025_fy[["Ticker", "Filing Date", "Fiscal Year", "Period", "Form", "Metric"]].head(20))
        
        # Check filing dates for these rows
        print("\nFiling dates for Fiscal Year 2025 rows:")
        df_2025_fy["Filing Date"] = pd.to_datetime(df_2025_fy["Filing Date"], errors="coerce")
        print(f"Filing Date range: {df_2025_fy['Filing Date'].min()} to {df_2025_fy['Filing Date'].max()}")
else:
    print("No 'Fiscal Year' column found")

