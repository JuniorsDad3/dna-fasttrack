# add_columns.py
import pandas as pd

EXCEL_FILE = r"C:\Users\dell5348\fasttrack\forensic_cases.xlsx"

# Load the sheet
df = pd.read_excel(EXCEL_FILE, sheet_name="cases")

# Add columns if they don't exist
if "Status" not in df.columns:
    df["Status"] = ""  # or default value
if "Suspect" not in df.columns:
    df["Suspect"] = ""  # or default value

# Save back to Excel
df.to_excel(EXCEL_FILE, sheet_name="cases", index=False)

print("Columns 'Status' and 'Suspect' ensured in Excel file.")
