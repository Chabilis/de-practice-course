# pipeline.py - data engineering project 1
# excel to SQlite database (1st DE pipline)

import pandas as pd
import sqlite3
import os
from datetime import datetime

print("Data pipeline - marlou")
print("="*50)

# 1. Read any excel file (even messy ones)
file_path = "test1.xlsx"                      # test excel file

if not os.path.exists(file_path):
    print(f"File {file_path} not fould: Creating a new one.")
    # create sample data
    # Dictionary (key:[set])
    data = {
        "Date": ["2025-01-15", "2025-01-16", "2025-01-17", "", "2025-01-19"],
        "Project": ["NCTCPP", "NCTCPP", "Bridge Project", "NCTCPP", "Highway"],
        "Item": ["Rebar", "Concrete", "Formwork", "Cement", "Steel"],
        "Quantity": [1500, 85.5, 220, 45, "1200"],
        "Unit": ["kg", "cu.m", "", "bags", "kg"],
        "Cost": [75000, 102000, 88000, None, 600000]
    }

    df = pd.DataFrame(data)
    df.to_excel("sample_data.xlsx", index=False)    # creating excel no index
    print("sample excel created: sample_data.xlsx")
else:
    df = pd.read_excel(file_path, engine="openpyxl")
    # Opens an Excel file safely (openpyxl = the only engine that works reliably in 2025)
    print(f"Loaded {file_path} to {len(df)} rows")

print("\nRaw data preview:")
print(df.head())

# 2. clean the data
print("\n Cleaning the data...")
df_clean = df.copy()
df_clean = df_clean.dropna(how="all")

# DYNAMIC COLUMN DETECTION (this is what makes you ₱100k+)
# Find columns that contain these keywords (case-insensitive)
def find_column(columns, keywords):
    for col in columns:
        if any(k.lower() in col.lower() for k in keywords):
            return col
    return None

# Find real column names
date_col = find_column(df_clean.columns, ["date"])
project_col = find_column(df_clean.columns, ["project", "site"])
item_col = find_column(df_clean.columns, ["item", "material", "description"])
qty_col = find_column(df_clean.columns, ["qty", "quantity", "amount", "volume"])
unit_col = find_column(df_clean.columns, ["unit", "uom"])
cost_col = find_column(df_clean.columns, ["cost", "price", "amount", "total"])

# Convert only if column exists
if qty_col:
    df_clean[qty_col] = pd.to_numeric(df_clean[qty_col], errors="coerce")
if cost_col:
    df_clean[cost_col] = pd.to_numeric(df_clean[cost_col], errors="coerce")
if date_col:
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors="coerce")

print(f"Found columns → Qty: '{qty_col}' | Cost: '{cost_col}' | Date: '{date_col}'")
print(f"Cleaned → {len(df_clean)} rows")

# 3. Save to SQLite database (perma storage)
db_path = "construction_data.db"
conn = sqlite3.connect(db_path)                                                # Creates/opens a real database file on your computer
df_clean.to_sql("materials", conn, if_exists="replace", index=False)           # Dumps the entire clean table into the database in ONE line
conn.close()

print(f"\nSUCCESS! Data saved to {db_path}")
print(f"Table name: materials")

# 4. Prove it works – run a query
conn = sqlite3.connect(db_path)
query_result = pd.read_sql("SELECT Project, SUM(Cost) as Total_Cost FROM materials GROUP BY Project", conn)
# Runs real SQL to answer business questions
conn.close()

print("\nQUERY RESULT - Total cost per project:")
print(query_result)

print("\nPortfolio Project #1 = completed")