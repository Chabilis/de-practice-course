# pipeline.py - data engineering project 1
# excel to SQlite database (1st DE pipline)

import pandas as pd
import sqlite3
import os
from datetime import datetime

print("Data pipeline - marlou")
print("="*50)

# 1. Read any excel file (even messy ones)
file_path = "sample_data.xlsx"                      # test excel file

if not os.path.exists(file_path):
    print(f"File {file_path} not fould: Creating a new one.")
    # create sample data
    data = {
        "Date": ["2025-01-15", "2025-01-16", "2025-01-17", "", "2025-01-19"],
        "Project": ["NCTCPP", "NCTCPP", "Bridge Project", "NCTCPP", "Highway"],
        "Item": ["Rebar", "Concrete", "Formwork", "Cement", "Steel"],
        "Quantity": [1500, 85.5, 220, 45, "1200"],
        "Unit": ["kg", "cu.m", "", "bags", "kg"],
        "Cost": [75000, 102000, 88000, None, 600000]
    }

    df = pd.DataFrame(data)
    df.to_excel("sample_data.xlsx", index=False)
    print("sample excel created: sample_data.xlsx")
else:
    df = pd.read_excel(file_path, engine="openpyxl")
    print(f"Loaded {file_path} to {len(df)} rows")

print("\nRaw data preview:")
print(df.head())

# 2. clean the data
print("\n Cleaning the data...")
df_clean = df.copy()

# Fix common issues
df_clean = df_clean.dropna(how="all")  # remove empty rows
df_clean["Quantity"] = pd.to_numeric(df_clean["Quantity"], errors="coerce")
df_clean["Cost"] = pd.to_numeric(df_clean["Cost"], errors="coerce")
df_clean["Date"] = pd.to_datetime(df_clean["Date"], errors="coerce")

# 3. Save to SQLite database (perma storage)
db_path = "construction_data.db"
conn = sqlite3.connect(db_path)
df_clean.to_sql("materials", conn, if_exists="replace", index=False)
conn.close()

print(f"\nSUCCESS! Data saved to {db_path}")
print(f"Table name: materials")

# 4. Prove it works – run a query
conn = sqlite3.connect(db_path)
query_result = pd.read_sql("SELECT Project, SUM(Cost) as Total_Cost FROM materials GROUP BY Project", conn)
conn.close()

print("\nQUERY RESULT - Total cost per project:")
print(query_result)

print("\nPortfolio Project #1 = completed")