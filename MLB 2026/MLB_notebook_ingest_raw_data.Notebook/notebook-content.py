# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ccc7e16c-a9b8-410c-8caf-7b2d26574761",
# META       "default_lakehouse_name": "MLB_lakehouse",
# META       "default_lakehouse_workspace_id": "4a840812-99d9-446b-921e-716bac127e16",
# META       "known_lakehouses": [
# META         {
# META           "id": "ccc7e16c-a9b8-410c-8caf-7b2d26574761"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## get raw MLB team standings data
# this notebook does the following:
# 1. gets MLB teams win/loss data from espn.com
# 2. validates extracted data
# 3. writes data to MLB_lakehouse

# MARKDOWN ********************

# 
# ### 0. set up libraries
# required to get data via REST API data and use Python to reshape data

# CELL ********************

import requests
import pandas as pd
from datetime import date


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1. import data
# get a snapshot of data from ESPN REST API and display snapshot

# CELL ********************

url = "https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings"

response = requests.get(url)

if response.status_code != 200:
    raise Exception(f"API request failed with status {response.status_code}")

data = response.json()

rows = []

for league in data.get('children', []):
    for entry in league.get('standings', {}).get('entries', []):
        
        team_name = entry['team']['displayName']
        
        stats = {}
        for stat in entry.get('stats', []):
            name = stat.get('name')
            value = stat.get('value', stat.get('displayValue'))
            stats[name] = value
        
        rows.append({
            "Team": team_name,
            "Wins": stats.get("wins"),
            "Losses": stats.get("losses"),
            "WinPct": stats.get("winPercent"),
            "SnapshotDate": pd.to_datetime(date.today()),
            "Season": 2026
        })

df = pd.DataFrame(rows)

print(f"Extracted {len(df)} rows")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. data validation
# - there should be 30 teams
# - there should note be NULL values in Wins, Losses, and WinPCT
# - Wins, Losses > 0
# - WinPCT should be between 0 and 1

# CELL ********************


errors = []

# Team count check
team_count = df['Team'].nunique()
if team_count != 30:
    errors.append(f"Expected 30 teams, found {team_count}")

# Null checks
null_cols = ['Wins', 'Losses', 'WinPct']

for col in null_cols:
    if df[col].isnull().any():
        errors.append(f"Null values found in column: {col}")

# Logical checks
numeric_cols = ['Wins', 'Losses',]

for col in numeric_cols:
    if (df[col] < 1).any():
        errors.append(f"Invalid values in: {col} ")

# duplicate team check
if df.duplicated(subset=['Team']).any():
    errors.append("Duplicate teams found in dataset")

# error handling message
if errors:
    error_message = "❌ DATA VALIDATION FAILED:\n" + "\n".join(errors)
    raise ValueError(error_message)

print("✅ All data validation checks passed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. write raw data to lakehouse table
# - if data validation passed, write data to Delta table in OneLake Lakehouse MLB_lakehouse
# - table naming conventions = lakehouse_MLB.raw_mlb_snapshot_YYYYMMDD
# - if data validation failed, return an error message

# CELL ********************


snapshot_date_str = date.today().strftime("%Y%m%d")

table_name = f"raw_mlb_snapshot_{snapshot_date_str}"

print(f"Writing to table: {table_name}")

spark_df = spark.createDataFrame(df)

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"✅ Data successfully written to {table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
