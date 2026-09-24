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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "7fd7ffb3-95c9-4ac5-9b7b-fdfc4032febc",
# META       "known_warehouses": [
# META         {
# META           "id": "7fd7ffb3-95c9-4ac5-9b7b-fdfc4032febc",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## validate and combine MLB snapshots
# this notebook does the following:
# 
# 
# 0. set up necessary libraries and config data parameters
# 1. get latest data from raw_snapshot tables
# 2. identify new snapshots tables for ingestion
# 3. validate data 30/30 teams exist; no NULL values in key columns
# 4. ingest the new snapshots into fact_MLB_standings table in MLB_lakehouse 

# MARKDOWN ********************

# ### 1. set up and config

# CELL ********************

# import libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

# set up data parameters
LAKEHOUSE = "MLB_lakehouse"
SCHEMA = "dbo"
TARGET_TABLE = f"{LAKEHOUSE}.{SCHEMA}.fact_MLB_standings_silver"
VALIDATION_TABLE = f"{LAKEHOUSE}.{SCHEMA}.mlb_snapshot_validation_log"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. discover snapshot tables 

# CELL ********************


tables = spark.catalog.listTables(f"{LAKEHOUSE}.{SCHEMA}")

table_list = [
    t.name for t in tables 
    if t.name.startswith("raw_mlb_snapshot_")
]

table_list = sorted(table_list)
print(f"Found {len(table_list)} snapshot tables")
print(table_list)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. validate snapshots
# - 30 rows
# - 30 distinct teams
# - No NULLs in key columns (Wins, Losses, etc.)

# CELL ********************

results = []

for table in table_list:
    df = spark.table(f"{LAKEHOUSE}.{SCHEMA}.{table}")
    
    total_rows = df.count()
    distinct_teams = df.select("Team").distinct().count()
    
    # NULL checks
    null_count = df.filter(
        F.col("Team").isNull() |
        F.col("Wins").isNull() |
        F.col("Losses").isNull() |
        F.col("WinPct").isNull()
    ).count()
    
    status = "PASS" if (
        total_rows == 30 and 
        distinct_teams == 30 and 
        null_count == 0
    ) else "FAIL"
    
    results.append((table, total_rows, distinct_teams, null_count, status))

validation_df = spark.createDataFrame(
    results,
    ["snapshot_name", "total_rows", "distinct_teams", "null_issues", "status"]
)


if validation_df.filter(F.col("status") == "FAIL").count() == 0:
    print("✅ All tests passed")
else:
    print("Bad data, please review raw MLB snapshot tables.")

display(validation_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write test results to validation table
validation_df.write.mode("overwrite").saveAsTable(VALIDATION_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#fast fail - if bad data appears, stop process, do not load into silver tables everything
fail_count = validation_df.filter("status = 'FAIL'").count()

if fail_count > 0:
    raise Exception(f"{fail_count} snapshot(s) failed validation!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. incremental load snapshots into fact_mlb_standings_silver

# CELL ********************

#filter valid snapshots
valid_snapshots = [
    row.snapshot_name 
    for row in validation_df.collect() 
    if row.status == "PASS"
]

print(f"Valid snapshots: {len(valid_snapshots)}")
print(valid_snapshots)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#stage data for incremental load
final_df = None

for table in valid_snapshots:
    snapshot_date = table.split("_")[-1]  # extract YYYYMMDD
    
    df = spark.table(f"{LAKEHOUSE}.{SCHEMA}.{table}") \

    final_df = df if final_df is None else final_df.unionByName(df)

print(final_df)
display(final_df)    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



final_df = None

for table in valid_snapshots:
    snapshot_date = table.split("_")[-1]  # extract YYYYMMDD
    
    df = spark.table(f"{LAKEHOUSE}.{SCHEMA}.{table}") \
        .withColumn(
            "SnapshotDate",
            F.to_date(F.lit(snapshot_date), "yyyyMMdd")  
        ) \
        .withColumn(
            "ingested_at",
            F.current_timestamp()   # 
        )
    
    final_df = df if final_df is None else final_df.unionByName(df, allowMissingColumns=True)

if final_df is None:
    print("❌ No valid data to load")
else:
    # Check existing dates
    if spark.catalog.tableExists(TARGET_TABLE):
        existing_dates = spark.table(TARGET_TABLE) \
            .select("SnapshotDate") \
            .distinct()
        
        new_data = final_df.join(existing_dates, on="SnapshotDate", how="left_anti")
    else:
        print("Target table does not exist. Creating it now...")
        new_data = final_df

    new_count = new_data.count()
    print(f"✅ Inserted {new_count} new rows into {TARGET_TABLE}")

    if new_count > 0:
        new_data.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(TARGET_TABLE)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
