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

# ## MLB_notebook_validate_and_write_to_silver
# 
# this notebook does the following:
# 1. set up necessary libraries and config data parameters
# 2. get latest data from raw MLB snapshot tables
# 3. identify NEW snapshot tables that haven't been validated yet (incremental — avoids re-validating every historic snapshot on each refresh)
# 4. validate the new data only: 30/30 teams exist; no NULL values in key columns
# 5. ingest the new, passing snapshots into fact_MLB_standings_silver table in MLB_lakehouse


# MARKDOWN ********************

# ### Set up environment
# get libraries and configure parameters

# CELL ********************

# import libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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

# ### discover snapshot tables, then filter down to only the NEW ones
# The validation log already records every snapshot that's been checked (pass or fail). We use it as the "already processed" list so each run only touches newly-landed snapshot tables instead of the full history.


# CELL ********************

tables = spark.catalog.listTables(f"{LAKEHOUSE}.{SCHEMA}")
 
all_snapshot_tables = sorted([
    t.name for t in tables
    if t.name.startswith("raw_mlb_snapshot_")
])
print(f"Found {len(all_snapshot_tables)} snapshot tables total")
 
if spark.catalog.tableExists(VALIDATION_TABLE):
    already_validated = {
        row.snapshot_name
        for row in spark.table(VALIDATION_TABLE)
            .select("snapshot_name")
            .distinct()
            .collect()
    }
else:
    already_validated = set()
 
table_list = [t for t in all_snapshot_tables if t not in already_validated]
 
print(f"{len(already_validated)} snapshot(s) already validated previously")
print(f"{len(table_list)} new snapshot(s) to validate this run")
print(table_list)
 
if not table_list:
    print("✅ No new snapshots to validate — nothing to do.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### validate new snapshots only
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
 
validation_schema = StructType([
    StructField("snapshot_name", StringType(), True),
    StructField("total_rows", IntegerType(), True),
    StructField("distinct_teams", IntegerType(), True),
    StructField("null_issues", IntegerType(), True),
    StructField("status", StringType(), True),
])

validation_df = spark.createDataFrame(
    results,
    schema=validation_schema
).withColumn("validated_at", F.current_timestamp())
 
if len(results) == 0:
    print("Nothing new to validate this run.")
elif validation_df.filter(F.col("status") == "FAIL").count() == 0:
    print("✅ All new snapshots passed")
else:
    print("Bad data, please review raw MLB snapshot tables.")
 
display(validation_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# append this run's results onto the validation log instead of overwriting
if validation_df.count() > 0:
    validation_df.write.mode("append").saveAsTable(VALIDATION_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fast fail - if bad data appears among the NEW snapshots, stop the process
# before anything gets loaded into silver
fail_count = validation_df.filter("status = 'FAIL'").count()

if fail_count > 0:
    raise Exception(f"{fail_count} new snapshot(s) failed validation!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### incrementally load the newly-validated snapshots into fact_mlb_standings_silver


# CELL ********************

# filter to this run's passing snapshots
valid_snapshots = [
    row.snapshot_name
    for row in validation_df.collect()
    if row.status == "PASS"
]

print(f"Valid new snapshots: {len(valid_snapshots)}")
print(valid_snapshots)

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
            F.current_timestamp()
        )

    final_df = df if final_df is None else final_df.unionByName(df, allowMissingColumns=True)

if final_df is None:
    print("❌ No valid new data to load")
else:
    # Belt-and-suspenders: this anti-join guards against a snapshot date that's
    # somehow already in silver, even though the table-name filter in step 2
    # is what does the real incremental filtering now.
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
