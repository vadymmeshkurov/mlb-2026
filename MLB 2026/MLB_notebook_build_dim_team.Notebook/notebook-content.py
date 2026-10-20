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

# ## build dim_team
# this notebook does the following:
# 1. maps ESPN team names to MLBAMGames logo file codes
# 2. downloads each team's light and dark SVG logo from GitHub
# 3. encodes each logo as a data URI (so Power BI can render it without OneLake auth)
# 4. writes dim_team to MLB_lakehouse

# CELL ********************

import requests
import urllib.parse
from pyspark.sql import Row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1. team mapping
# keys must match ESPN `displayName` exactly (that is what lands in fact_mlb_standings_silver[Team])

# CELL ********************

TEAM_CODES = {
    "Arizona Diamondbacks": "ari",
    "Atlanta Braves": "atl",
    "Baltimore Orioles": "bal",
    "Boston Red Sox": "bos",
    "Chicago Cubs": "chc",
    "Chicago White Sox": "cws",
    "Cincinnati Reds": "cin",
    "Cleveland Guardians": "cle",
    "Colorado Rockies": "col",
    "Detroit Tigers": "det",
    "Houston Astros": "hou",
    "Kansas City Royals": "kc",
    "Los Angeles Angels": "laa",
    "Los Angeles Dodgers": "lad",
    "Miami Marlins": "mia",
    "Milwaukee Brewers": "mil",
    "Minnesota Twins": "min",
    "New York Mets": "nym",
    "New York Yankees": "nyy",
    "Athletics": "oak",
    "Philadelphia Phillies": "phi",
    "Pittsburgh Pirates": "pit",
    "San Diego Padres": "sd",
    "San Francisco Giants": "sf",
    "Seattle Mariners": "sea",
    "St. Louis Cardinals": "stl",
    "Tampa Bay Rays": "tb",
    "Texas Rangers": "tex",
    "Toronto Blue Jays": "tor",
    "Washington Nationals": "wsh",
}

assert len(TEAM_CODES) == 30, f"expected 30 teams, got {len(TEAM_CODES)}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. download logos and encode as data URIs

# CELL ********************

BASE_URL = "https://raw.githubusercontent.com/MLBAMGames/mlb_teams_logo_svg/main"
MAX_TEXT_LEN = 32766  # Power BI truncates text values longer than this

def svg_to_data_uri(svg_text: str) -> str:
    # collapse whitespace, then percent-encode everything except a minimal safe set
    compact = " ".join(svg_text.split())
    return "data:image/svg+xml;utf8," + urllib.parse.quote(compact, safe=" =:/;,.-_()")

def fetch_logo(code: str, variant: str) -> str:
    suffix = "l" if variant == "light" else "d"
    url = f"{BASE_URL}/{variant}/{code}_{suffix}.svg"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    uri = svg_to_data_uri(resp.text)
    assert len(uri) <= MAX_TEXT_LEN, f"{url} is {len(uri)} chars, over the Power BI limit"
    return uri

rows = []
for team, code in TEAM_CODES.items():
    rows.append(Row(
        Team=team,
        TeamCode=code.upper(),
        LogoUrl=fetch_logo(code, "light"),
        LogoUrlDark=fetch_logo(code, "dark"),
    ))

print(f"downloaded logos for {len(rows)} teams")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. write dim_team to the lakehouse

# CELL ********************

table_name = "dim_team"

df = spark.createDataFrame(rows)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✅ Data successfully written to {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
