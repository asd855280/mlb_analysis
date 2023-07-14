# Databricks notebook source
# MAGIC %run "../lib/delta_ops"

# COMMAND ----------

dbutils.widgets.text("game_date", "2023-06-18")
game_date = dbutils.widgets.get("game_date")
game_date

# COMMAND ----------

year, month, date = tuple(game_date.split("-"))
v_date_code = int(year + month + date)

# COMMAND ----------

df = spark.sql("""
WITH pitcher_profile AS (
SELECT pitcher_id,
       pitcher_hand,
       pitcher_name,
       pitcher_team_id,
       row_number() OVER(PARTITION BY pitcher_id ORDER BY date_code DESC) AS newest,
       date_code
  FROM mlb_analysis.silver.atbat_record
)
SELECT pitcher_id,
       pitcher_name,
       pitcher_hand,
       pitcher_team_id,
       current_timestamp() AS ingestion_date
  FROM pitcher_profile
 WHERE newest = 1
""")

df.createOrReplaceTempView("dim_pitchers_df")

merge_condition = "tgt.pitcher_id = src.pitcher_id"
merge_delta_data("dim_pitchers_df", 'mlb_analysis', 'gold', 'dim_pitchers', merge_condition)

# COMMAND ----------

# %sql
# SELECT *
#   FROM mlb_analysis.gold.dim_pitchers;
