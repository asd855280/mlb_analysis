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

df = spark.sql(f"""
WITH hitter_profile AS (
SELECT hitter_id,
       hitter_hand,
       hitter_name,
       hitter_team_id,
       row_number() OVER(PARTITION BY hitter_id ORDER BY date_code DESC) AS newest,
       date_code
  FROM mlb_analysis.silver.atbat_record
--  WHERE date_code = {v_date_code}
)
SELECT hitter_id,
       hitter_name,
       hitter_hand,
       hitter_team_id,
       current_timestamp() AS ingestion_date
  FROM hitter_profile
 WHERE newest = 1
""")
# display(df)
df.createOrReplaceTempView("dim_hitters_df")

merge_condition = "tgt.hitter_id = src.hitter_id"
merge_delta_data("dim_hitters_df", 'mlb_analysis', 'gold', 'dim_hitters', merge_condition)

# COMMAND ----------

# %sql
# SELECT *
#   FROM mlb_analysis.gold.dim_hitters;
