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
SELECT at_bat_id,
       event_desc,
       game_date,
       game_id,
       half,
       hitter_id,
       inning,
       is_hit,
       outcome_id,
       pitcher_id,
       rbi,
       run,
       stadium_id,
       date_code,
       current_timestamp() AS ingestion_date       
  FROM mlb_analysis.silver.atbat_record
 WHERE date_code = {v_date_code}
""")
# display(df)
df.createOrReplaceTempView("fact_atbat_df")

merge_condition = "tgt.at_bat_id = src.at_bat_id"
merge_delta_data("fact_atbat_df", 'mlb_analysis', 'gold', 'fact_atbat', merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC   FROM mlb_analysis.gold.fact_atbat;
