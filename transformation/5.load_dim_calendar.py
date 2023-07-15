# Databricks notebook source
# MAGIC %run "../lib/delta_ops"

# COMMAND ----------

dbutils.widgets.text("game_date", "2023-06-18")
game_date = dbutils.widgets.get("game_date")
game_date

# COMMAND ----------

year, month, date = tuple(game_date.split("-"))
v_date_code = int(year + month + date)
v_game_month = int(year + month)

# COMMAND ----------

df = spark.sql(f"""
SELECT {v_date_code} AS date_code,
       {year} AS game_year,
       {v_game_month} AS game_month,
       {month} AS month_name,
       MIN(game_date) AS game_time
  FROM mlb_analysis.silver.atbat_record
 WHERE date_code = {v_date_code}
""")
# display(df)
df.createOrReplaceTempView("dim_calendar_df")

merge_condition = "tgt.date_code = src.date_code"
merge_delta_data("dim_calendar_df", 'mlb_analysis', 'gold', 'dim_calendar', merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM mlb_analysis.gold.dim_calendar
# MAGIC ORDER BY date_code;
