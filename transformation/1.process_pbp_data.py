# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %run "../lib/json_parsing"

# COMMAND ----------

# MAGIC %run "../lib/delta_ops"

# COMMAND ----------

dbutils.widgets.text("game_date", "2023-06-18")
game_date = dbutils.widgets.get("game_date")
game_date

# COMMAND ----------

year, month, date = tuple(game_date.split("-"))
df = spark.read.json(f"/mnt/mlbanalysisdl/raw/daily_schedule/{year}/{game_date}.json")
games = df.collect()

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

for game in games:
    game_id = game['game_id']
    home_team = game['home_team']
    away_team = game['away_team']
    pbp_file_name = f"/mnt/mlbanalysisdl/raw/game_play_by_play/{year}/{month}/{date}/{home_team}-vs-{away_team}-{game_id}.json"
    # Read json file
    json_content = dbutils.fs.head(pbp_file_name, 1000000)
    json_obj = json.loads(json_content)
    
    # Extract fields that we need
    pbp_list = parse_pbp_info(json_obj)
    if pbp_list == 'postponed':
        continue

    # Transfer selected data into dataframe
    pbp_json_RDD = sc.parallelize(pbp_list)
    pbp_df = spark.read.json(pbp_json_RDD)

    final_pbp_df = pbp_df.withColumn("game_year", lit(year)) \
                         .withColumn("game_month", lit(year + month)) \
                         .withColumn("date_code", lit(year + month + date)) \
                         .withColumn("ingestion_date", current_timestamp())

    final_pbp_df.createOrReplaceTempView("final_pbp_df")
    merge_condition = "tgt.at_bat_id = src.at_bat_id"
    merge_delta_data("final_pbp_df", 'mlb_analysis', 'silver', 'atbat_record', merge_condition)
