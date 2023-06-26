# Databricks notebook source
# MAGIC %md
# MAGIC ## Retrieve Game info for the imput date

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %run "../lib/json_parsing"

# COMMAND ----------

dbutils.widgets.text("response_format", "json")
response_format = dbutils.widgets.get("response_format")
response_format

# COMMAND ----------

dbutils.widgets.text("game_date", "2023-06-18")
game_date = dbutils.widgets.get("game_date")
game_date

# COMMAND ----------

year, month, date = tuple(game_date.split("-"))

# COMMAND ----------

mlb_key = dbutils.secrets.get(scope='mlb-scope', key='mlb-api-key')
url = f"https://api.sportradar.com/mlb/trial/v7/en/games/{year}/{month}/{date}/schedule.{response_format}?api_key={mlb_key}"

# COMMAND ----------

r = requests.get(url)
response_body = r.content.decode('utf-8')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform
# MAGIC 1. Retrieve game_id, game_time, stadium, home team and away team name

# COMMAND ----------

import json
json_str = json.loads(response_body)
trim_json = parse_game_info(json_str)
trim_json_str = str(trim_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Json File
# MAGIC 1. To Azure Data Lake Storage Gen 2 Container

# COMMAND ----------

dbutils.fs.mkdirs(f"/mnt/mlbanalysisdl/raw/daily_schedule/{year}/")

dbutils.fs.put(f"/mnt/mlbanalysisdl/raw/daily_schedule/{year}/{game_date}.json", trim_json_str, overwrite=True)

# COMMAND ----------

# Test
# df = spark.read.json(f"/mnt/mlbanalysisdl/raw/daily_schedule/{year}/{game_date}.json")
# display(df)
