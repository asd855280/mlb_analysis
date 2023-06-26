# Databricks notebook source
# MAGIC %md
# MAGIC ### Retrieve play by play data

# COMMAND ----------

import requests
import json
import time

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

mlb_key = dbutils.secrets.get(scope='mlb-scope', key='mlb-api-key')

# COMMAND ----------

year, month, date = tuple(game_date.split("-"))

# COMMAND ----------

df = spark.read.json(f"/mnt/mlbanalysisdl/raw/daily_schedule/{year}/{game_date}.json")

# COMMAND ----------

dbutils.fs.mkdirs(f"/mnt/mlbanalysisdl/raw/game_play_by_play/{year}/{month}/{date}/")

# COMMAND ----------

games = df.collect()
for game in games:
    game_id = game['game_id']
    home_team = game['home_team']
    away_team = game['away_team']
    # print(game_id, home_team, away_team)
    url = f"https://api.sportradar.com/mlb/trial/v7/en/games/{game_id}/pbp.{response_format}?api_key={mlb_key}"

    # Call API
    r = requests.get(url)
    response_body = r.content.decode('utf-8')
    json_str = json.dumps(json.loads(response_body))

    # Save Json file
    dbutils.fs.put(f"/mnt/mlbanalysisdl/raw/game_play_by_play/{year}/{month}/{date}/{home_team}-vs-{away_team}-{game_id}.json", json_str, overwrite=True)
    time.sleep(2)

