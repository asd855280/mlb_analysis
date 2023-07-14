# Databricks notebook source
# MAGIC %run "../lib/delta_ops"

# COMMAND ----------

dbutils.widgets.text("game_date", "2023-06-18")
game_date = dbutils.widgets.get("game_date")
game_date

# COMMAND ----------

year, month, date = tuple(game_date.split("-"))
v_date_code = int(year + month + date)
df = spark.read.json(f"/mnt/mlbanalysisdl/raw/daily_schedule/{year}/{game_date}.json")
games = df.collect()

# COMMAND ----------

import json
from pyspark.sql.functions import lit, current_timestamp

team_list = []
for game in games:
    game_id = game['game_id']
    home_team = game['home_team']
    away_team = game['away_team']
    pbp_file_name = f"/mnt/mlbanalysisdl/raw/game_play_by_play/{year}/{month}/{date}/{home_team}-vs-{away_team}-{game_id}.json"
    # Read json file
    json_content = dbutils.fs.head(pbp_file_name, 1000000)
    json_obj = json.loads(json_content)

    # Parsing team names, and match them with team IDs
    home_team_id = json_obj['game']['home_team']
    away_team_id = json_obj['game']['away_team']

    team1_json = {}
    team1_json.update({
        'team_id': home_team_id,
        'team_name': home_team
    })
    team2_json = {}
    team2_json.update({
        'team_id': away_team_id,
        'team_name': away_team
    })
    team_list.append(json.dumps(team1_json))
    team_list.append(json.dumps(team2_json))

# COMMAND ----------

# Transfer selected data into dataframe
team_json_RDD = sc.parallelize(team_list)
team_df = spark.read.json(team_json_RDD)
team_df = team_df.dropDuplicates()

# COMMAND ----------

final_team_df = team_df.withColumn('ingestion_date', current_timestamp())
# display(final_team_df)

# COMMAND ----------

final_team_df.createOrReplaceTempView("final_team_df")
merge_condition = "tgt.team_id = src.team_id"
merge_delta_data("final_team_df", 'mlb_analysis', 'gold', 'dim_teams', merge_condition)

# COMMAND ----------

# %sql
# SELECT *
#   FROM mlb_analysis.gold.dim_teams;
