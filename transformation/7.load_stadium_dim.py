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

stadium_list = []
for game in games:
    game_id = game['game_id']
    home_team = game['home_team']
    away_team = game['away_team']
    pbp_file_name = f"/mnt/mlbanalysisdl/raw/game_play_by_play/{year}/{month}/{date}/{home_team}-vs-{away_team}-{game_id}.json"
    # Read json file
    json_content = dbutils.fs.head(pbp_file_name, 1000000)
    json_obj = json.loads(json_content)

    # Parsing stadium info
    venue_json = json_obj['game']['venue']
    venue_json.update({
        'latitude': json_obj['game']['venue']['location']['lat'],
        'longitude': json_obj['game']['venue']['location']['lng']
    })
    del venue_json['location']
    stadium_list.append(json.dumps(venue_json))

# COMMAND ----------

# Transfer selected data into dataframe
stadium_json_RDD = sc.parallelize(stadium_list)
stadium_df = spark.read.json(stadium_json_RDD)
stadium_df = stadium_df.dropDuplicates()

# COMMAND ----------

final_stadium_df = stadium_df.withColumn('ingestion_date', current_timestamp()) \
                             .withColumnRenamed('name', 'stadium_name') \
                             .withColumnRenamed('id', 'stadium_id') \
                             .withColumnRenamed('zip', 'zip_code') \
                             .drop('address') \
                             .drop('market') \
                             .drop('surface') \
                             .drop('time_zone')
# display(final_stadium_df)

# COMMAND ----------

final_stadium_df.createOrReplaceTempView("final_stadium_df")
merge_condition = "tgt.stadium_id = src.stadium_id"
merge_delta_data("final_stadium_df", 'mlb_analysis', 'gold', 'dim_stadiums', merge_condition)

# COMMAND ----------

# %sql
# SELECT *
#   FROM mlb_analysis.gold.dim_stadiums;
