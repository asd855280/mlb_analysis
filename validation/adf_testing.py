# Databricks notebook source
dbutils.widgets.text("response_format", "json")
response_format = dbutils.widgets.get("response_format")
response_format

# COMMAND ----------

dbutils.widgets.text("game_date", "2023-06-18")
game_date = dbutils.widgets.get("game_date")
game_date

# COMMAND ----------

print(game_date)

# COMMAND ----------

print(response_format)
