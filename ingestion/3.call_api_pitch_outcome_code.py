# Databricks notebook source
import requests
import json
import os

# COMMAND ----------

dbutils.widgets.text("response_format", "json")
response_format = dbutils.widgets.get("response_format")
response_format

# COMMAND ----------

mlb_key = dbutils.secrets.get(scope='mlb-scope', key='mlb-api-key')
url = f"https://api.sportradar.com/mlb/trial/v7/en/league/glossary.{response_format}?api_key={mlb_key}"

# COMMAND ----------

r = requests.get(url)
response_body = r.content.decode('utf-8')
glossary_json = json.loads(response_body)
pitch_outcomes = glossary_json['pitch_outcomes']
json_str = json.dumps(pitch_outcomes)

# COMMAND ----------

dbutils.fs.put(f"abfss://bronze@mlbanalysisucexternaldl.dfs.core.windows.net/glossary/pitch_outcome_code.json", json_str, overwrite=True)
