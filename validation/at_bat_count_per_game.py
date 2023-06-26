# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT game_id, date_code, count(1)
# MAGIC   FROM mlb_analysis.silver.atbat_record
# MAGIC GROUP BY game_id, date_code
# MAGIC ORDER BY date_code
