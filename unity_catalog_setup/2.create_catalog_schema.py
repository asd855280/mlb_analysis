# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS mlb_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG mlb_analysis;
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC MANAGED LOCATION 'abfss://bronze@mlbanalysisucexternaldl.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC MANAGED LOCATION 'abfss://silver@mlbanalysisucexternaldl.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC MANAGED LOCATION 'abfss://gold@mlbanalysisucexternaldl.dfs.core.windows.net/'
