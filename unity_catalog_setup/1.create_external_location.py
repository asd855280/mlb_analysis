# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION mlbanalysis_uc_external_bronze
# MAGIC  URL 'abfss://bronze@mlbanalysisucexternaldl.dfs.core.windows.net/'
# MAGIC  WITH (STORAGE CREDENTIAL `mlb_analysis_external_credential`);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION mlbanalysis_uc_external_silver
# MAGIC  URL 'abfss://silver@mlbanalysisucexternaldl.dfs.core.windows.net/'
# MAGIC  WITH (STORAGE CREDENTIAL `mlb_analysis_external_credential`);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION mlbanalysis_uc_external_gold
# MAGIC  URL 'abfss://gold@mlbanalysisucexternaldl.dfs.core.windows.net/'
# MAGIC  WITH (STORAGE CREDENTIAL `mlb_analysis_external_credential`);
