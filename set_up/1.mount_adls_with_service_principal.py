# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake using Service Principal
# MAGIC 1. Set workspace secret scope
# MAGIC 2. Get client_id, tenant_id and client_secret from key vault
# MAGIC 3. Set Spark config with App/ Client_id, Directory/ tenant_id & Secret
# MAGIC 4. Call file system utility mount to mount the storage
# MAGIC 5. Explore other file systems utilities to mount

# COMMAND ----------


