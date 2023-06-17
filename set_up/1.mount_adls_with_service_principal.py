# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake using Service Principal
# MAGIC 1. Set workspace secret scope
# MAGIC 2. Get client_id, tenant_id and client_secret from key vault
# MAGIC 3. Set Spark config with App/ Client_id, Directory/ tenant_id & Secret
# MAGIC 4. Call file system utility mount to mount the storage
# MAGIC 5. Explore other file systems utilities to mount

# COMMAND ----------

def mount_adls(storage_account_name: str, container_name: str):
    client_id = dbutils.secrets.get(scope='mlb-scope', key='mlb-app-AD-client-id')
    tenant_id = dbutils.secrets.get(scope='mlb-scope', key='mlb-app-AD-tenant-id')
    client_secret = dbutils.secrets.get(scope='mlb-scope', key='mlb-app-AD-client-secret')

    configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    for each in dbutils.fs.mounts():
        if f'/mnt/{storage_account_name}/{container_name}' == each.mountPoint:
            dbutils.fs.unmount(f'/mnt/{storage_account_name}/{container_name}')
    
    dbutils.fs.mount(
                    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                    mount_point = f"/mnt/{storage_account_name}/{container_name}",
                    extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('mlbanalysisdl' ,'raw')
