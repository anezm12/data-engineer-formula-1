# Databricks notebook source
# MAGIC %md
# MAGIC ### Mounting containers for the project
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):

    #Get secrets from key vault
    client_id = dbutils.secrets.get(scope='formula1-scope', key='client-id')
    tenant_id = dbutils.secrets.get(scope='formula1-scope', key='tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope', key='client-secret')

    # Set spark conf

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    # Mount the storage account container

    dbutils.fs.mount(source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/", 
                     mount_point = f"/mnt/{storage_account_name}/{container_name}", 
                     extra_configs = configs)
    
    #filtering mounts points 
    
    filtered_mounts = [mount for mount in dbutils.fs.mounts() if mount.mountPoint.startswith(f"/mnt/{storage_account_name}")]
    
    # Display filtered mounts
    display(filtered_mounts)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mounting containers

# COMMAND ----------

# Raw Container
mount_adls('f1de', 'raw')

# COMMAND ----------

mount_adls('f1de', 'processed')

# COMMAND ----------

mount_adls('f1de', 'presentation')

# COMMAND ----------

mount_adls('f1de', 'raw-incremental-load')

# COMMAND ----------

mount_adls('f1de', 'delta-demo')