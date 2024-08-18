# Databricks notebook source
# MAGIC %md 
# MAGIC #ADLS Access using Service Principle

# COMMAND ----------

def mount_adls(storage_acc_name, containers):
    #Get Secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1-storage-access-token', key='client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-storage-access-token', key='tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-storage-access-token', key='app-secret')
    
    #Get Configs:
    configs = {
        "fs.azure.account.auth.type" : "OAuth",
        "fs.azure.account.oauth.provider.type" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id" : client_id,
        "fs.azure.account.oauth2.client.secret" : client_secret,
        "fs.azure.account.oauth2.client.endpoint" : f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    for container_name in containers:
        #Unmount container if already mounted
        if any(mount.mountPoint == f"/mnt/{storage_acc_name}/{container_name}" for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(f"/mnt/{storage_acc_name}/{container_name}")
        #Mount container
        dbutils.fs.mount(
            source = f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storage_acc_name}/{container_name}",
            extra_configs = configs
        )
    
    #Display mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

containers = ['raw', 'proccessed', 'presentation']
storage_acc_name = 'formula1dbpjt'
mount_adls(storage_acc_name, containers)

# COMMAND ----------

# display(dbutils.fs.ls("abfss://formula1@formula1dbpjt.dfs.core.windows.net"))

# COMMAND ----------

# display(spark.read.csv("abfss://formula1@formula1dbpjt.dfs.core.windows.net/circuits.csv"))
