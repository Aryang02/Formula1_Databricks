# Databricks notebook source
# MAGIC %md 
# MAGIC #ADLS Access using SAS Token

# COMMAND ----------

access_key = dbutils.secrets.get(scope = 'formula1-storage-access-token', key='formula1db-sas')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dbpjt.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dbpjt.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dbpjt.dfs.core.windows.net", access_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://formula1@formula1dbpjt.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://formula1@formula1dbpjt.dfs.core.windows.net/circuits.csv"))
