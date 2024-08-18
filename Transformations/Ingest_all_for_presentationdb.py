# Databricks notebook source
res = dbutils.notebook.run("race_results", 300)
display(res)

# COMMAND ----------

res = dbutils.notebook.run("driver_standings", 300)
display(res)

# COMMAND ----------

res = dbutils.notebook.run("constructor_standings", 300)
display(res)

# COMMAND ----------

res = dbutils.notebook.run("Calculated_race_results", 300)
display(res)
