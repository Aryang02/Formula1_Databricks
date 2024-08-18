# Databricks notebook source
dates = ["2021-03-21", "2021-03-28", "2021-04-18"]

# COMMAND ----------

for date in dates:
    res = dbutils.notebook.run(
        "Ingesting circuits file",
        300,
        {"p_file_date": date}
    )
print(res)

# COMMAND ----------

for date in dates:
    res = dbutils.notebook.run("Ingesting races file", 300, {"p_file_date": date})
display(res)

# COMMAND ----------

for date in dates:
    res = dbutils.notebook.run("Ingesting constructors file", 300, {"p_file_date": date})
display(res)

# COMMAND ----------

for date in dates:
    res = dbutils.notebook.run("Ingesting drivers file", 300, {"p_file_date": date})
display(res)

# COMMAND ----------

for date in dates:
    res = dbutils.notebook.run("Ingesting lap-time folder", 300, {"p_file_date": date})
display(res)

# COMMAND ----------

for date in dates:
    res = dbutils.notebook.run("Ingesting pit-stop file", 300, {"p_file_date": date})
display(res)

# COMMAND ----------

for date in dates:
    res = dbutils.notebook.run("Ingesting qualifying folder", 300, {"p_file_date": date})
display(res)

# COMMAND ----------

for date in dates:
    res = dbutils.notebook.run("Ingesting results file", 300, {"p_file_date": date})
display(res)
