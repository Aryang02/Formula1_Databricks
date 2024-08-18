# Databricks notebook source
# MAGIC %run "../setup/config"

# COMMAND ----------

# MAGIC %run "/Workspace/Formula1/setup/utility_functions"

# COMMAND ----------

circuits_df = spark.table('f1_proccessed.circuits').withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

races_df = spark.table('f1_proccessed.races').withColumnRenamed('race_timestamp', 'race_date') \
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

drivers_df = spark.table('f1_proccessed.drivers').withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

results_df = spark.table('f1_proccessed.results')

# COMMAND ----------

constructors_df = spark.table('f1_proccessed.constructors').withColumnRenamed('name', 'team')

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(circuits_df.circuit_location, races_df.race_year, races_df.race_name, races_df.race_date, races_df.race_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

fin_df = results_df.join(circuits_races_df, circuits_races_df.race_id == results_df.race_id, "inner") \
    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id, "inner") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")

# COMMAND ----------

race_results_df = fin_df.select(races_df.race_id, races_df.race_year, drivers_df.driver_id, races_df.race_name, races_df.race_date, circuits_df.circuit_location, drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality, constructors_df.team, results_df.grid, results_df.fastest_lap, results_df.time, results_df.points, results_df.position) \
.withColumn('created_date', current_timestamp()) 

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id"
ingest_files(race_results_df, presentation_folder_path, "f1_presentation", "race_results", merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")
