# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting lap_time folder

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../setup/config"

# COMMAND ----------

# MAGIC %run "../setup/utility_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Creating the schema for data ingestion

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

lap_time_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Populating the data in a Dataframe

# COMMAND ----------

lap_time_df = spark.read.schema(lap_time_schema).csv(f'{raw_folder_path}/{v_file_date}/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transforming Columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

lap_time_transformed_df = add_ingestion_date(lap_time_df) \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

display(lap_time_transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4:Writing the proccessed data

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id"
ingest_files(lap_time_transformed_df, proccessed_folder_path, "f1_proccessed", "lap_times", merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
