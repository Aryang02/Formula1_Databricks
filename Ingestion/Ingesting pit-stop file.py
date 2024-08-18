# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting pit-stops.json file

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

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Populating the data in a Dataframe

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine", True).json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transforming Columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

pit_stops_transformed_df = add_ingestion_date(pit_stops_df) \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

display(pit_stops_transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4:Writing the proccessed data

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id"
ingest_files(pit_stops_transformed_df, proccessed_folder_path, "f1_proccessed", "pit_stops", merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
