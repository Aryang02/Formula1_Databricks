# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting results.json file

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

results_schema = StructType(fields = [
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True),
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Populating the data in a Dataframe

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transforming Columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat

# COMMAND ----------

results_new_df = results_df.drop(col('statusId'))

# COMMAND ----------

results_transformed_df = add_ingestion_date(results_new_df) \
    .withColumnRenamed('resultId', 'result_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('positionText', 'position_text') \
    .withColumnRenamed('positionOrder', 'position_order') \
    .withColumnRenamed('fastestLap', 'fastest_lap') \
    .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4:Writing the proccessed data

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
ingest_files(results_transformed_df, proccessed_folder_path, "f1_proccessed", "results", merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")
