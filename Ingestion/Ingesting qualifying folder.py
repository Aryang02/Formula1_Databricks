# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting Qualifying folder

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

qualifying_schema = StructType(fields = [
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Populating the data in a Dataframe

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiLine", True).json(f'{raw_folder_path}/{v_file_date}/qualifying')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transforming Columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

qualifying_transformed_df = add_ingestion_date(qualifying_df) \
    .withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('constructorId', 'constructor_id')

# COMMAND ----------

display(qualifying_transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4:Writing the proccessed data

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
ingest_files(qualifying_transformed_df, proccessed_folder_path, "f1_proccessed", "qualifying", merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
