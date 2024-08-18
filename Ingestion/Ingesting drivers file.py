# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting drivers.json file

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

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType

# COMMAND ----------

name_schema = StructType(fields = [
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields = [
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Populating the data in a Dataframe

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transforming Columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat

# COMMAND ----------

drivers_new_df = drivers_df.drop(col('url'))

# COMMAND ----------

drivers_transformed_df = add_ingestion_date(drivers_new_df).withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))).withColumnRenamed('driverId', 'driver_id').withColumnRenamed('driverRef', 'driver_ref')

# COMMAND ----------

display(drivers_transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4:Writing the proccessed data

# COMMAND ----------

drivers_transformed_df.write.mode("overwrite").format("delta").saveAsTable("f1_proccessed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
