# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting circuits.csv file

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

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Populating the data in a Dataframe

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transforming Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId").alias("circuit_id"), col("circuitRef").alias("circuit_ref"), col("name"), col("location"), col("country"), col("lat").alias("lattitude"), col("long").alias("longitude"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Adding column for audit purpose

# COMMAND ----------

circuits_fin_df = add_ingestion_date(circuits_selected_df)

# COMMAND ----------

display(circuits_fin_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5:Writing the proccessed data

# COMMAND ----------

circuits_fin_df.write.mode("overwrite").format("delta").saveAsTable("f1_proccessed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
