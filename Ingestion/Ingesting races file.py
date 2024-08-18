# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting races.csv file

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

races_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Populating the data in a Dataframe

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transforming Columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

races_concated_df = races_df.withColumn('race_timestamp', to_timestamp(concat(col("date"),lit(' '), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_selected_df = races_concated_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Adding column for audit purpose

# COMMAND ----------

races_fin_df = add_ingestion_date(races_selected_df)

# COMMAND ----------

display(races_fin_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5:Writing the proccessed data

# COMMAND ----------

races_fin_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_proccessed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
