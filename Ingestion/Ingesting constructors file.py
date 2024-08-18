# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-23")
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

constructors_schema = StructType(fields = [
    StructField("constructorId", IntegerType(), False),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Populating the data in a Dataframe

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Transforming Columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_new_df = constructors_df.drop(col('url'))

# COMMAND ----------

constructors_fin_df = add_ingestion_date(constructors_new_df).withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

display(constructors_fin_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4:Writing the proccessed data

# COMMAND ----------

constructors_fin_df.write.mode("overwrite").format("delta").saveAsTable("f1_proccessed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
