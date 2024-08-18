# Databricks notebook source
# MAGIC %run "../setup/config"

# COMMAND ----------

# MAGIC %run "../setup/utility_functions"

# COMMAND ----------

race_results_df = spark.table("f1_presentation.race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_id", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = " tgt.driver_id = src.driver_id AND tgt.race_year = src.race_year"
ingest_files(driver_standings_df, presentation_folder_path, "f1_presentation", "driver_standings", merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
