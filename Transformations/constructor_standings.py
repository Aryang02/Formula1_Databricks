# Databricks notebook source
# MAGIC %run "../setup/config"

# COMMAND ----------

# MAGIC %run "../setup/utility_functions"

# COMMAND ----------

race_results_df = spark.table("f1_presentation.race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
ingest_files(final_df, presentation_folder_path, "f1_presentation", "constructor_standings", merge_condition, "race_year")

# COMMAND ----------

dbutils.notebook.exit("Success")
