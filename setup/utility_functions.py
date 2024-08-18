# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def ingest_files(input_df, folder_path, db_name, table_name, merge_condition, partition_col = None):
    from delta.tables import DeltaTable
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    if (spark.catalog.tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forName(spark, f"{db_name}.{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    elif partition_col:
        input_df.write.mode("overwrite").partitionBy(f"{partition_col}").format("delta").saveAsTable(f"{db_name}.{table_name}")
    else:
        input_df.write.mode("overwrite").format("delta").saveAsTable(f"{db_name}.{table_name}")
