# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
    (
    race_year STRING,
    team STRING,
    driver_id INT,
    driver_name STRING,
    race_id INT,
    points INT,
    position INT,
    calculated_points INT,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------


spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW race_result_updated
            AS
            SELECT r.race_year,
            c.name AS team,
            d.driver_id,
            d.name AS driver_name,
            r.race_id,
            res.points,
            res.position,
            11 - res.position AS calculated_points
            FROM f1_proccessed.results AS res
            JOIN f1_proccessed.drivers AS d ON (res.driver_id = d.driver_id)
            JOIN f1_proccessed.constructors AS c ON (res.constructor_id = c.constructor_id)
            JOIN f1_proccessed.races AS r ON (r.race_id = res.race_id)
            WHERE res.position <= 10 AND res.file_date = '{v_file_date}';
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_result_updated src
# MAGIC ON (tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id)
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.position = src.position,
# MAGIC              tgt.points = src.points,
# MAGIC              tgt.calculated_points = src.calculated_points,
# MAGIC              tgt.updated_date = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (race_year, team, driver_id, driver_name, race_id, points, position, calculated_points, created_date)
# MAGIC   VALUES (race_year, team, driver_id, driver_name, race_id, points, position, calculated_points, current_timestamp)
