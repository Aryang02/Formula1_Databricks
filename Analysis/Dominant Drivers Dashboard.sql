-- Databricks notebook source
-- Link to Dashboard
-- https://adb-28304223513374.14.azuredatabricks.net/?o=28304223513374#notebook/570068401081171/dashboard/1cf4ddb0-fe97-4bc3-97a7-e5a8321ddd15/present

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT Driver_Name,
count(1) total_races,
SUM(calculated_points) total_points,
AVG(calculated_points) avg_total_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY Driver_Name
HAVING total_races>=50
ORDER BY avg_total_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers_2011_to_2020
AS
SELECT Driver_Name,
count(1) total_races,
SUM(calculated_points) total_points,
AVG(calculated_points) avg_total_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY Driver_Name
ORDER BY avg_total_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers_2001_to_2010
AS
SELECT Driver_Name,
count(1) total_races,
SUM(calculated_points) total_points,
AVG(calculated_points) avg_total_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY Driver_Name
ORDER BY avg_total_points DESC;

-- COMMAND ----------

-- ALL TIME TOP 10 BEST DRIVERS

SELECT race_year, Driver_Name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank<=10)
GROUP BY race_year, Driver_Name
ORDER BY race_year, avg_total_points DESC;

-- COMMAND ----------

-- Best drivers betwwen 2011 and 2020

SELECT race_year, Driver_Name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers_2011_to_2020 WHERE (driver_rank <= 10) AND (race_year BETWEEN 2011 AND 2020))
GROUP BY race_year, Driver_Name
ORDER BY race_year, avg_total_points DESC;

-- COMMAND ----------

-- Best drivers betwwen 2001 and 2010

SELECT race_year, Driver_Name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers_2001_to_2010 WHERE (driver_rank<=10) AND (race_year BETWEEN 2001 AND 2010))
GROUP BY race_year, Driver_Name
ORDER BY race_year, avg_total_points DESC;
