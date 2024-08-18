-- Databricks notebook source
-- ALL TIME BEST DRIVERS

DROP TABLE IF EXISTS f1_presentation.all_time_best_drivers;
CREATE TABLE f1_presentation.all_time_best_drivers
AS
SELECT driver_name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races>=50
ORDER BY avg_total_points DESC;

-- COMMAND ----------

-- Best drivers betwwen 2011 and 2020

DROP TABLE IF EXISTS f1_presentation.best_drivers_2011_to_2020;
CREATE TABLE f1_presentation.best_drivers_2011_to_2020
AS
SELECT driver_name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING total_races>=50
ORDER BY avg_total_points DESC;

-- COMMAND ----------

-- Best drivers betwwen 2001 and 2010

DROP TABLE IF EXISTS f1_presentation.best_drivers_2001_to_2010;
CREATE TABLE f1_presentation.best_drivers_2001_to_2010
AS
SELECT driver_name,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING total_races>=50
ORDER BY avg_total_points DESC;
