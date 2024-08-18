-- Databricks notebook source
-- Link to Dashboard
-- https://adb-28304223513374.14.azuredatabricks.net/?o=28304223513374#notebook/570068401081171/dashboard/5519c91e-1444-4887-807d-2d66eec47244/present

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT Team,
count(1) total_races,
SUM(calculated_points) total_points,
AVG(calculated_points) avg_total_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
GROUP BY Team
HAVING total_races>=50
ORDER BY avg_total_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams_2011_to_2020
AS
SELECT Team,
count(1) total_races,
SUM(calculated_points) total_points,
AVG(calculated_points) avg_total_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY Team
ORDER BY avg_total_points DESC;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams_2001_to_2010
AS
SELECT Team,
count(1) total_races,
SUM(calculated_points) total_points,
AVG(calculated_points) avg_total_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY Team
ORDER BY avg_total_points DESC;

-- COMMAND ----------

-- ALL TIME TOP 10 BEST TEAMS

SELECT race_year, Team,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
WHERE Team IN (SELECT Team FROM v_dominant_teams WHERE team_rank<=10)
GROUP BY race_year, Team
ORDER BY race_year, avg_total_points DESC;

-- COMMAND ----------

-- Best Teams betwwen 2011 and 2020

SELECT race_year, Team,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
WHERE Team IN (SELECT Team FROM v_dominant_teams_2011_to_2020 WHERE (team_rank <= 10) AND (race_year BETWEEN 2011 AND 2020))
GROUP BY race_year, Team
ORDER BY race_year, avg_total_points DESC;

-- COMMAND ----------

-- Best Teams betwwen 2001 and 2010

SELECT race_year, Team,
count(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) as avg_total_points
FROM f1_presentation.calculated_race_results
WHERE Team IN (SELECT Team FROM v_dominant_teams_2001_to_2010 WHERE (team_rank<=10) AND (race_year BETWEEN 2001 AND 2010))
GROUP BY race_year, Team
ORDER BY race_year, avg_total_points DESC;
