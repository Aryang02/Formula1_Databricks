-- Databricks notebook source
-- MAGIC %run "../setup/config"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

SHOW TABLES IN f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  long DOUBLE,
  alt INT,
  url STRING
)
USING CSV
OPTIONS (path "abfss://raw@formula1dbpjt.dfs.core.windows.net/circuits.csv", header "true");

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (path "abfss://raw@formula1dbpjt.dfs.core.windows.net/races.csv", header true);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://raw@formula1dbpjt.dfs.core.windows.net/constructors.json");

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename:STRING,surename:STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://raw@formula1dbpjt.dfs.core.windows.net/drivers.json");

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed INT,
  statusId STRING
)
USING json
OPTIONS (path "abfss://raw@formula1dbpjt.dfs.core.windows.net/results.json")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop STRING,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING json
OPTIONS (path "abfss://raw@formula1dbpjt.dfs.core.windows.net/pit_stops.json", multiLine 'true')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_time(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS (path "abfss://raw@formula1dbpjt.dfs.core.windows.net/lap_times")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS (path "abfss://raw@formula1dbpjt.dfs.core.windows.net/qualifying", multiLine True)
