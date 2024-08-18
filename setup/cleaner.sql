-- Databricks notebook source
DROP DATABASE IF EXISTS f1_proccessed CASCADE;


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_proccessed
MANAGED LOCATION "abfss://proccessed@formula1dbpjt.dfs.core.windows.net/";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
MANAGED LOCATION "abfss://presentation@formula1dbpjt.dfs.core.windows.net/";
