-- Databricks notebook source
-- External bronze table, when creating table with specified file path, it created an external table 
DROP TABLE IF EXISTS mlb_analysis.bronze.pitch_outcome_codes;

CREATE TABLE IF NOT EXISTS mlb_analysis.bronze.pitch_outcome_codes
(
  id STRING,
  desc STRING
)
USING json
OPTIONS (path "abfss://bronze@mlbanalysisucexternaldl.dfs.core.windows.net/glossary/pitch_outcome_code.json");

-- COMMAND ----------

-- External bronze table, when creating table with specified file path, it created an external table 
DROP TABLE IF EXISTS mlb_analysis.bronze.stadium_info;

CREATE TABLE IF NOT EXISTS mlb_analysis.bronze.stadium_info
(
  stadium_id STRING,
  stadium_name STRING,
  stadium_type STRING,
  capacity INT,
  country STRING,
  state STRING,
  city STRING,
  zip_code INT,
  field_orientation STRING,
  longitude NUMBER,
  latitude NUMBER
)
USING json
OPTIONS (path "abfss://bronze@mlbanalysisucexternaldl.dfs.core.windows.net/venue/stadium_info.json");
