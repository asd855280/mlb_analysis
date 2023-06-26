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
