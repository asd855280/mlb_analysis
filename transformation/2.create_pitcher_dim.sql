-- Databricks notebook source
WITH pitcher_profile AS (
SELECT pitcher_id,
       pitcher_hand,
       pitcher_name,
       pitcher_team_id,
       row_number() OVER(PARTITION BY pitcher_id ORDER BY date_code DESC) AS newest,
       date_code
  FROM mlb_analysis.silver.atbat_record
)
SELECT pitcher_id,
       pitcher_name,
       pitcher_hand,
       pitcher_team_id
  FROM pitcher_profile
 WHERE newest = 1

-- COMMAND ----------

SELECT max(date_code) 
  FROM mlb_analysis.silver.atbat_record;
