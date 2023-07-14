-- Databricks notebook source
-- Create managed dim_pitchers table
DROP TABLE IF EXISTS mlb_analysis.gold.dim_pitchers;

CREATE TABLE IF NOT EXISTS mlb_analysis.gold.dim_pitchers
(  
  pitcher_hand STRING,
  pitcher_id STRING,
  pitcher_name STRING,
  pitcher_team_id STRING,  
  ingestion_date TIMESTAMP
);

-- COMMAND ----------

-- Create managed dim_hitters table
DROP TABLE IF EXISTS mlb_analysis.gold.dim_hitters;

CREATE TABLE IF NOT EXISTS mlb_analysis.gold.dim_hitters
(
  hitter_hand STRING,
  hitter_id STRING,
  hitter_name STRING,
  hitter_team_id STRING,  
  ingestion_date TIMESTAMP
);

-- COMMAND ----------

-- Create managed dim_stadiums table
DROP TABLE IF EXISTS mlb_analysis.gold.dim_stadiums;

CREATE TABLE IF NOT EXISTS mlb_analysis.gold.dim_stadiums
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
  longitude FLOAT,
  latitude FLOAT,  
  ingestion_date TIMESTAMP
);

-- COMMAND ----------

-- Create managed dim_calendar table
DROP TABLE IF EXISTS mlb_analysis.gold.dim_calendar;

CREATE TABLE IF NOT EXISTS mlb_analysis.gold.dim_calendar
(
  date_code INT,
  game_year INT,
  game_month INT,
  month_name INT,
  game_time TIMESTAMP  
);

-- COMMAND ----------

-- Create managed fact_atbat table
DROP TABLE IF EXISTS mlb_analysis.gold.fact_atbat;

CREATE TABLE IF NOT EXISTS mlb_analysis.gold.fact_atbat
(
  at_bat_id STRING,
  event_desc STRING,
  game_date TIMESTAMP,
  game_id STRING,
  half STRING,
  hitter_id STRING,
  inning INT,
  is_hit BOOLEAN,
  outcome_id STRING,
  pitcher_id STRING,
  rbi INT,
  run INT,
  stadium_id STRING, 
  date_code INT,
  ingestion_date TIMESTAMP
);

-- COMMAND ----------

-- Create managed dim_team table
DROP TABLE IF EXISTS mlb_analysis.gold.dim_teams;

CREATE TABLE IF NOT EXISTS mlb_analysis.gold.dim_teams
(
  team_id STRING,
  team_name STRING,  
  ingestion_date TIMESTAMP  
);
