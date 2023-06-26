-- Databricks notebook source
-- Create managed table
DROP TABLE IF EXISTS mlb_analysis.silver.atbat_record;

CREATE TABLE IF NOT EXISTS mlb_analysis.silver.atbat_record
(
  at_bat_id STRING,
  event_desc STRING,
  game_date TIMESTAMP,
  game_id STRING,
  half STRING,
  hitter_hand STRING,
  hitter_id STRING,
  hitter_name STRING,
  hitter_team_id STRING,
  inning INT,
  is_hit BOOLEAN,
  outcome_id STRING,
  pitcher_hand STRING,
  pitcher_id STRING,
  pitcher_name STRING,
  pitcher_team_id STRING,
  rbi INT,
  run INT,
  stadium_id STRING,
  stadium_name STRING,
  game_year INT,
  game_month INT,
  date_code INT,
  ingestion_date TIMESTAMP
);
