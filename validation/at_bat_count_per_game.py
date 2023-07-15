# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT game_id, date_code, count(1)
# MAGIC   FROM mlb_analysis.silver.atbat_record
# MAGIC GROUP BY game_id, date_code
# MAGIC ORDER BY date_code

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.pitcher_id,
# MAGIC        p.pitcher_name,
# MAGIC        p.pitcher_hand,
# MAGIC        t.team_name
# MAGIC   FROM mlb_analysis.gold.dim_pitchers p
# MAGIC   LEFT JOIN mlb_analysis.gold.dim_teams t ON p.pitcher_team_id = t.team_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT h.hitter_id,
# MAGIC        h.hitter_name,
# MAGIC        h.hitter_hand,
# MAGIC        t.team_name
# MAGIC   FROM mlb_analysis.gold.dim_hitters h 
# MAGIC   LEFT JOIN mlb_analysis.gold.dim_teams t ON h.hitter_team_id = t.team_id
