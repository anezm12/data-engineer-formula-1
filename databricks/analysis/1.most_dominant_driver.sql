-- Databricks notebook source
desc f1_presentation.calculated_race_results

-- COMMAND ----------

select 
  driver_name, 
  round(sum(points)) as total, 
  round(avg(points), 2) as avg_points,
  count(1) as total_races
from 
  f1_presentation.calculated_race_results
group by driver_name
having count(1) > 100
order by avg_points desc