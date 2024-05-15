-- Databricks notebook source
select 
  driver_name, 
  round(sum(points)) as total_points, 
  round(avg(points), 2) as avg_points,
  count(1) as total_races,
  rank() over(order by avg(points) desc) as driver_rank
from 
  f1_presentation.calculated_race_results
group by driver_name
having count(1) >= 50
order by avg_points desc

-- COMMAND ----------

select count(1) from f1_presentation.calculated_race_results

-- COMMAND ----------

  select 
    race_year,
    driver_name, 
    count(1) as total_races,
    round(sum(points)) as total_points, 
    round(avg(points), 2) as avg_points

from 
  f1_presentation.calculated_race_results
group by race_year, driver_name
order by  race_year, avg_points desc

-- COMMAND ----------

with cte as (select 
  driver_name, 
  round(sum(points)) as total_points, 
  round(avg(points), 2) as avg_points,
  count(1) as total_races,
  rank() over(order by avg(points) desc) as driver_rank
from 
  f1_presentation.calculated_race_results
group by driver_name
having count(1) >= 50
order by avg_points desc)
select 
  r.race_year,
  c.driver_name, 
  c.total_races,
  c.total_points, 
  c.avg_points
from cte c
join f1_presentation.calculated_race_results r on c.driver_name = r.driver_name
where c.driver_rank <= 10
order by r.race_year, c.avg_points desc

-- COMMAND ----------

with cte1 as (
  select 
  driver_name, 
  round(sum(points)) as total_points, 
  round(avg(points), 2) as avg_points,
  count(1) as total_races,
  rank() over(order by avg(points) desc) as driver_rank
from 
  f1_presentation.calculated_race_results
group by driver_name
having count(1) >= 50
order by avg_points desc),
cte2 (
select 
  race_year,
  driver_name, 
  count(1) as total_races,
  round(sum(points)) as total_points, 
  round(avg(points), 2) as avg_points
from 
  f1_presentation.calculated_race_results
group by race_year, driver_name
order by  race_year, avg_points desc
)
select cte2.race_year, cte1.driver_name, cte2.total_races, cte2.total_points, cte1.avg_points
from cte1
join cte2 on cte1.driver_name = cte2.driver_name
where cte1.driver_rank <= 10
order by cte2.race_year

