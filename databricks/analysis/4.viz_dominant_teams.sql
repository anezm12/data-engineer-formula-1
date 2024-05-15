-- Databricks notebook source
with cte1 as (
  select 
  constructor, 
  round(sum(points)) as total_points, 
  round(avg(points), 2) as avg_points,
  count(1) as total_races,
  rank() over(order by avg(points) desc) as constructor_rank
from 
  f1_presentation.calculated_race_results
group by constructor
having count(1) >= 100
order by avg_points desc),
cte2 (
select 
  race_year,
  constructor, 
  count(1) as total_races,
  round(sum(points)) as total_points, 
  round(avg(points), 2) as avg_points
from 
  f1_presentation.calculated_race_results
group by race_year, constructor
order by  race_year, avg_points desc
)
select cte2.race_year, cte1.constructor, cte2.total_races, cte2.total_points, cte1.avg_points
from cte1
join cte2 on cte1.constructor = cte2.constructor
where cte1.constructor_rank <= 5
order by cte2.race_year

