-- 4- Full backfill
insert into actors_history_scd
with with_previous as (
	select 
		actorid,
		current_year,
		quality_class,
		is_active,
		lag(quality_class, 1) over(partition by actorid order by current_year) as previous_quality_class,
		lag(is_active, 1) over(partition by actorid order by current_year) as previous_is_active
	from actors a 
	where current_year <= 2021
),
	with_indicator as (
	select *,
		case 
			when quality_class <> previous_quality_class then 1
			when is_active <> previous_is_active then 1
			else 0
		end as change_indicator
	from with_previous	
), 
	with_streaks as (
	select 
		*, 
		sum(change_indicator) over(partition by actorid order by current_year) as streak_identifier
	from with_indicator
	order by actorid, streak_identifier desc 
)
select 
	actorid,
	is_active,
	quality_class,
	2020 as current_year,
	min(current_year) as start_date,
	max(current_year) as end_date
from with_streaks
group by actorid, streak_identifier, is_active, quality_class
order by actorid, start_date;