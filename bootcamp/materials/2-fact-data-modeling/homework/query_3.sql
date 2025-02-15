-- 3 device_activity_datelist
insert into user_devices_cumulated
with today as (
	select 
		e.user_id,
		date(e.event_time) as date,
		e.device_id,
		d.browser_type,
		row_number() over(partition by e.user_id, e.device_id, d.browser_type) as row_num
	from events as e
	left join devices as d
	on e.device_id = d.device_id
	where date(e.event_time) = ('2023-01-31')
	and e.user_id is not null 
	and e.device_id is not null
), 
	deduped_today as (
	select 
		* 
	from today
	where row_num = 1
), yesterday as (
	select * from user_devices_cumulated 
	where date = date('2023-01-30')
)
select 
	coalesce(t.user_id, y.user_id) as user_id,
	coalesce(t.device_id, y.device_id) as device_id,
	coalesce(t.browser_type, y.browser_type) as browser_type,
	coalesce(t.date, y.date + 1) as date,
	case 
		when y.device_activity_datelist is null
			then array[t.date]
		when t.date is null
			then y.device_activity_datelist
		else y.device_activity_datelist || array[t.date]
	end as device_activity_datelist	
from deduped_today as t
full outer join yesterday as y
	on y.user_id = t.user_id
	and y.device_id = t.device_id
	and y.browser_type = t.browser_type