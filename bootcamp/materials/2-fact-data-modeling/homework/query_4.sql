-- 4 - 	
with user_devices as (
	select * from user_devices_cumulated
	where date = date('2023-01-31')
), series as (
select * 
from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') as series_date
), placeholder_ints as (
	select 
		case 
			when device_activity_datelist @> array[date(s.series_date)]
			then cast(pow(2, 32 - (date - date(s.series_date))-1) as bigint)
			else 0
		end	as placeholder_int_value, *
	from user_devices as ud
	cross join series as s
)
select 
	user_id,
	device_id,
	browser_type,
	device_activity_datelist,
	cast(cast(sum(p.placeholder_int_value) as bigint) as bit(32)) as datelist_int
from placeholder_ints as p
group by user_id, device_id, browser_type, device_activity_datelist
