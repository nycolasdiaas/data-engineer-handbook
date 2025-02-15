-- 6 Incremental Query to generate host_activity_datelist
insert into hosts_cumulated 
with today as (
	select 
		host,
		date(event_time) as date
	from events e 
	where date(e.event_time) = date('2023-01-08')
	group by host, date(event_time)
), yesterday as (
	select 
		* 
	from hosts_cumulated
	where month_start = date('2023-01-01')
)
select 
	coalesce(t.host, y.host) as host,
	coalesce(date_trunc('month', t.date), y.month_start) as month_start,
	case 
		when y.host_activity_datelist is null
		then array[t.date]
		when t.date is null
		then y.host_activity_datelist
		else y.host_activity_datelist || array[t.date]
	end as host_activity_datelist
from today as t
full outer join yesterday as y
on t.host = y.host
on conflict(host, month_start)
do
	update set host_activity_datelist = excluded.host_activity_datelist
