-- 8 An incremental query that loads host_activity_reduced
insert into host_activity_reduced 
with today as (
	select 
		host,
		date(event_time) as date,
		count(distinct user_id) as unique_visitors,
		count(1) as hit
	from events e 
	where date(e.event_time) = date('2023-01-10')
	group by host, date(event_time)
), yesterday as (
	select 
		* 
	from host_activity_reduced
	where month_start = date('2023-01-01')
)
select 
	coalesce(t.host, y.host) as host,
	coalesce(date_trunc('month', t.date), y.month_start) as month_start,
	case 
		when y.hit_array is not null
		then y.hit_array || array[coalesce(t.hit, 0)]
		when y.hit_array is null
		then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(t.hit, 0)]
	end as hit_array,
	case 
		when y.unique_visitors is not null
		then y.unique_visitors || array[coalesce(t.unique_visitors, 0)]
		when y.unique_visitors is null
		then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(t.unique_visitors, 0)]
	end as unique_visitors
from today as t
full outer join yesterday as y
on t.host = y.host
on conflict(host, month_start)
do
	update set unique_visitors = excluded.unique_visitors,
		hit_array = excluded.hit_array