-- 7 -- a monthly, reduced fact table DDL host_activity_reduced
create table host_activity_reduced (
	host text,
	month_start date,
	hit_array bigint[],
	unique_visitors bigint[],
	primary key (host, month_start)
);