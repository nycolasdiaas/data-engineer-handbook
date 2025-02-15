-- 5 a DDL for hosts_cumulated_table

create table hosts_cumulated (
	"host" text,
	month_start date,
	host_activity_datelist date[],
	primary key (host, month_start)
);