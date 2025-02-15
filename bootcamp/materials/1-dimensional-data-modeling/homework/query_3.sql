-- 3 - DDL for actors_history_scd
create table actors_history_scd (
	actorid text,
	is_active boolean,
	quality_class quality_class,
	current_year integer,
	start_date integer,
	end_date integer
);