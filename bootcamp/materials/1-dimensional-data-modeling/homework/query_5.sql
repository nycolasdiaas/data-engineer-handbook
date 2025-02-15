-- 5º) - Incremental Backfill :)
create type actors_scd_type as (
	quality_class quality_class,
	is_active boolean,
	start_date integer,
	end_date integer
);

with last_year_scd as (
	select * from actors_history_scd 
	where current_year = 2020
	and end_date = 2020
),
	historical_scd as (
	select 
		actorid,
		quality_class,
		is_active,
		start_date,
		end_date
	from actors_history_scd 
	where current_year = 2020
	and end_date < 2020
), 
	this_year_data as (
	select * from actors
	where current_year = 2021
), 
	unchanged_records as (
	select 
		coalesce(ty.actorid, ly.actorid) as actorid,
		coalesce(ty.quality_class, ly.quality_class) as quality_class,
		coalesce(ty.is_active, ly.is_active) as is_active,
		ly.start_date,
		ty.current_year as end_date
	from this_year_data as ty
	join last_year_scd as ly
		on ly.actorid = ty.actorid
	where ty.quality_class = ly.quality_class
		and ty.is_active = ly.is_active
), 
	changed_records as (
	select 
		coalesce(ty.actorid, ly.actorid) as actorid,
		unnest(array[
			row(
				ly.quality_class,
				ly.is_active,
				ly.start_date,
				ly.end_date
			)::actors_scd_type,
			row(
				ty.quality_class,
				ty.is_active,
				ty.current_year,
				ty.current_year
			)::actors_scd_type
		]) as records
	from this_year_data ty
	left join last_year_scd ly
		on ly.actorid = ty.actorid
	where (ty.quality_class <> ly.quality_class
			or ly.is_active <> ty.is_active)
), unnested_changed_records as (
	select 
		actorid,
		(records::actors_scd_type).quality_class,
		(records::actors_scd_type).is_active,
		(records::actors_scd_type).start_date,
		(records::actors_scd_type).end_date
	from changed_records
), new_records as (
	select 
		ty.actorid,
		ty.quality_class,
		ty.is_active,
		ty.current_year as start_date,
		ty.current_year as end_date
	from this_year_data ty
	left join last_year_scd ly
		on ly.actorid = ty.actorid
	where ly.actorid is null
)
select * from historical_scd
union all
select * from unchanged_records
union all
select * from unnested_changed_records
union all
select * from new_records
order by end_date desc