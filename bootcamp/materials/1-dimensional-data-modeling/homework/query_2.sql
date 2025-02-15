-- cumulative query
insert into actors
with last_year as (
	select * from actors a 
	where current_year = 2020
),
	this_year as (
	select * from actor_films
	where year = 2021
), 
	this_year_films_and_ratings as (
	select 
		actorid, 
		actor, 
		year,
		array_agg(row(
			ty.film,
			ty.votes,
			ty.rating,
			ty.filmid)::films) as current_films,
		avg(rating) as average_rating
	from this_year as ty
	group by actorid, actor, year
)
select 
	coalesce(ly.actorid, ty.actorid) as actorid,
	coalesce(ly.actor, ty.actor) as actor,
	coalesce(ly.current_year+ 1, ty.year) as current_year,
	case 
		when ly.current_year is null
		then ty.current_films
		when ty.year is null
		then ly.films
		else ly.films || ty.current_films
	end::films[] as films,
	case 
		when ty.average_rating is null
		then ly.quality_class
		else 
			case 
				when ty.average_rating > 8 then 'star'
				when ty.average_rating > 7 then 'good'
				when ty.average_rating > 6 then 'average'
				else 'bad'				
			end::quality_class
	end::quality_class,
	case 
		when ty is null
		then false
		else true
	end as is_active	
from this_year_films_and_ratings as ty
full outer join last_year ly
	on ly.actorid = ty.actorid