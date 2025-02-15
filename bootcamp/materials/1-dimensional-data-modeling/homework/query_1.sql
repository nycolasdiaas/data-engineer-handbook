create type films as (
	film text,
	votes integer,
	rating real,
	filmid text
);

create type quality_class as 
	enum ('star', 'good','average', 'bad');

create table actors (
	actorid text,
	actor text,
	current_year integer,
	films films[],
	quality_class quality_class,
	is_active boolean,
	primary key (actorid, current_year)
);