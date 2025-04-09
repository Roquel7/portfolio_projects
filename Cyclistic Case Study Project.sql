--merging all the tables into one table


CREATE TABLE annual_trips AS 
SELECT * 
FROM (
	SELECT * FROM january_2025
	UNION ALL
	SELECT * FROM february_2024
	UNION ALL
	SELECT * FROM march_2024
	UNION ALL
	SELECT * FROM april_2024
	UNION ALL
	SELECT * FROM may_2024
	UNION ALL
	SELECT * FROM june_2024
	UNION ALL
	SELECT * FROM july_2024
	UNION ALL
	SELECT * FROM august_2024
	UNION ALL
	SELECT * FROM september_2024
	UNION ALL
	SELECT * FROM october_2024
	UNION ALL
	SELECT * FROM november_2024
	UNION ALL
	SELECT * FROM december_2024
	);


/*
cleaning the data. There are 5854383 total trips, we are going to remove any rows with null values, and any duplicate rows with the same ride_id.
*/


SELECT * FROM annual_trips;


--converting started_at & ended_at into timestamp data type.


ALTER TABLE annual_trips
ALTER COLUMN started_at TYPE TIMESTAMP USING started_at::timestamp without time zone;

ALTER TABLE annual_trips
ALTER COLUMN ended_at TYPE TIMESTAMP USING ended_at::timestamp without time zone;


--viewing all the columns with null values.


SELECT 
  COUNT(*) - COUNT(ride_id) AS ride_id_count,
  COUNT(*) - COUNT(rideable_type) AS rideable_type_count,
  COUNT(*) - COUNT(started_at) AS started_at_count,
  COUNT(*) - COUNT(ended_at) AS ended_at_count,
  COUNT(*) - COUNT(start_lat) AS start_lat_count,
  COUNT(*) - COUNT(start_lng) AS start_lng_count,
  COUNT(*) - COUNT(end_lat) AS end_lat_count, --7005 null values
  COUNT(*) - COUNT(end_lng) AS end_lng_count, -- 7005 null values
  COUNT(*) - COUNT(start_station_name) AS start_station_name_count, -- 1077638 null values
  COUNT(*) - COUNT(start_station_id) AS start_station_id_count, -- 1077638 null values
  COUNT(*) - COUNT(end_station_name) AS end_station_name_count, -- 1107977 null values
  COUNT(*) - COUNT(end_station_id) AS end_station_id_count, -- 1107977 null values
  COUNT(*) - COUNT(member_casual) AS member_casual_count
FROM annual_trips;


--creating a backup table to remove nulls


CREATE TABLE trips_bkp AS 
SELECT * FROM annual_trips 
WHERE 
start_station_name IS NOT NULL AND
end_station_name IS NOT NULL AND
end_station_id IS NOT NULL AND
end_lat IS NOT NULL AND
end_lng IS NOT NULL AND
start_station_id IS NOT NULL;

TRUNCATE annual_trips;

INSERT INTO annual_trips
SELECT * FROM trips_bkp;

DROP TABLE trips_bkp;


--at this point we have all the nulls removed from the annual_trips table
SELECT * FROM annual_trips
--deleting duplicate ride_ids.


CREATE TABLE trips_bkp 
AS SELECT * FROM annual_trips;

ALTER TABLE trips_bkp ADD COLUMN row_number INT generated always as identity;

DELETE FROM trips_bkp
WHERE row_number IN (SELECT max(row_number) FROM trips_bkp
				GROUP BY ride_id
				HAVING count(*) > 1
			);

ALTER TABLE trips_bkp DROP COLUMN row_number;

TRUNCATE annual_trips;

INSERT INTO annual_trips
SELECT * FROM trips_bkp
order by started_at desc;

drop table trips_bkp;


--inserting the following columns: ride_length, start_time, end_time, date, year, month, day, day_of_week.


create table trips_bkp as
SELECT 
	*,
	(ended_at::time - started_at::time) as ride_length,
	started_at::time as start_time,
	ended_at::time as end_time,
	started_at::date as date,
	extract(year from started_at) as year,
	extract(month from started_at) as month,
	extract(day from started_at) as day,
	(case 
		when extract(isodow from started_at) = 1 then 'Mon'
		when extract(isodow from started_at) = 2 then 'Tues'
		when extract(isodow from started_at) = 3 then 'Wed'
		when extract(isodow from started_at) = 4 then 'Thurs'
		when extract(isodow from started_at) = 5 then 'Fri'
		when extract(isodow from started_at) = 6 then 'Sat'
		when extract(isodow from started_at) = 7 then 'Sun'
	end) as day_of_week
FROM annual_trips;

drop table annual_trips;

create table annual_trips as 
SELECT * FROM trips_bkp
order by started_at desc;

drop table trips_bkp;
