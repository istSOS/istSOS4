select sensorthings.add_table_to_versioning('Observation','sensorthings')

insert
	into
	sensorthings."Observation" (
		"phenomenonTime",
		"resultTime",
		"result",
		"resultQuality",
		"validTime",
		"parameters",
		"datastream_id",
		"featuresofinterest_id"
	)
values (
	now(),
	now(),
	99,
	null,
	null,
	null,
	1,
	1
) returning current_timestamp;

update
	sensorthings."Observation"
set
	--"phenomenonTime" = NOW(),
	--"resultTime" = NOW(),
	"result" = 100,
	"resultQuality" = '{"quality": 100}',
	--"validTime" = null,
	--parameters = null,
	--datastream_id = 1,
	featuresofinterest_id = 1
	--system_time_validity = '["2023-03-25 14:35:11.272722+01",infinity)'::tstzrange
where
	id = 2;

update
	sensorthings."Observation"
set
	--"phenomenonTime" = NOW(),
	--"resultTime" = NOW(),
	"result" = 200,
	"resultQuality" = '{"quality": 200}',
	--"validTime" = null,
	--parameters = null,
	--datastream_id = 1,
	featuresofinterest_id = 1
	--system_time_validity = '["2023-03-25 14:35:11.272722+01",infinity)'::tstzrange
where
	id = 2;

update
	sensorthings."Observation"
set
	--"phenomenonTime" = NOW(),
	--"resultTime" = NOW(),
	"result" = 300,
	"resultQuality" = '{"quality": 300}',
	--"validTime" = null,
	--parameters = null,
	--datastream_id = 1,
	featuresofinterest_id = 1
	--system_time_validity = '["2023-03-25 14:35:11.272722+01",infinity)'::tstzrange
where
	id = 2;

--delete from sensorthings."Observation" where id = 2;