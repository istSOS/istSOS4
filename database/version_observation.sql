select sensorthings.add_table_to_versioning('Observation','sensorthings')

insert
	into
	sensorthings."Observation" ("phenomenonTime",
	"resultTime",
	"result",
	"resultQuality",
	"validTime",
	"parameters",
	"datastream_id",
	"feature_of_interest_id")
values (now(),
now(),
99,
null,
null,
null,
1,
1);

update
	sensorthings."Observation"
set
	"phenomenonTime" = NOW(),
	"resultTime" = NOW(),
	"result" = 100,
	"resultQuality" = null,
	"validTime" = null,
	parameters = null,
	datastream_id = 1,
	feature_of_interest_id = 1
	--system_time_validity = '["2023-03-25 14:35:11.272722+01",infinity)'::tstzrange
where
	id = 14;

delete from sensorthings."Observation" where id = 14;
