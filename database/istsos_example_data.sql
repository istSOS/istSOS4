-- location
insert
	into
	sensorthings."Location" ("name",
	"description",
	"encodingType",
	"location")
values ('Room 101',
'The first room in the building',
'application/vnd.geo+json',
ST_SetSRID(ST_MakePoint(-73.987,
40.766),
4326));

insert
	into
	sensorthings."Location" ("name",
	"description",
	"encodingType",
	"location")
values ('Room 102',
'The second room in the building',
'application/vnd.geo+json',
ST_SetSRID(ST_MakePoint(-73.987,
40.766),
4326));

insert
	into
	sensorthings."Location" ("name",
	"description",
	"encodingType",
	"location")
values ('SIC 102',
'Lab at IIT Bombay',
'application/vnd.geo+json',
ST_SetSRID(ST_MakePoint(-19.131004980831737, 72.91701812621127),
4326));




-- thing
insert
	into
	sensorthings."Thing" ("name",
	"description",
	"properties",
	"location_id")
values ('Temperature Sensor',
'A sensor that measures the temperature in a room',
'{"manufacturer": "ACME Inc.", "model": "TS-100"}',
1);

insert
	into
	sensorthings."Thing" ("name",
	"description",
	"properties",
	"location_id")
values ('Humidity Sensor',
'A sensor that measures the humidity in a room',
'{"manufacturer": "ACME Inc.", "model": "TS-100"}',
2);

insert
	into
	sensorthings."Thing" ("name",
	"description",
	"properties",
	"location_id")
values ('Temperature Sensor',
'A sensor that measures the humidity in a room',
'{"manufacturer": "ACME Inc.", "model": "TS-100"}',
3);


-- historical location

insert
	into
	sensorthings."HistoricalLocation" ("time",
	"thing_id",
	"location_id")
values ('2023-03-25 10:00:00-04',
1,
1);


insert
	into
	sensorthings."HistoricalLocation" ("time",
	"thing_id",
	"location_id")
values ('2023-03-25 10:00:00-04',
2,
2);

insert
	into
	sensorthings."HistoricalLocation" ("time",
	"thing_id",
	"location_id")
values ('2023-03-25 10:00:00-04',
1,
3);


-- observed property


insert
	into
	sensorthings."ObservedProperty" ("name",
	"definition",
	"description")
values ('Temperature',
'http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#Temperature',
'The degree or intensity of heat present in a substance or object');

insert
	into
	sensorthings."ObservedProperty" ("name",
	"definition",
	"description")
values ('Humidity',
'http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#Humidity',
'The percentage of humidity present in a substance or object');



-- sensor

insert
	into
	sensorthings."Sensor" ("name",
	"encodingType",
	"metadata")
values ('Temperature Sensor',
'application/pdf',
'{"specification": "https://example.com/temperature-sensor-specs.pdf"}');

insert
	into
	sensorthings."Sensor" ("name",
	"encodingType",
	"metadata")
values ('HUmidity Sensor',
'application/pdf',
'{"specification": "https://example.com/humidity-sensor-specs.pdf"}');



-- datastream

insert
	into
	sensorthings."Datastream" ("name",
	"description",
	"unitOfMeasurement",
	"observationType",
	"observedArea",
	"phenomenonTime",
	"resultTime",
	"thing_id",
	"sensor_id",
	"observedproperty_id")
values ('Temperature Datastream',
'A datastream that provides the temperature measurements from a temperature sensor',
'{"name": "degree Celsius", "symbol": "degC", "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"}',
'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
ST_MakePolygon(ST_GeomFromText('LINESTRING(-73.987 40.766, -73.987 40.768, -73.983 40.768, -73.983 40.766, -73.987 40.766)')),
tstzrange('2023-03-25 10:00:00-04',
'2023-03-25 11:00:00-04'),
tstzrange('2023-03-25 10:00:00-04',
'2023-03-25 11:00:00-04'),
1,
1,
1);


insert
	into
	sensorthings."Datastream" ("name",
	"description",
	"unitOfMeasurement",
	"observationType",
	"observedArea",
	"phenomenonTime",
	"resultTime",
	"thing_id",
	"sensor_id",
	"observedproperty_id")
values ('Temperature Datastream',
'A datastream that provides the temperature measurements from a temperature sensor',
'{"name": "degree Celsius", "symbol": "degC", "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"}',
'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',
ST_MakePolygon(ST_GeomFromText('LINESTRING(-73.987 40.766, -73.987 40.768, -73.983 40.768, -73.983 40.766, -73.987 40.766)')),
tstzrange('2023-03-25 10:00:00-04',
'2023-03-25 11:00:00-04'),
tstzrange('2023-03-25 10:00:00-04',
'2023-03-25 11:00:00-04'),
2,
2,
2);

-- features of interest
insert
	into
	sensorthings."FeaturesOfInterest" ("name",
	"encodingType",
	"feature")
values ('Room 101',
'application/vnd.geo+json',
ST_SetSRID(ST_MakePoint(-73.987,
40.766),
4326));

insert
	into
	sensorthings."FeaturesOfInterest" ("name",
	"encodingType",
	"feature")
values ('SIC 102',
'application/vnd.geo+json',
ST_SetSRID(ST_MakePoint(-19.131004980831737, 72.91701812621127),
4326));

insert
	into
	sensorthings."FeaturesOfInterest" ("name",
	"encodingType",
	"feature")
values ('Room 102',
'application/vnd.geo+json',
ST_SetSRID(ST_MakePoint(-73.987,
40.766),
4326));


-- observation

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
values ('2023-03-25 10:30:00-04',
'2023-03-25 10:30:00-04',
23.5,
null,
null,
null,
1,
1);

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
values ('2023-03-25 10:30:00-04',
'2023-03-25 10:30:00-04',
23.5,
null,
null,
null,
2,
1);

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
values ('2023-03-25 10:30:00-04',
'2023-03-25 10:30:00-04',
23.5,
null,
null,
null,
2,
2);