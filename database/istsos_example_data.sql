-- location
INSERT INTO sensorthings."Location" ("name", "description", "encodingType", "location", "properties")
VALUES ('Room 101', 'The first room in the building', 'application/vnd.geo+json', ST_SetSRID(ST_MakePoint(-73.987, 40.766), 4326), '{}');

INSERT INTO sensorthings."Location" ("name", "description", "encodingType", "location", "properties")
VALUES ('Room 102', 'The second room in the building', 'application/vnd.geo+json', ST_SetSRID(ST_MakePoint(-73.987, 40.766), 4326), '{}');

INSERT INTO sensorthings."Location" ("name", "description", "encodingType", "location", "properties")
VALUES ('SIC 102', 'Lab at IIT Bombay', 'application/vnd.geo+json', ST_SetSRID(ST_MakePoint(-19.131004980831737, 72.91701812621127), 4326), '{}');

-- thing
INSERT INTO sensorthings."Thing" ("name", "description", "properties", "location_id")
VALUES ('Temperature Sensor', 'A sensor that measures the temperature in a room', '{"manufacturer": "ACME Inc.", "model": "TS-100"}', 1);

INSERT INTO sensorthings."Thing" ("name", "description", "properties", "location_id")
VALUES ('Humidity Sensor', 'A sensor that measures the humidity in a room', '{"manufacturer": "ACME Inc.", "model": "TS-100"}', 2);

INSERT INTO sensorthings."Thing" ("name", "description", "properties", "location_id")
VALUES ('Pressure Sensor', 'A sensor that measures the humidity in a room', '{"manufacturer": "ACME Inc.", "model": "TS-100"}', 3);


-- historical location

INSERT INTO sensorthings."HistoricalLocation" ("time", "thing_id", "location_id")
VALUES ('2023-03-25 10:00:00-04', 1, 1);

INSERT INTO sensorthings."HistoricalLocation" ("time", "thing_id", "location_id")
VALUES ('2023-03-25 10:00:00-04', 2, 2);

INSERT INTO sensorthings."HistoricalLocation" ("time", "thing_id", "location_id")
VALUES ('2023-03-25 10:00:00-04', 3, 3);



-- observed property

INSERT INTO sensorthings."ObservedProperty" ("name", "definition", "description", "properties")
VALUES ('Temperature', 'http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#Temperature', 'The degree or intensity of heat present in a substance or object', '{}');

INSERT INTO sensorthings."ObservedProperty" ("name", "definition", "description", "properties")
VALUES ('Humidity', 'http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#Humidity', 'The percentage of humidity present in a substance or object', '{}');

INSERT INTO sensorthings."ObservedProperty" ("name", "definition", "description", "properties")
VALUES ('Pressure', 'http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#Pressure', 'The pressure of a substance or object', '{}');


-- sensor

INSERT INTO sensorthings."Sensor" ("name", "description", "encodingType", "metadata", "properties")
VALUES ('Temperature Sensor', 'A temperature sensor', 'application/pdf', '{"specification": "https://example.com/temperature-sensor-specs.pdf"}', '{}');

INSERT INTO sensorthings."Sensor" ("name", "description", "encodingType", "metadata", "properties")
VALUES ('Humidity Sensor', 'A humidity sensor', 'application/pdf', '{"specification": "https://example.com/humidity-sensor-specs.pdf"}', '{}');

INSERT INTO sensorthings."Sensor" ("name", "description", "encodingType", "metadata", "properties")
VALUES ('Pressure Sensor', 'A pressure sensor', 'application/pdf', '{"specification": "https://example.com/pressure-sensor-specs.pdf"}', '{}');


-- datastream

INSERT INTO sensorthings."Datastream" ("name", "description", "unitOfMeasurement", "observationType", "observedArea", "phenomenonTime", "resultTime", "properties", "thing_id", "sensor_id", "observedproperty_id")
VALUES ('Temperature Datastream', 'A datastream for temperature measurements', '{"name": "degree Celsius", "symbol": "degC", "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"}', 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement', ST_MakePolygon(ST_GeomFromText('LINESTRING(-73.987 40.766, -73.987 40.768, -73.983 40.768, -73.983 40.766, -73.987 40.766)')), tstzrange('2023-03-25 10:00:00-04', '2023-03-25 11:00:00-04'), tstzrange('2023-03-25 10:00:00-04', '2023-03-25 11:00:00-04'), '{}', 1, 1, 1);

INSERT INTO sensorthings."Datastream" ("name", "description", "unitOfMeasurement", "observationType", "observedArea", "phenomenonTime", "resultTime", "properties", "thing_id", "sensor_id", "observedproperty_id")
VALUES ('Humidity Datastream', 'A datastream for humidity measurements', '{"name": "percent", "symbol": "%", "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#Percent"}', 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement', ST_MakePolygon(ST_GeomFromText('LINESTRING(-73.987 40.766, -73.987 40.768, -73.983 40.768, -73.983 40.766, -73.987 40.766)')), tstzrange('2023-03-25 10:00:00-04', '2023-03-25 11:00:00-04'), tstzrange('2023-03-25 10:00:00-04', '2023-03-25 11:00:00-04'), '{}', 2, 2, 2);

INSERT INTO sensorthings."Datastream" ("name", "description", "unitOfMeasurement", "observationType", "observedArea", "phenomenonTime", "resultTime", "properties", "thing_id", "sensor_id", "observedproperty_id")
VALUES ('Pressure Datastream', 'A datastream for pressure measurements', '{"name": "pascal", "symbol": "Pa", "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#Pascal"}', 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement', ST_MakePolygon(ST_GeomFromText('LINESTRING(-73.987 40.766, -73.987 40.768, -73.983 40.768, -73.983 40.766, -73.987 40.766)')), tstzrange('2023-03-25 10:00:00-04', '2023-03-25 11:00:00-04'), tstzrange('2023-03-25 10:00:00-04', '2023-03-25 11:00:00-04'), '{}', 3, 3, 3);

-- features of interest

INSERT INTO sensorthings."FeaturesOfInterest" ("name", "description", "encodingType", "feature", "properties")
VALUES ('Room 101', 'Feature of interest for Room 101', 'application/vnd.geo+json', ST_SetSRID(ST_MakePoint(-73.987, 40.766), 4326), '{}');

INSERT INTO sensorthings."FeaturesOfInterest" ("name", "description", "encodingType", "feature", "properties")
VALUES ('SIC 102', 'Feature of interest for SIC 102', 'application/vnd.geo+json', ST_SetSRID(ST_MakePoint(-19.131004980831737, 72.91701812621127), 4326), '{}');

INSERT INTO sensorthings."FeaturesOfInterest" ("name", "description", "encodingType", "feature", "properties")
VALUES ('Room 102', 'Feature of interest for Room 102', 'application/vnd.geo+json', ST_SetSRID(ST_MakePoint(-73.987, 40.766), 4326), '{}');



-- observation
INSERT INTO sensorthings."Observation" ("phenomenonTime", "resultTime", "resultType", "resultDouble", "resultQuality", "validTime", "parameters", "datastream_id", "feature_of_interest_id")
VALUES ('2023-03-25 10:30:00-04', '2023-03-25 10:30:00-04', 2, 23.5, NULL, NULL, NULL, 1, 1);

INSERT INTO sensorthings."Observation" ("phenomenonTime", "resultTime", "resultType", "resultDouble", "resultQuality", "validTime", "parameters", "datastream_id", "feature_of_interest_id")
VALUES ('2023-03-25 10:30:00-04', '2023-03-25 10:30:00-04', 2, 23.5, NULL, NULL, NULL, 2, 2);

INSERT INTO sensorthings."Observation" ("phenomenonTime", "resultTime", "resultType", "resultDouble", "resultQuality", "validTime", "parameters", "datastream_id", "feature_of_interest_id")
VALUES ('2023-03-25 10:30:00-04', '2023-03-25 10:30:00-04', 2, 23.5, NULL, NULL, NULL, 3, 3);

INSERT INTO sensorthings."Observation" ("phenomenonTime", "resultTime", "resultType", "resultString", "resultQuality", "validTime", "parameters", "datastream_id", "feature_of_interest_id")
VALUES ('2023-03-26 10:30:00-04', '2023-03-26 10:30:00-04', 0, "Test", NULL, NULL, NULL, 3, 3);

