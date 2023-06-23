"""
Module: STA2REST Test

Author: Filippo Finke

This module provides unit tests for the STA2REST module.
"""
import unittest
from sta2rest import STA2REST

class STA2RESTTestCase(unittest.TestCase):
    def test_convert_entity(self):
        entity_mappings = {
            "Things": 'Thing',
            "Locations": 'Location',
            "Sensors": 'Sensor',
            "ObservedProperties": 'ObservedProperty',
            "Datastreams": 'Datastream',
            "Observations": 'Observation',
            "FeaturesOfInterest": 'FeatureOfInterest',
            "HistoricalLocations": 'HistoricalLocation'
            # Add more entity mappings as needed
        }

        for entity, expected in entity_mappings.items():
            self.assertEqual(STA2REST.convert_entity(entity), expected)

    def test_convert_property(self):
        property_mappings = {
            "name": 'name',
            "description": 'description',
            "encodingType": 'encodingType',
            "metadata": 'metadata'
            # Add more property mappings as needed
        }

        for prop, expected in property_mappings.items():
            self.assertEqual(STA2REST.convert_property(prop), expected)

    def test_convert_query_param(self):
        query_param_mappings = {
            "$orderby": "order",
            "$top": "limit",
            "$skip": "offset"
            # Add more query parameter mappings as needed
        }

        for param, expected in query_param_mappings.items():
            self.assertEqual(STA2REST.convert_query_param(param), expected)

    def test_convert_sensor_things_query(self):
        query_mappings = {
            "$filter=type eq 'temperature'&$orderby=timestamp desc&$top=10&$skip=5":
                "order=timestamp.desc&limit=10&offset=5&type=eq.temperature",
            "$filter=type eq 'humidity'&$top=5":
                "limit=5&type=eq.humidity",
            "$orderby=timestamp asc&$skip=2":
                "order=timestamp.asc&offset=2",
            "$select=id,name,description,properties&$top=1000&$filter=properties/type eq 'station'&$expand=Locations,Datastreams($select=id,name,unitOfMeasurement;$expand=ObservedProperty($select=name),Observations($select=result,phenomenonTime;$orderby=phenomenonTime desc;$top=1))":
            "select=id,name,description,properties,Location(*),ObservedProperty(name),Observation(result,phenomenonTime),Datastream(id,name,unitOfMeasurement)&limit=1000&Datastream.Observation.order=phenomenonTime.desc&Datastream.Observation.limit=1&properties->>type=eq.station",
            "$select=@iot.id,description&$expand=Datastreams($select=@iot.id,description)": "select=@iot.id,description,Datastream(@iot.id,description)",
            "$expand=Datastreams": "select=Datastream(*)",
            "$expand=Observations,ObservedProperty": "select=Observation(*),ObservedProperty(*)",
            "$expand=Observations($filter=result eq 1)": "select=Observation(*)&Observation.result=eq.1",
            "$expand=Observations($select=result)": "select=Observation(result)",
            "$select=result,resultTime": "select=result,resultTime",
            "$orderby=result": "order=result",
            "$expand=Datastream&$orderby=Datastreams/id desc,phenomenonTime": "order=Datastreams.id.desc,phenomenonTime&select=Datastream(*)",
            "$top=5": "limit=5",
            "$top=5&$orderby=phenomenonTime%20desc": "limit=5&order=phenomenonTime.desc",
            "$skip=5": "offset=5",
            "$count=true": "count=true",
            "$filter=result lt 10.00": "result=lt.10.00",
        }

        for query, expected in query_mappings.items():
            self.assertEqual(STA2REST.convert_query(query), expected)

if __name__ == '__main__':
    unittest.main()