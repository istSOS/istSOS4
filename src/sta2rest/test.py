"""
Module: STA2REST Test

Author: Filippo Finke

This module provides unit tests for the STA2REST module.
"""
import unittest
from sta2rest import STA2REST

class STA2RESTTestCase(unittest.TestCase):
    """
    Test case for STA2REST module.
    """
    def test_convert_entity(self):
        """
        Test the conversion of entities.
        """
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

    def test_convert_sensor_things_query(self):
        """
        Test the conversion of sensor things queries.
        """
        query_mappings = {
            "$filter=type eq 'temperature'&$orderby=timestamp desc&$top=10&$skip=5":
                "type=eq.temperature&order=timestamp.desc&offset=5&limit=10",
            "$filter=type eq 'humidity'&$top=5":
                "type=eq.humidity&limit=5",
            "$orderby=timestamp asc&$skip=2":
                "order=timestamp.asc&offset=2",
            "$select=id,name,description,properties&$top=1000&$filter=properties/type eq 'station'&$expand=Locations,Datastreams($select=id,name,unitOfMeasurement;$expand=ObservedProperty($select=name),Observations($select=result,phenomenonTime;$orderby=phenomenonTime desc;$top=1))":
            "Datastream.Observation.order=phenomenonTime.desc&Datastream.Observation.limit=1&select=id,name,description,properties,Location(*),Datastream(id,name,unitOfMeasurement),ObservedProperty(name),Observation(result,phenomenonTime)&properties->>type=eq.station&limit=1000",
            "$select=id,description&$expand=Datastreams($select=id,description)": "select=id,description,Datastream(id,description)",
            "$expand=Datastreams": "select=Datastream(*)",
            "$expand=Observations,ObservedProperty": "select=Observation(*),ObservedProperty(*)",
            "$expand=Observations($filter=result eq 1)": "Observation.result=eq.1&select=Observation(*)",
            "$expand=Observations($select=result)": "select=Observation(result)",
            "$select=result": "select=result",
            "$orderby=result": "order=result.asc",
            "$expand=Datastream&$orderby=Datastreams/id desc,phenomenonTime": "select=Datastream(*)&order=Datastreams/id.desc,phenomenonTime.asc",
            "$top=5": "limit=5",
            "$top=5&$orderby=phenomenonTime%20desc": "order=phenomenonTime.desc&limit=5",
            "$skip=5": "offset=5",
            "$count=true": "count=true",
            "$filter=result lt 10.00": "result=lt.10.00",
            "$filter=unitOfMeasurement/name eq 'degree Celsius'": "unitOfMeasurement->>name=eq.degree Celsius",
            "$filter=unitOfMeasurement/name ne 'degree Celsius'": "unitOfMeasurement->>name=neq.degree Celsius",
            "$filter=result gt 20.0": "result=gt.20.0",
            "$filter=result ge 20.0": "result=gte.20.0",
            "$filter=result lt 100": "result=lt.100",
            "$filter=result le 100": "result=lte.100",
            "$filter=result le 3.5 and FeatureOfInterest/id eq 1": "result=lte.3.5&FeatureOfInterest->>id=eq.1",
            "$filter=result gt 20 or result le 3.5": "or=(result.gt.20,result.lte.3.5)",
            "$filter=id eq 1 or id eq 2 or id eq 3": "or=(id.eq.1,id.eq.2,id.eq.3)",
            "$filter=id eq 1 or id eq 2 or id eq 3 or id eq 4": "or=(id.eq.1,id.eq.2,id.eq.3,id.eq.4)",
            "$filter=(id eq 1 or id eq 2 or id eq 3 or id eq 4) and location_id eq 2": "or=(id.eq.1,id.eq.2,id.eq.3,id.eq.4)&location_id=eq.2",
            "$filter=location_id eq 2 and id eq 2": "location_id=eq.2&id=eq.2",
        }

        for query, expected in query_mappings.items():
            print("QUERY", query)
            self.assertEqual(STA2REST.convert_query(query), expected)

if __name__ == '__main__':
    # Run all tests
    unittest.main()