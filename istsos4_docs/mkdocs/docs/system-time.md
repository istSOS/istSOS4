# OGC STA API *time-travel* Extension

- **Title:** Tempora Data Support
- **Identifier:** <https://stac-extensions.github.io/authentication/v1.1.0/schema.json>
- **Field Name Prefix:** *time-travel*
- **Scope:**
- **Extension [Maturity Classification](https://github.com/radiantearth/stac-spec/tree/master/extensions/README.md#extension-maturity):** Proposal
- **Owner**: @massimiliano.cannata

This document explains the "Temporal Data Support" Extension to the [SensorThings API](https://www.ogc.org/standard/sensorthings/) (STA) specification.

- [OGC STA API *time-travel* Extension](#ogc-sta-api-time-travel-extension)
  - [Overview](#overview)
  - [Definition](#definition)
  - [Requirements](#requirements)
  - [Examples](#examples)
  - [Using Extensions](#using-extensions)
  - [System-time Extension](#system-time-extension)

## Overview

This extension helps users istSTA with time travel data. It gives a way to retrive data from a Web service as they were offered 
in a specific time instant by means of a new query paramter ***as_of***.  
Additionally, it introduce the new Entity ***Commit*** which enable data lineage permitting to trace data changes.  
From e scientific point of view this extension enable FAIR data management allowing to permanently cite a dataset 
by using the combination of the service address (<r>in red</r>), the request that return the dataset (<g>in green</g>) and the time instant the dataset status (<o>in orange</o>) as Persistent Identifier to refer to.

Example:  
  <r>https://localhost/STA/v1.1/Things</r><g>?$Extend=ObservedProperties</g><o>&as_of=2017-02-09T18:02:00.000Z</o>

## Definition

The Time Travel extension add optional following query parameters to any STA request:

| Parameter | Type               | Description                                                      |
| --------- | ------------------ | ---------------------------------------------------------------- |
| *as_of*   | ISO 8601 date-time | a date-time to specify the instant for which data are requested  |
| *from_to* | ISO 8601 period    | a period that specify the interval for which data were requested |
| *commit*  | Boolean            | True if commit messages are requested to be returned             |

The Time Travel extension add optional response Header parameters:

| Parameter | Type               | Multiplicity and use | Description                                                        |
| --------- | ------------------ | -------------------- | ------------------------------------------------------------------ |
| *as_of*   | ISO 8601 date-time | One (mandatory)      | a date-time that specify the instant for which data were requested |


The Time Travel extension add the new Entity Commit with the following properties:

| Properties     | Type               | Multiplicity and use | Description                                                              |
| -------------- | ------------------ | -------------------- | ------------------------------------------------------------------------ |
| *author*       | string(128)        | One (mandatory)      | authority and username or link to user profile                           |
| *encodingType* | string             | One (optional)       | the encoding type of the message (default is text)                       |
| *message*      | string(256)        | One (mandatory)      | commit message detailing scope, motivation and method of the transaction |
| *date*         | ISO 8601 date-time | One (mandatory)      | a date-time that specify the instant for which the commit was executed   |



| *validityRange* | array[2]    | One (optional)       | the period for which the commit datetime has been valid                  |

Commits are related to any SensorThings API entities with a relation 1 to 0:1

## Requirements

!!! Note "Req 1: request-data/as_of-parameter"  
    An OGC SensorThings API service with extension *time-travel* SHALL evaluate the *$as_of*
    query options to return data as where served in the specified date-time.

!!! Note "Req 2: request-data/commit-parameter"  
    An OGC SensorThings API service with extension *time-travel* SHALL evaluate the *$commit*
    query options to return *commit* element in the response is set to *true*.

!!! Note "Req. 3: response-data/as_of-response-header" 
    An OCG SensorThings API service with extension *time-travel* SHALL include in the response
    header the parameter *as_of* with specific ISO 8601 date-time that identify the request time instant

!!! Note "Req 4: request-data/as_of-no-future-dates"  
    An OGC SensorThings API service with extension *time-travel* SHALL verify that, if present,
    the *$as_of* query options is not relative to a future instant

!!! Note "Req 5: request-data/as_of-default-is-now"  
    An OGC SensorThings API service with extension *time-travel* SHALL verify that if the *\$as_of*
    query options is not present return the currently valid data, which is equivalent of setting 
    *\$as_of* value to now() (returned as_of header parameter should refer to date time now)

Requirements for transactions

!!! Note "Req 6: request-data/commit-is-true-or-false"
    An OGC SensorThings API service with extension *time-travel* SHALL verify that if the *$commit*
    query options is present may have only true or false value.

!!! Note "Req 7: request-data/commit-default-is-false"
    An OGC SensorThings API service with extension *time-travel* SHALL verify that if the *$commit*
    query options is not present its default value is false, and should not return any commit elements.

!!! Note "Req 8: request-data/commit-element-validity"
    An OGC SensorThings API service with extension *time-travel* SHALL verify that if the *$commit*
    query options is set to true it should include the Commit element in the response with the selfLink

<!-- !!! Note "Req 9: request-data/commit-and-as_of-element-validity"
    An OGC SensorThings API service with extension *time-travel* SHALL verify that if the *\$commit* 
    query options is set to true and the *$as_of* parameter is not null, it should include the full 
    Commit element in the response: included of all its properties non null: 
    *author*, *message*, *date*, *encodingType* -->

<!-- COMMIT ELEMENT -->

!!! Note "Req 9: create/commit-on-commits"  
    An OGC SensorThings API service with extension *time-travel* SHALL not accepted the create request with *Commit* elements.

!!! Note "Req: 9: update-delete/commit"
    No update or delete of Commit elements

!!! Note "Req 10: update-delete/commit-element"  
    An OGC SensorThings API service with extension *time-travel* SHALL evaluate the *Commit* element
    included in the edited element and expanding its properties.

!!! Note "Req 11: update-delete/commit-element-properties"  
    An OGC SensorThings API service with extension *time-travel* SHALL evaluate that the *Commit* element
    included in the edited element has the *author* and *message* properties and that the *date* 
    is not included. (*date* is ReadOnly and fully system managed)

!!! Note "Req 12: update-delete/commit-element-versioning"  
    An OGC SensorThings API service with extension *time-travel* SHALL persistently keep versioning of the edited elements 
    end register commit elements that caused any variation in time.

<!-- VESRIONING REQUEST as_of is interval-->
Requirements for versioning

!!! Note "Req 14: request-data/element-history-validity"  
    An OGC SensorThings API service with extension *time-travel* SHALL evaluate the **$from_to* query option and verify that it can be used on a single entity (no expand) or return an error.

!!! Note "Req 15: request-data/element-history-elements"  
    An OGC SensorThings API service with extension *time-travel* SHALL evaluate the **$from_to* query option and return all the registered variations whose validity intersects the period and meet any additional filters.


## Examples

??? note "Get Things in a given instant"

    http://localhost/istsosta/v1.1/Things(1)?\$as_of=2017-02-07T18:12:00.000Z&$commit=True

    ``` {.json annontate} 
    {
      "value" : [
        {
          "name" : "My camping lantern",
          "description" : "camping lantern",
          "properties" : {
          "property1" : "it’s waterproof",
          "property2" : "it glows in the dark"
        },
          "Locations@iot.navigationLink" : "Things(1)/Locations?$as_of=2017-02-07T18:12:00.000Z",
          "HistoricalLocations@iot.navigationLink": "Things(1)/HistoricalLocations?$as_of=2017-02-07T18:12:00.000Z",
          "Datastreams@iot.navigationLink" : "Things(1)/Datastreams?$as_of=2017-02-07T18:12:00.000Z",
          "Commits@iot.navigationLink" : "Things(1)/Commit?$as_of=2017-02-07T18:12:00.000Z",
          "@iot.id" : 1,
          "@iot.selfLink" : "/istSTA/v1.0/Things(1)"
        }
    }
    ```

??? Note "Get the commit associated"

    http://localhost/istsosta/v1.1/Things(1)/Commit?$as_of=2017-02-07T18:12:00.000Z

    ``` {.json annontate} 
    {
      "@iot.id": 5,
      "author": "maxi",
      "encodingType": "text",
      "message": "update due to missing value",
      "date": "2017-02-07T13:12:00.000Z"
    }
    ```


http://localhost/istsosta/v1.1/Things(1)?\$as_of=2017-02-07T18:12:00.000Z\$commit=true&$expand=Commits
{
  "value" : [
    {
      "name" : "My camping lantern",
      "description" : "camping lantern",
      "properties" : {
      "property1" : "it’s waterproof",
      "property2" : "it glows in the dark"
    },
      "Locations@iot.navigationLink" : "Things(1)/Locations",
      "HistoricalLocations@iot.navigationLink": "Things(1)/HistoricalLocations",
      "Datastreams@iot.navigationLink" : "Things(1)/Datastreams",
      "Commit@iot.navigationLink" : "Things(1)/Commit",
      "Commit": {
        "author": "maxi",
        "message": "update due to missing value",
        "validityRange": ["2017-02-07T18:02:00.000Z","2017-02-09T18:02:00.000Z"]
      },
      "@iot.id" : 1,
      "@iot.selfLink" : "/FROST-Server/v1.0/Things(1)"
    }
}
```




### create a Thing




Requirements Class Party

``` {.json annontate}



observations?$expand=commits

{
  "@iotid": 27,
  "name": "Temperature Monitoring System",
  "description": "Sensor system monitoring area temperature",
  "properties": {
    "Deployment Condition": "Deployed in a third floor balcony",
    "Case Used": "Radiation shield"
  },
  




  "Commit@iot.navigationLink": "http://localhost/istsos/v1.1/things(27)/commit?$as_of=2017-02-09T18:02:00.000Z"

  "commit": {
    "author": "maxi",
    "message": "update due to missing value",
    (GET)"validityRange": ["2017-02-07T18:02:00.000Z","2017-02-09T18:02:00.000Z"]
  }
    
    "Thing": {
      "name": "Temperature Monitoring System",
      "description": "Sensor system monitoring area temperature",
      "properties": {
        "Deployment Condition": "Deployed in a third floor balcony",
        "Case Used": "Radiation shield"
      },
      "datastream": {
        "name": "Air Temperature DS",
        "description": "Datastream for recording temperature",
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unitOfMeasurement": {
          "name": "Degree Celsius",
          "symbol": "degC",
          "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"
        },
      }
    }
  },



 "commits": [
    {
      "author": "maxi",
      "message": "update due to missing value",
      "validityRange": ["2017-02-07T18:02:00.000Z","2017-02-09T18:02:00.000Z"]
    },  {
      "author": "maxi",
      "message": "update due to missing value",
      "validityRange": ["2017-02-07T18:02:00.000Z","2017-02-09T18:02:00.000Z"]
    },  {
      "author": "maxi",
      "message": "update due to missing value",
      "validityRange": ["2017-02-07T18:02:00.000Z","2017-02-09T18:02:00.000Z"]
    }]

}
```

1) ad ogni inserimento vogliamo un commit con auth, msg, time=now()
2) quando chiedo un elemento voglio sapere il commit associato auth, msg
3) quando 






<!-- !! Note "Req. 5: request-data/as_of   -->

- INSERT
  - author: ORCID / GOOGLE / google:massimiliano.cannata / supsi:massimiliano.cannata (OAUTH identifier)
  - msg:
- UPDATE
  - author:
  - msg:

only past time
-default is now

The Time Travel extension add the following parameters:

as_of

Any response provided by the STA add an Header parameter

## Using Extensions

SEE:
https://github.com/stac-extensions/xarray-assets
https://github.com/radiantearth/stac-spec/blob/master/extensions/README.md
https://stac-extensions.github.io/

This extension helps users of STA with data versioning. It gives a place for

OGC services do not support versioning of data and do not permit to:

1. Access the dataset as it was at a given instant in time
2. Evaluate the changes a dataset undergo in a give time period
3. Access all the record that has been changed after a given instant in time

This prevent scientific "reproducibility" of research and scientific analysis of data management.

For this reason we are proposing an extension that fill the above mentioned gaps.

## System-time Extension

The status of a layer or dataset should be accessible as was archived and offered in a given instant in time specified by the parameter **_asof_**
