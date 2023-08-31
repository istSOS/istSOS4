import csv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values





def id_sequence():
    connection_url = "postgres://admin:admin@database:5432/istsos"
    conn = psycopg2.connect(connection_url)


    update_sql = f""" 
    SELECT id FROM sensorthings."Datastream"
    ORDER BY "id" DESC
    LIMIT 1;    """

    cur = conn.cursor()

    # cur.execute('SELECT COUNT(*) FROM sensorthings."Observation";')
    # test=cur.fetchone()
    # print(type(test))



    cur.execute('SELECT id FROM sensorthings."Location" ORDER BY "id" DESC LIMIT 1;')
    location_id=cur.fetchone()
    if location_id!=None:
        location_id=location_id[0]
    else:
        location_id=0
    print(location_id)


    cur.execute('SELECT id FROM sensorthings."Thing" ORDER BY "id" DESC LIMIT 1;')
    thing_id=cur.fetchone()
    if thing_id!=None:
        thing_id=thing_id[0]
    else:
        thing_id=0
    print(thing_id)


    cur.execute('SELECT id FROM sensorthings."HistoricalLocation" ORDER BY "id" DESC LIMIT 1;')
    historicalLoc_id=cur.fetchone()
    if historicalLoc_id!=None:
        historicalLoc_id=historicalLoc_id[0]
    else:
        historicalLoc_id=0
    print(historicalLoc_id)


    cur.execute('SELECT id FROM sensorthings."ObservedProperty" ORDER BY "id" DESC LIMIT 1;')
    observedProperty_id=cur.fetchone()
    if observedProperty_id!=None:
        observedProperty_id=observedProperty_id[0]
    else:
        observedProperty_id=0
    print(observedProperty_id)


    cur.execute('SELECT id FROM sensorthings."Sensor" ORDER BY "id" DESC LIMIT 1;')
    sensor_id=cur.fetchone()
    if sensor_id!=None:
        sensor_id=sensor_id[0]
    else:
        sensor_id=0
    print(sensor_id)

    cur.execute('SELECT id FROM sensorthings."Datastream" ORDER BY "id" DESC LIMIT 1;')
    datastream_id=cur.fetchone()
    if datastream_id!=None:
        datastream_id=datastream_id[0]
    else:
        datastream_id=0
    print(datastream_id)



    cur.execute('SELECT id FROM sensorthings."FeaturesOfInterest" ORDER BY "id" DESC LIMIT 1;')
    featuresOfInterest_id=cur.fetchone()
    if featuresOfInterest_id!=None:
        featuresOfInterest_id=featuresOfInterest_id[0]
    else:
        featuresOfInterest_id=0
    print(featuresOfInterest_id)

    cur.execute('SELECT COUNT(*) FROM sensorthings."Observation";')
    observation_id=cur.fetchone()
    if observation_id!=None:
        observation_id=observation_id[0]
    else:
        observation_id=0
    print(observation_id)


    # Close the cursor and connection
    conn.commit()
    cur.close()

    conn.close()


  

    return location_id,thing_id,historicalLoc_id,observedProperty_id,sensor_id,datastream_id,featuresOfInterest_id,observation_id



