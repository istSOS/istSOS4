from fastapi import FastAPI, HTTPException
from typing import List
from pydantic import BaseModel
import psycopg2

# Define a Pydantic model for the Datastream data
class Datastream(BaseModel):
    id: int
    name: str
    description: str
    unitOfMeasurement: dict
    observationType: str
    observedArea: str
    phenomenonTime: str
    resultTime: str
    thing_id: int
    sensor_id: int
    observedproperty_id: int

# Create a FastAPI instance
app = FastAPI()

# Endpoint to fetch Datastream data by ID from the database
@app.get("/datastreams/{datastream_id}", response_model=Datastream)
def get_datastream_by_id(datastream_id: int):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="your_database",
        user="your_username",
        password="your_password"
    )
    
    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()
    
    # Execute the SQL query to fetch the Datastream data by ID from the database
    cursor.execute("""
        SELECT 
            id, name, description, unitOfMeasurement, observationType, 
            ST_AsGeoJSON(observedArea), phenomenonTime, resultTime, 
            thing_id, sensor_id, observedproperty_id 
        FROM 
            sensorthings."Datastream"
        WHERE
            id = %s
    """, (datastream_id,))
    
    # Fetch the row returned by the query
    row = cursor.fetchone()
    
    # Close the cursor and database connection
    cursor.close()
    conn.close()
    
    # If no datastream is found for the provided ID, raise an HTTPException
    if row is None:
        raise HTTPException(status_code=404, detail="Datastream not found")
    
    # Create a Datastream object from the retrieved row
    datastream = Datastream(
        id=row[0],
        name=row[1],
        description=row[2],
        unitOfMeasurement=row[3],
        observationType=row[4],
        observedArea=row[5],
        phenomenonTime=row[6],
        resultTime=row[7],
        thing_id=row[8],
        sensor_id=row[9],
        observedproperty_id=row[10]
    )
    
    return datastream