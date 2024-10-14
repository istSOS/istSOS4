# From istSOS to istSOS4

istSOS is a software implemented to manage monitoring data in a standard way.

It's first implementation started in the 2010 when the need of integrating data from 5 different hydro-meteorological monitoring networks, distributed in two countries, raised in the frame of the Locarno region (Ticino, Switzerland) flood protection.

At that time, the new *Sensor Observation Service* (SOS) was just approved, and the 52North implementation in Java was the only open source available. Unfortunately, due to the interaction with hydrologists and civil protection, we had clear from the begin that we would have need a solution we could easily customize to satisfy stakeholders needs.

For this reason we decided to implement **istSOS** a new SOS compliant solution in Python. The next version, **istSOS2**,  has been enriched with several special extra features: the most significant of which is the introduction of a RESTful API based on Json data format. Feature that has been later adopted also from other SOS compliant solutions. XML was, in fact, too verbose to be consumed by Web services and Python libraries, which are at the core of Data Science.

**istSOS3** was a tentative of redesign istSOS taking advantage of micro-service approach by adopting RPC and specialized services to respond to different requests. While being used in a few projects, that version has been abandoned since its final results was not considered satisfactory.  

In the mean time, a new standard named *Sensor Things API* (STA), a service based on the OData standard by OASIS, emerged and while being not widely adopted for a long time, recently gets a hype thanks to the adoption of few relevant organizations (USGS, CNR) and the setup of a new family of standards from the OGC which are based on RESTful/Json approach and are named OGC APIs.

The STA standard follows for most of its data model the SOS specifications, particularly in the adoption of the standard *Observation and Modeling* (O&M) data encoding schema. However, it differs by the introduction of the *Thing*  and the *Datastream* objects. The *datastream* identifies a *Thing* observed by a *Sensor* that measure a *Property* by collecting *Observations*.

Since, we still have the requirement of implementing special features that meet stakeholders needs, in the new release of **istSOS4** we have completely redesigned the application to support the STA standard. While maintaining the istSOS core technologies, which are Python and PostgreSQL/PostGIS, we have introduced other solid technologies like fastAPI, asyncpg, sqlalchemy.