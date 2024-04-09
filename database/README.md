# =============================
# THIS IS THE istSOSm DATABASE
# =============================

# This database should reflect the STA standard data model
# additionally other SOS specific metadata could be considered as 
#  and extension




phenomenonTime (timePeriod): period in time for which the mesaurment applies
resultTime ()

phenomenonTime:	The time instant or period of when the Observation happens
result:	The Estimated value of Sensor reading for the ObservedProperty
resultTime:	The Time when the observations result was generated
resultQuality:	Describes the quality of the result
validTime:	The time period during which the result may be used.
parameters:	Key-Value pairs describing the environmental conditions during measurement
Datastream:	The navigationLink to the Datastream of this Observation.
FeatureOfInterest:	The navigationLink to the FeatureOfInterest of this Observation.


ATTENZIONE:
phenomenonTime, salviamo nella tabella osservazioni solo l'istante, poi nella tabella datastream definiamo un timelag (positivo, zero o negativo) che identifica il preriodo (range) a cui si riferisce la misura.