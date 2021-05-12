# Stop_fires_VIC
Pyspark application to detect fires in VIC cities.
## Summary
StopFire is a campaign started by Monash University to predict and stop fire in Victorian cities. They have employed sensors in different cities of Victoria and have collected a large amount of data. The data is so big that their techniques have failed to provide results on time to predict fire. Hence the data is being migrated to NoSQL database (MongoDB). The main aim of this project is to develop a complete application setup from streaming to storing and analyzing the data from the **Apache Kafka, Apache Spark Streaming and MongoDB**.
<br>
## Data
- The data is provided in .CSV format, which consists of hsitoric_climatic data, temperature_hotspots and climate_historic data.
- Climatic data is recorded on a daily basis whereas Fire data is recorded based on the occurance of a fire on a particular day.

## Implementation 
<img width="872" alt="implementation" src="https://user-images.githubusercontent.com/39896565/118009795-bbf5a580-b391-11eb-9e9d-0e68d2f95968.png">
The above fig gives a idea of implementation of this real time application.<br>


- **Producer 1:** Loads all the data from climate_streaming.csv and randomly fed the data to the stream every 5 seconds. Data transformation is performed to detect the data partition from the producer.
-  **Producer 2:** Loads all the data from hotspot_aqua_streaming.csv and randomly fed the data to the  stream every 10- 30 seconds. Aqua is a satellite of NASA that reports latitute, longitude, confidence and surface temperature of a location. The data is transfomed before fed to streaming, to detect the partition from producer2.
-  **Producer 3:** Loads all the data from hotspot_terra_straming.csv and randomly fed every 10-30 seconds. Terra is another satillite of NASA similar to AQUA providign similar metrics.

### Streaming application
Streaming application in Apache Spark Streaming which has a local streaming context with two execution threads and a batch interval of 10 seconds. The streaming application will recieve streaming data from all 3 producers. Different operations on RDD performed are as below.<br>


- The streams are joined based on the location(latitude and longitude) and create the data model.
- If two locations are close to each other or not is determined by implementing the **geo-hashing algorithm**, with a precision of 5 which determines the number of characters in Geohash.

## Data Visualisation
- For the incoming climate data a line graph is plotted for air temperature against arrival time. Then the max and min temperatures are marked.<br>
<img width="655" alt="realtime_visualization" src="https://user-images.githubusercontent.com/39896565/118012640-9fa73800-b394-11eb-8f9b-58971a6b591f.png">
- The fire locations are mapped on the map with air temperature, surface temperature, relative humidity and confidence.
<img width="571" alt="realtime_map" src="https://user-images.githubusercontent.com/39896565/118012922-eac14b00-b394-11eb-9227-5a64f1374b24.png">







