# Weather-Stations_Monitoring

This project integrates weather data from local weather stations and the Open-Meteo API, processes the data using Apache Kafka, and stores it in Elasticsearch for analysis. The project includes several components, including producers, consumers, and an uploader that watches for new data files and uploads them to Elasticsearch.

![](FinalProject.gif)


## Project Structure

- `KAFKA/`: Contains the Kafka producer code.
- `central_station/`: Contains the central station consumer code.
- `ELK/`: Contains the uploader code and Dockerfile for Elasticsearch, Logstash, and Kibana.
- `K8S/`: Contains Kubernetes deployment files for Kafka, producers, central station, and ELK stack.
- `Streaming/`: Contains the processing streaming code.
- `RemoteApi/`: Contains code for integrating with the remote Open-Meteo API.
- `Logs/`: Directory to store logs.
- `system.sh`: Shell script to build, deploy, and manage the system.

## Prerequisites

- Docker
- Kubernetes (Minikube or Kind)
- Apache Kafka
- Maven
- Java
- Elasticsearch
- Python

## Setup and Deployment

### Build and Deploy the System

1. Clone the repository:

   ```bash
   git clone [https://github.com/Mahmoud-Mohammed-Fathallah/Weather-Stations_Monitoring.git]
   cd Weather-Stations_Monitoring
   ```

2. Run the system setup script:

   ```bash
   ./system.sh up
   ```

   This script performs the following steps:
   - Builds the Docker images for the producers, central station, and uploader.
   - Deploys Kafka, storage, producers,remote api, central station, uploader, and the ELK stack to Kubernetes.

3. Optionally, you can skip building the JARs and Docker images:

   ```bash
   ./system.sh up njar
   ./system.sh up njar nbuild
   ```

### Shut Down the System

To bring down the system:

```bash
./system.sh down
```

## Scripts

### `system.sh`

This script manages the build and deployment of the system. It supports the following commands:

- `up`: Builds and deploys the system.
- `up njar`: Deploys the system without building the JAR files.
- `up njar nbuild`: Deploys the system without building the JAR files or Docker images.
- `down`: Shuts down and cleans up the deployed system.


## Kubernetes

 - `central_station.yml`: Deployment file for central station.
  - `Elk.yml`: Deployment file for Elasticsearch, Logstash, and Kibana stack.
  - `KafkaProcessor.yml`: Deployment file for Kafka stream processor.
  - `kafka.yml`: Deployment file for Kafka.
  - `meto_station.yml`: Deployment file for meto station.
  - `run10.sh`: Script to deploy ten producers.
  - `stop10.sh`: Script to stop ten producers.
  - `storage.yml`: Deployment file for storage.
  - `ten_weather_stations.yml`: Deployment file for ten weather stations.
  - `upload_parquets.yml`: Deployment file for uploader.
  - `weather_station.yml`: Deployment file for weather station.

## Integration Patterns

This project implements several Enterprise Integration Patterns:

1. **Polling Consumer**: The central station poll weather data from local stations and the Open-Meteo API.
2. **Dead-Letter Channel**: Handles failed message processing by putting in another Topic.
3. **Channel Adapter**: Integrates external weather data sources into the Kafka message system.
4. **Envelope Wrapper**: Encapsulates messages with metadata for routing and processing.
   
## Bitcask Usage in Weather Stations Monitoring Project

### Segment Files
- **Record Structure**: Key is an integer (since we only have 10 stations) and value is the incoming message as a string.
  - ![image](https://github.com/Mahmoud-Mohammed-Fathallah/Weather-Stations_Monitoring/assets/94381197/cae9e5d6-6179-4eef-a9d1-d1c8c60b3e0f)

- **File Threshold**: Set to 50 KB for testing purposes, enabling quick testing of new segment file creation and compaction.
- **Segment File Identifiers**: Sequential numbers starting from 0.
- **Accessing Records**: An in-memory structure (keyDir hashmap) stores the file identifier, byte offset, and record length.
  - ![image](https://github.com/Mahmoud-Mohammed-Fathallah/Weather-Stations_Monitoring/assets/94381197/b859a433-50e5-48dd-aa6a-e23fcec059f4)


### Hint Files
- **Purpose**: Aid in the rehashing of the keyDir during recovery from a crash, avoiding the need to read all segment files.
- **Creation**: Generated after each new segment file, containing the keyDir.
- **Recovery**: Reads the latest hint file and any newer segment files to rebuild the keyDir.
  -![image](https://github.com/Mahmoud-Mohammed-Fathallah/Weather-Stations_Monitoring/assets/94381197/b693e5f6-8ec1-45a8-9532-1a10f3f4ceac)


### Compaction
- **Process**: A separate thread runs every two minutes, takes a snapshot of the keyDir, and compacts old segment files.
- **Active File Handling**: Ignores keys in the active file during compaction.
- **Updating Storage**: Acquires a write lock on keyDir during the update to prevent interruptions.
- **Hint File**: Writes a new hint file after compaction.
  ![](diagrams/diagrams/bitcaskModel.gif)
- **Efficient Data Storage**: Uses Bitcask's log-structured storage for high-performance write operations.
- **Quick Data Retrieval**: In-memory index allows for rapid access to historical weather data.
- **Optimal Disk Usage**: Compaction process removes outdated entries, maintaining efficient storage.
- **Reliability**: Ensures quick and reliable recording of data from various weather stations.
- 
## Weather Station Mock

### Weather Station Producer
- Sends weather messages to the Kafka `station` topic.
- Implements message properties:
  - Dropped messages: 10%.
  - Battery status: 30% Low, 40% Medium, 30% High.
- Dead messages are sent to the `droppedmessage` channel for verification in Elasticsearch.
  - ![Class Diagram of Weather Station Server](WS.png)

### Kafka Streaming Processor
- Streams messages from the `station` topic in Kafka.
- Checks for humidity levels above 70%.
- Sends warning messages to a new Kafka topic called `rain`.

### Weather Stations
- 10 weather stations, each sending messages to the Kafka `station` topic and dropped messages to the `droppedmessage` channel.
- Central station consumes messages from weather stations, updates Bitcask storage with the latest status, archives messages in Parquet files, and handles additional processing.



## kibana results 
![](battery_status.gif)

![](dropped_message.gif)

## Observing File Changes

The uploader component uses the `watchdog` library to monitor the specified directory for changes and upload new or modified Parquet files to Elasticsearch.

## Contributing

Contributions are welcome! Please fork the repository and create a pull request with your changes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contact

For any questions or issues, please contact mAm.

---

By mAm

---

