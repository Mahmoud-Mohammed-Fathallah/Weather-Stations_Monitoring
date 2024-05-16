#!/bin/bash


#!/bin/bash

# Loop to create 10 pods with different station IDs
for ((i=1; i<=10; i++)); do
    # Set the station ID
    STATION_ID="$i"
    
    # Set the pod name
    POD_NAME="weather-station-producer-$STATION_ID"
    
    # Apply the configuration with the current station ID and pod name
    sed -e "s/STATION_ID_PLACEHOLDER/$STATION_ID/g" -e "s/POD_NAME_PLACEHOLDER/$POD_NAME/g" ten_weather_stations.yml |  kubectl  apply -f -
done
