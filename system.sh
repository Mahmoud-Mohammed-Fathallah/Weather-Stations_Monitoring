#!/bin/bash

# Define colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to build the producer jar
build_producer_jar() {
    echo -e "${GREEN}Building the jar of the producer${NC}"
    mvn clean install package
}

# Function to build the Streaming jar
build_Streaming_jar() {
    echo -e "${GREEN}Building the jar of the streaming${NC}"
    mvn clean install package
}

# Function to build the central station jar
build_central_station_jar() {
    echo -e "${GREEN}Building the jar of the central station${NC}"
    mvn clean install package
}

# Function to build the producer image
build_producer_image() {
    echo -e "${GREEN}Building the docker image of the producer${NC}"
    sudo docker build -t weather-station -f DockerfileProducer .
    echo -e "${GREEN}Loading the docker image of the producer${NC}"
    minikube image load weather-station:latest
}

# Function to build the streaming image
build_streaming_image() {
    echo -e "${GREEN}Building the docker image of the streaming${NC}"
    sudo docker build -t kafkastreaming -f DockerfileStreaming .
    echo -e "${GREEN}Loading the docker image of the streaming${NC}"
    minikube image load kafkastreaming:latest
}



# Function to build the central station image
build_central_station_image() {
    echo -e "${GREEN}Building the docker image of the central station${NC}"
    sudo docker build -t central-station -f DockerfileConsumer .
    echo -e "${GREEN}Loading the docker image of the central station${NC}"
    minikube image load central-station:latest
}

# Function to build the uploader image
build_uploader_image() {
    echo -e "${GREEN}Building the docker image of the uploader${NC}"
    sudo docker build -t upload-app .
    echo -e "${GREEN}Loading the docker image of the uploader${NC}"
    minikube image load upload-app:latest
}

# Function to bring up the system
up() {
    echo -e "${YELLOW}Bringing up the system...${NC}"
    if [[ "$1" != "njar" ]]; then
        # Build the producer jar
        cd KAFKA
        build_producer_jar
        # Build the central station jar
        cd ../central_station   
        build_central_station_jar

        # Build the Streaming jar
        cd ../Streaming  
        build_Streaming_jar

        cd ..
    fi

    if [[ "$2" != "nbuild" ]]; then
        # Build the producer image
        cd KAFKA
        build_producer_image
        # Build the central station image
        cd ../central_station
        build_central_station_image
        # Build the uploader image
        cd ../ELK
        build_uploader_image

        # Build the Streaming jar
        cd ../Streaming  
        build_streaming_image

        cd ..
    fi

    echo -e "${YELLOW}Deploying the system...${NC}"
    # Deploy the system
    cd K8S
    # Deploy Kafka
    echo -e "${GREEN}Deploying Kafka${NC}"
    minikube kubectl -- apply -f kafka.yml
    # Deploy Streaming
    echo -e "${GREEN}Deploying Streaming${NC}"
    minikube kubectl -- apply -f KafkaProcessor.yml
    # Deploy the storage
    echo -e "${GREEN}Deploying the storage${NC}"
    minikube kubectl -- apply -f storage.yml
    # Deploy ten producers
    echo -e "${GREEN}Deploying the producers${NC}"
    ./run10.sh
    # Deploy the central station
    echo -e "${GREEN}Deploying the central station${NC}"
    minikube kubectl -- apply -f central_station.yml
    # Deploy the uploader
    echo -e "${GREEN}Deploying the uploader${NC}"
    minikube kubectl -- apply -f upload_parquets.yml
    # Deploy the ELK stack
    echo -e "${GREEN}Deploying the ELK stack${NC}"
    minikube kubectl -- apply -f Elk.yml
}

# Function to bring down the system
down() {
    echo -e "${YELLOW}Bringing down the system...${NC}"
    cd K8S
    # Delete the ELK stack
    echo -e "${RED}Deleting the ELK stack${NC}"
    minikube kubectl -- delete -f Elk.yml
    # Delete the uploader
    echo -e "${RED}Deleting the uploader${NC}"
    minikube kubectl -- delete -f upload_parquets.yml
    # Delete the central station
    echo -e "${RED}Deleting the central station${NC}"
    minikube kubectl -- delete -f central_station.yml
    # Delete Kafka
    echo -e "${RED}Deleting Kafka${NC}"
    minikube kubectl -- delete -f kafka.yml
    # Delete Streaming
    echo -e "${RED}Deleting Streaming${NC}"
    minikube kubectl -- delete -f KafkaProcessor.yml
    # Delete the producers
    echo -e "${RED}Deleting the producers${NC}"
    ./stop10.sh
}

# Main script
case "$1" in
    up)
        if [[ "$2" == "njar" || "$2" == "nbuild" || -z "$2" ]]; then
            up "$2" "$3"
        else
            echo -e "${RED}Invalid argument: $2${NC}"
            echo -e "${YELLOW}Usage: ./system.sh up [njar] [nbuild]${NC}"
            echo -e "${YELLOW}Options:${NC}"
            echo -e "${YELLOW}  njar: Skip building jars${NC}"
            echo -e "${YELLOW}  nbuild: Skip building images${NC}"
            exit 1
        fi
        ;;
    down)
        down
        ;;
    *)
        echo -e "${RED}Usage: $0 {up|down}${NC}"
        exit 1
esac

exit 0
