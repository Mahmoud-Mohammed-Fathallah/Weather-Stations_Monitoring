import os
import time
import logging
from fastavro import reader
from elasticsearch import Elasticsearch
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Set up logging
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

es_url = os.getenv('ELASTICSEARCH_URL')

# Connect to Elasticsearch
logging.info('Connecting to Elasticsearch')


es = None
while es is None:
    try:
        es = Elasticsearch([es_url])
        if not es.ping():
            es = None
            raise ConnectionError("Could not connect to Elasticsearch. Retrying...")
    except ConnectionError as e:
        logging.warning(" Retrying in 10 seconds...")
        time.sleep(10)

# Define the Avro schema
logging.info('Defining the Avro schema')
schema = {
  "type": "record",
  "name": "WeatherData",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "s_no", "type": "long"},
    {"name": "batteryStatus", "type": "string"},
    {"name": "statusTimestamp", "type": "long"},
    {
      "name": "weather",
      "type": {
        "type": "record",
        "name": "weather",
        "fields": [
          {"name": "humidity", "type": "int"},
          {"name": "temperature", "type": "int"},
          {"name": "windSpeed", "type": "int"}
        ]
      }
    }
  ]
}

class FileChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith('.parquet'):
            logging.info(f'Modified file detected: {event.src_path}')
            
            # Read the Avro file
            logging.info('Reading the Avro file')
            with open(event.src_path, 'rb') as f:
                avro_reader = reader(f, reader_schema=schema)
                records = [record for record in avro_reader]
            logging.info(f'Read {len(records)} records from the Avro file')
            # Upload the records to Elasticsearch
            logging.info('Uploading the records to Elasticsearch')
            for i, record in enumerate(records):
                es.index(index=os.getenv('ELASTICSEARCH_INDEX'), id=i, body=record)

# Create an observer
logging.info('Creating an observer')
observer = Observer()

# Set the observer to use your custom handler and watch the directory
logging.info('Setting the observer to watch the directory')
observer.schedule(FileChangeHandler(), path=os.getenv('DATA_PATH'), recursive=True)

# Start the observer
logging.info('Starting the observer')
observer.start()

# Keep the script running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
