import os
import time
import logging
import pandas as pd
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
        logging.warning("Retrying in 10 seconds...")
        time.sleep(10)

# Define the Elasticsearch index
elasticsearch_index = os.getenv('ELASTICSEARCH_INDEX')

class FileChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith('.parquet'):
            logging.info(f'Modified file detected: {event.src_path}')
            
            # Check file size and skip if empty
            if os.path.getsize(event.src_path) == 0:
                logging.warning(f'Skipping empty file: {event.src_path}')
                return

            # Read the Parquet file
            logging.info('Reading the Parquet file')
            try:
                df = pd.read_parquet(event.src_path)
                logging.info(f'Read {len(df)} records from the Parquet file')

                # Upload the records to Elasticsearch
                logging.info('Uploading records to Elasticsearch')
                for index, row in df.iterrows():
                    record = row.to_dict()
                    stationid = record.get('id') 
                    s_no = record.get('s_no') 

                    if stationid is not None and s_no is not None:
                        # Use stationid and s_no as the Elasticsearch document ID
                        document_id = f"{stationid}_{s_no}"
                        es.index(index=elasticsearch_index, id=document_id, body=record)
                    else:
                        logging.warning(f"Skipping record with missing 'stationid' or 's_no'")

            except Exception as e:
                logging.error(f'Error processing file: {event.src_path}, {e}')

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

