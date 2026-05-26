import json
import logging
import joblib
import pandas as pd
from kafka import KafkaConsumer,KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import sys

import os
from dotenv import load_dotenv
load_dotenv()

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants and Configuration ---
# InfluxDB configs
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "${INFLUX_TOKEN}")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "individual")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "anomaly-detection")

# Kafka configuration
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
RAW_DATA_TOPIC = os.getenv("RAW_DATA_TOPIC", "raw-data")
ANOMALIES_TOPIC = os.getenv("ANOMALIES_TOPIC", "anomalies")

# Paths to the saved model and scaler artifacts from the training step.
MODEL_PATH = 'isolation_forest.joblib'
SCALER_PATH = 'scaler.joblib'


def main():
    logger.info("Starting data consumer...")

# loading the consumer to the producers(all datapoints produced) data
    consumer = None
    while not consumer:
        try:
            logger.info(f"Attempting to connect to Kafka at {KAFKA_BROKER_URL}...")
            consumer = KafkaConsumer(RAW_DATA_TOPIC,bootstrap_servers = [KAFKA_BROKER_URL], 
                                     group_id = 'anomaly-detector-group',
                                     value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                                     auto_offset_reset='earliest')
            
            logger.info("Successfully connected to Kafka and subscribed to topic '{}'.".format(RAW_DATA_TOPIC))

        except:
            logger.warning(f"Could not connect to Kafka at {KAFKA_BROKER_URL}. Retrying in 5 seconds...")
            time.sleep(5)

# the producer(gives the output - flags by ml model - this will send to influxdb)
    producer = None
    # Use the same resilient connection loop for the producer.
    while not producer:
        try:
            logger.info(f"Attempting to connect Kafka producer at {KAFKA_BROKER_URL}...")
            # The producer needs to know how to serialize data into bytes.
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER_URL],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Successfully connected Kafka producer.")
        except NoBrokersAvailable:
            logger.warning(f"Could not connect Kafka producer. Retrying in 5 seconds...")
            time.sleep(5)

# loading the influxdb (storing the data points - data persistence)
    influxdb_client = None
    while not influxdb_client:
        try:
            logger.info(f"Attempting to connect to InfluxDB at {INFLUXDB_URL}...") 
            influxdb_client = InfluxDBClient(
                url=INFLUXDB_URL,
                token=INFLUXDB_TOKEN,
                org=INFLUXDB_ORG
            )
            write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
            health = influxdb_client.health()
            if health.status == "pass":
                logger.info("Successfully connected to InfluxDB and health check passed.")
            else:
                logger.error(f"InfluxDB health check failed with status: {health.status}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Could not connect to InfluxDB: {e}")
            sys.exit(1)

# loading the model
    try:
        # Load the pre-trained Isolation Forest model.
        logger.info(f"Loading model from {MODEL_PATH}...")
        model = joblib.load(MODEL_PATH)
        logger.info("Model loaded successfully.")

        # Load the pre-fitted StandardScaler.
        logger.info(f"Loading scaler from {SCALER_PATH}...")
        scaler = joblib.load(SCALER_PATH)
        logger.info("Scaler loaded successfully.")
    
    except FileNotFoundError as e:
        # If the files are not found,we log a critical error and exit.
        logger.error(f"Error loading model/scaler files: {e}")
        logger.error("Have you run the train_model.py script to generate the artifacts?")
        sys.exit(1)

    logger.info("Consumer is running. Waiting for messages...")

# sending the messages (from producer's marked flags via model) to influxdb 
    try:
        for message in consumer:
            # 1. Deserialize the message value into a dictionary.
            data_dict = message.value
            
            # 2. Preprocess the data for the model.
            value = data_dict['value']
            value_df = pd.DataFrame([[value]], columns=['value'])
            scaled_value_array = scaler.transform(value_df)
            
            # 3. Make a prediction.
            prediction_array = model.predict(scaled_value_array)
            prediction = int(prediction_array[0])

            # 4. Enrich the original data with the prediction result.
            is_anomaly = (prediction == -1)
            data_dict['is_anomaly'] = is_anomaly
            

            if is_anomaly:
                # Use a WARNING log level to make anomalies highly visible.
                producer.send(ANOMALIES_TOPIC, value=data_dict)
                logger.warning(f"ANOMALY DETECTED: {json.dumps(data_dict)}")
            else:
                # Use an INFO log level for normal operations.
                logger.info(f"Normal data processed: {json.dumps(data_dict)}")
            try:
                # 3. Create an InfluxDB Point object from our data.
                # A "Point" is a single data record in InfluxDB.
                point = Point("sensor_readings") .tag("sensor_id", data_dict["sensor_id"]) .field("value", data_dict["value"]).field("is_anomaly", data_dict["is_anomaly"]) .time(data_dict["timestamp"])

                # 4. Use the Write API to send the point to our bucket.
                # The write is synchronous, meaning this line will block until
                # the write is confirmed by the database.
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

            except Exception as e:
                # This catches potential errors during the write process,
                # such as network issues or InfluxDB being down.
                logger.error(f"Error writing to InfluxDB: {e}")

    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Closing consumer...")
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed.")
        if producer:
            # It's crucial to close the producer to ensure all buffered messages are sent.
            producer.close()
            logger.info("Kafka producer closed.")
        if influxdb_client:
            influxdb_client.close()
            logger.info("InfluxDB client closed.")

if __name__ == "__main__":
    main()