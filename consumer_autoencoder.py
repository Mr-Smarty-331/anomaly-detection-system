# consumer_autoencoder.py

import json
import logging
import os
import sys
import time
from collections import deque

import joblib
import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from tensorflow.keras.models import load_model

# --- Configure logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants and Configuration ---
# Read configuration from environment variables
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
RAW_DATA_TOPIC = os.getenv("RAW_DATA_TOPIC", "raw-data")
ANOMALIES_TOPIC = os.getenv("ANOMALIES_TOPIC", "anomalies")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "${INFLUX_TOKEN}")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "individual")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "anomaly-detection")

# --- Model and Artifact Paths ---
MODEL_PATH = 'lstm_autoencoder.h5'
SCALER_PATH = 'scaler_autoencoder.joblib'
THRESHOLD_PATH = 'threshold.json'
TIME_STEPS = 20  # This MUST match the value used during training

# --- Helper Functions for Resilient Connections (Same as original consumer) ---
def create_kafka_producer():
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
    return producer
    
def create_kafka_consumer(topic):
    consumer = None
    while not consumer:
        try:
            logger.info(f"Attempting to connect to Kafka at {KAFKA_BROKER_URL}...")
            consumer = KafkaConsumer(topic,bootstrap_servers = [KAFKA_BROKER_URL], 
                                     group_id = 'anomaly-detector-group',
                                     value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                                     auto_offset_reset='earliest')
            
            logger.info("Successfully connected to Kafka and subscribed to topic '{}'.".format(topic))

        except Exception as e:
            logger.warning(f"Could not connect to Kafka at {KAFKA_BROKER_URL}. Retrying in 5 seconds...")
            time.sleep(5)
    return consumer

def create_influxdb_client():
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
    return influxdb_client,write_api

# --- Main Application Logic ---
if __name__ == "__main__":
    logging.info("Starting Autoencoder Consumer Service...")

    # --- Load the saved model and artifacts ---
    logging.info(f"Loading model from {MODEL_PATH}...")
    model = load_model(MODEL_PATH)
    logging.info("Model loaded successfully.")

    logging.info(f"Loading scaler from {SCALER_PATH}...")
    scaler = joblib.load(SCALER_PATH)
    logging.info("Scaler loaded successfully.")

    logging.info(f"Loading anomaly threshold from {THRESHOLD_PATH}...")
    with open(THRESHOLD_PATH, 'r') as f:
        threshold_data = json.load(f)
        anomaly_threshold = threshold_data['threshold']
    logging.info(f"Anomaly threshold loaded: {anomaly_threshold}")

    # --- Initialize connections ---
    consumer = create_kafka_consumer(RAW_DATA_TOPIC)
    producer = create_kafka_producer()
    influx_client, write_api = create_influxdb_client()

    # --- The Sliding Window: A crucial component for time-series inference ---
    # We use a deque, which is a highly efficient list-like data structure.
    # By setting maxlen, the deque automatically discards the oldest item
    # when a new item is added, creating a perfect sliding window.
    data_window = deque(maxlen=TIME_STEPS)
    logging.info(f"Initialized a data window with a size of {TIME_STEPS}.")

    try:
        logging.info("Starting to consume messages...")
        for message in consumer:
            # 1. Deserialize and Extract Data
            # data_point = json.loads(message.value.decode('utf-8'))
            data_point = message.value
            value = data_point['value']

            # 2. Scale the new data point
            # Note the double brackets [[value]] to create a 2D array, as scaler expects.
            scaled_value = scaler.transform([[value]])[0][0]

            # 3. Append to our sliding window
            data_window.append(scaled_value)

            is_anomaly = False # Default to False

            # 4. Perform Inference ONLY if the window is full
            if len(data_window) == TIME_STEPS:
                # Convert the deque to a NumPy array and reshape for the model
                # The model expects a 3D array: (samples, timesteps, features)
                # Here, we have 1 sample (our window), TIME_STEPS timesteps, and 1 feature.
                sequence = np.array(data_window).reshape(1, TIME_STEPS, 1)

                # Get the model's reconstruction of the sequence
                reconstruction = model.predict(sequence)

                # Calculate the reconstruction error (MAE)
                reconstruction_error = np.mean(np.abs(sequence - reconstruction))

                # 5. Classify based on the threshold
                if reconstruction_error > anomaly_threshold:
                    is_anomaly = True
                    logging.warning(f"ANOMALY DETECTED! Error: {reconstruction_error:.4f} > Threshold: {anomaly_threshold:.4f}")

                    # 6. Publish anomaly to the alerts topic
                    enriched_data = {**data_point, 'reconstruction_error': reconstruction_error, 'is_anomaly': True}
                    # producer.send(ANOMALIES_TOPIC, json.dumps(enriched_data).encode('utf-8'))
                    producer.send(ANOMALIES_TOPIC, enriched_data)
                    producer.flush()
                else:
                    logging.info(f"Normal data point. Error: {reconstruction_error:.4f} <= Threshold: {anomaly_threshold:.4f}")

            # 7. Persist to InfluxDB
            # We persist every point, but only after the window is full can we have a valid anomaly status.
            point = Point("sensor_readings") \
                .tag("sensor_id", data_point['sensor_id']) \
                .field("value", float(value)) \
                .field("is_anomaly", is_anomaly) \
                .time(data_point['timestamp'])
            
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            logging.info(f"Data point written to InfluxDB. Anomaly status: {is_anomaly}")

    except KeyboardInterrupt:
        logging.info("Consumer process interrupted. Shutting down.")
    finally:
        if consumer:
            consumer.close()
        if producer:
            producer.close()
        if influx_client:
            influx_client.close()
        logging.info("Connections closed. Goodbye.")
