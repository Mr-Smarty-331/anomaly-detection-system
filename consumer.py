
import json
import logging
import joblib
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants and Configuration ---
# Kafka configuration
KAFKA_BROKER_URL = "kafka:29092"
KAFKA_TOPIC = "raw-data"

# Paths to the saved model and scaler artifacts from the training step.
MODEL_PATH = 'isolation_forest.joblib'
SCALER_PATH = 'scaler.joblib'


def main():
    logger.info("Starting data consumer...")

    consumer = None

    while not consumer:
        try:
            logger.info(f"Attempting to connect to Kafka at {KAFKA_BROKER_URL}...")
            consumer = KafkaConsumer(KAFKA_TOPIC,bootstrap_servers = [KAFKA_BROKER_URL], 
                                     group_id = 'anomaly-detector-group',
                                     value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                                     auto_offset_reset='earliest')
            
            logger.info("Successfully connected to Kafka and subscribed to topic '{}'.".format(KAFKA_TOPIC))

        except:
            logger.warning(f"Could not connect to Kafka at {KAFKA_BROKER_URL}. Retrying in 5 seconds...")
            time.sleep(5)

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

    try:
        for message in consumer:
            data_dict = message.value
            logger.info(f"Successfully deserialized message of type {type(data_dict)}.")
            logger.info(f"Received data: {data_dict}")


            value = data_dict['value']
            value_df = pd.DataFrame([[value]], columns=['value'])
            scaled_value_array = scaler.transform(value_df)
            scaled_value = scaled_value_array[0][0]

            logger.info(f"Original value: {value:.4f}, Scaled value: {scaled_value:.4f}")

            prediction_array = model.predict(scaled_value_array)
            
            # The result is a numpy array (e.g., array([-1]) or array([1])).
            # We extract the single integer value from it.
            prediction = int(prediction_array[0])

            is_anomaly = (prediction == -1)

            data_dict['is_anomaly'] = is_anomaly
            

            if is_anomaly:
                # Use a WARNING log level to make anomalies highly visible.
                logger.warning(f"ANOMALY DETECTED: {json.dumps(data_dict)}")
            else:
                # Use an INFO log level for normal operations.
                logger.info(f"Normal data processed: {json.dumps(data_dict)}")

    except KeyboardInterrupt:
        logger.info("Shutdown signal received. Closing consumer...")
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()