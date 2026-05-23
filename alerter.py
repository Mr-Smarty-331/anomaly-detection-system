# since this is the alert microservice - we'll take in the anomaly flags as produced by consumer.py 

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import json 
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [Alerter] - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

KAFKA_BROKER_URL = "kafka:29092"
ANOMALIES_TOPIC = "anomalies"

def main():
    consumer = None
    logger.info("Starting alerter service...")
    
    while not consumer:
        try:
            consumer = KafkaConsumer(
                ANOMALIES_TOPIC,
                bootstrap_servers = [KAFKA_BROKER_URL],
                group_id = 'anomalies-alerter-group',
                value_deserializer = lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset = 'latest'
            )
            logger.info("Successfully connected to Kafka and subscribed to topic '{}'.".format(ANOMALIES_TOPIC))
        except NoBrokersAvailable:
            logger.warning("Could not connect to Kafka. Retrying in 5 seconds...")
            time.sleep(5)

    try:
        for message in consumer:
            anomaly_details = message.value
            # we'll add these alerts somewhere later
            logger.critical(f"🚨LISTEN ALL MA HOMIES CRITICAL ALERT RECEIVED 🚨\n{json.dumps(anomaly_details, indent=4)}")
    except KeyboardInterrupt:
        logger.info("Shutdown imminent💔")
    finally:
        if consumer:
            consumer.close()
        logger.info("Kafka consumer closed.")


if __name__ == "__main__":
    main()
