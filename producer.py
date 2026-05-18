import time
import json
import random
import math 
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

ANOMALY_PROBABILITY = 0.05

def generate_normal_data(counter:int) -> dict: #input a single argument : counter and output a dict of required json schems type of data : timestamp,srrverid,value
    sine_value = 50 + 40*math.sin(0.1 * counter)

    noise = random.uniform(-1.5,1.5)

    final_value = sine_value + noise

    timestamp = datetime.utcnow().isoformat() + "Z"

    data_point = {
        "timestamp": timestamp,
        "sensor_id": "sensor-001",
        "value": final_value
    }

    return data_point

def generate_anomalous_data(counter:int) -> dict:
    anomaly_type = random.choice("spike", "flatline")
    if anomaly_type == "spike": #spike
        # This simulates a sudden, sharp, and unexpected event.
        final_value = random.choice([random.uniform(150, 160), random.uniform(-60, -50)])
    else:  # flatline
        # This simulates a sensor failure or a process that has stalled.
        final_value = random.choice([0.0, 100.0])
    timestamp = datetime.utcnow().isoformat() + "Z"

    data_point = {
        "timestamp": timestamp,
        "sensor_id": "sensor-001",
        "value": final_value
    }
    return data_point

def main():
    print("Starting data producer...")

    KAFKA_BROKER_URL = "kafka:29092"

    # next will be a resilient connection loop
    while not producer:
        try:
            # Instantiate the KafkaProducer.
            producer = KafkaProducer(
                # The list of broker addresses to connect to.
                bootstrap_servers=[KAFKA_BROKER_URL],

                value_serializer=lambda v: json.dumps(v).encode('utf-8'),

                acks='all',
                # The number of times to retry sending a message if it fails.
                retries=5
            )
            print("Successfully connected to Kafka.")

        except NoBrokersAvailable:
            print(f"Could not connect to Kafka at {KAFKA_BROKER_URL}. Retrying in 5 seconds...")
            time.sleep(5)

    counter = 0
    try:
        # It will run indefinitely until the script is manually stopped (e.g., with Ctrl+C).
        while True:
            if random.random() < ANOMALY_PROBABILITY:
                # If the number is within our probability threshold, generate an anomaly
                data_point = generate_anomalous_data(counter)
                print(f"*** Anomaly Generated: {data_point} ***")
            else:
                # Otherwise, generate a normal data point
                data_point = generate_normal_data(counter)
                print(f"Generated data: {data_point}")

            time.sleep(1)

            # Increment the counter for the next iteration of the sine wave.
            counter += 1

    except KeyboardInterrupt:
    # This block catches the KeyboardInterrupt exception (Ctrl+C).
        print("\nShutting down producer...")
    finally:

        if producer:
            print("Closing Kafka producer.")
            # producer.flush() will block until all asynchronous messages are sent.
            producer.flush()
            # Closes the producer connection.
            producer.close()
            print("Producer closed.")

#____________________________________________#
if __name__ == "__main__":
    main()