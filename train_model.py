
from venv import logger
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import logging

from producer import generate_normal_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


TRAINING_DATA_SIZE = 5000

MODEL_PATH = 'isolation_forest.joblib'
SCALER_PATH = 'scaler.joblib'

def main():
    print("Starting model training ...")
    logger.info(f"importing {TRAINING_DATA_SIZE} datapoints for training")

    training_data = []

    for i in range (TRAINING_DATA_SIZE) :
        data_point = generate_normal_data(i)
        training_data.append(data_point)

    df = pd.DataFrame(training_data)

    print("Generated DataFrame head:")
    print(df.head())
    print(f"\\nDataFrame shape: {df.shape}")

    logger.info("\nPreprocessing data with StandardScaler...")

    # Instantiate the StandardScaler.
    scaler = StandardScaler()

    df['value_scaled'] = scaler.fit_transform(df[['value']])

    logger.info("\nDataFrame head after scaling:")
    logger.info(df.head())
    logger.info("\nDescription of scaled data:")
    logger.info(df['value_scaled'].describe())

    print("\\nModel training script finished.")


if __name__ == "__main__":
    main()