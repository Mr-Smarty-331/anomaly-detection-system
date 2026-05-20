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

    logger.info("/nInstantiating the isolation forest model....")


    model = IsolationForest(
        n_estimators=100,
        contamination='auto',
        random_state=42
    )

    logger.info(f"Model instantiated: {model}")

    logger.info("\\nTraining the model on the scaled data...")

    # The model expects a 2D array-like input, so we use the double-bracket
    model.fit(df[['value_scaled']])
    print("\\nModel training script finished.")

    logger.info(f"\\nSaving the trained model to {MODEL_PATH}...")
    # joblib.dump is used to serialize the Python object into a file.
    # We save our trained 'model' object to the path specified in our constant.
    joblib.dump(model, MODEL_PATH)
    logger.info("Model saved successfully.")


    logger.info(f"\\nSaving the scaler to {SCALER_PATH}...")
    # It is crucial to save the scaler as well, so we can use the exact same
    # scaling transformation on the live data in our consumer.
    joblib.dump(scaler, SCALER_PATH)
    logger.info("Scaler saved successfully.")

if __name__ == "__main__":
    main()