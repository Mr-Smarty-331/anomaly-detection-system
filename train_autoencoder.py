import logging
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, RepeatVector, TimeDistributed
import json

from producer import generate_normal_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_sequences(data, time_steps):
    X = []
    y = []
    for i in range(len(data) - time_steps):
        X.append(data.iloc[i:(i + time_steps)].values)
        y.append(data.iloc[i + time_steps].values) # or target sequence depending on autoencoder mapping
    return np.array(X), np.array(y)

def main():
    logging.info("starting training for lstm autoencoder")
    
    num_samples = 10000
    data_points = [generate_normal_data(i) for i in range (num_samples)]

    values = [dp['value'] for dp in data_points]

    df = pd.DataFrame(values,columns=['value'])

    scaler = StandardScaler()
    df['value_scaled'] = scaler.fit_transform(df[['value']])

    joblib.dump(scaler, 'scaler_autoencoder.joblib')
    logging.info("Scaler has been fitted and saved to 'scaler_autoencoder.joblib'.")

    TIME_STEPS = 20
    logging.info(f"Creating sequences with {TIME_STEPS} time steps...")

    X,y = create_sequences(df[['value_scaled']],TIME_STEPS)

    split_index = int(0.9 * len(X))

    X_train, X_val = X[:split_index], X[split_index:]
    y_train, y_val = y[:split_index], y[split_index:]

    logging.info(f"Training data shape: {X_train.shape}")
    logging.info(f"Validation data shape: {X_val.shape}")

    n_features = X_train.shape[2] 

    model = Sequential([
        # =================== Encoder ===================
        LSTM(128, activation='relu', input_shape=(TIME_STEPS, n_features), return_sequences=True),
        LSTM(64, activation='relu', return_sequences=False),
        # =================== Bridge ====================
        RepeatVector(TIME_STEPS),
        # =================== Decoder ===================
        LSTM(64, activation='relu', return_sequences=True),
        LSTM(128, activation='relu', return_sequences=True),
        # ================= Output Layer ================
        TimeDistributed(Dense(n_features))
    ])

    model.summary()

    model.compile(optimizer = 'adam',loss = 'mean_squared_error')

    history = model.fit(
        X_train, y_train,
        epochs=50,
        batch_size=32,
        validation_data=(X_val, y_val),
        shuffle=True
    )

    logging.info("training model complete")

    reconstructions = model.predict(X_val)
    reconstruction_errors = np.mean(np.abs(X_val - reconstructions), axis=(1, 2))
    threshold = np.percentile(reconstruction_errors, 99)
    logging.info(f"Calculated reconstruction error threshold: {threshold}")

    logging.info("Saving the trained model and threshold to disk...")
    
    # Save the Keras model to a single HDF5 file.
    model_path = 'lstm_autoencoder.h5'
    model.save(model_path)
    logging.info(f"Model saved successfully to '{model_path}'.")
    
    # Save the calculated threshold to a JSON file for easy loading later.
    threshold_path = 'threshold.json'
    with open(threshold_path, 'w') as f:
        json.dump({'threshold': threshold}, f)
    logging.info(f"Threshold saved successfully to '{threshold_path}'.")



if __name__ == "__main__" :
    main()