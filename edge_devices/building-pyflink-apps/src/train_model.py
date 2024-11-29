import pandas as pd
import numpy as np
import logging
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import LabelEncoder, StandardScaler


def preprocess_data(csv_file):
    """
    Preprocess the data from the CSV file for training.
    Args:
        csv_file (str): Path to the CSV file.
    Returns:
        X (np.ndarray): Features array.
        y (np.ndarray): Target array.
    """
    # Load the data
    data = pd.read_csv(csv_file)

    # Drop unnecessary columns
    drop_columns = ["trans_date_trans_time", "first", "last", "street", "city", "state", "dob", "trans_num"]
    data = data.drop(columns=drop_columns)

    # Encode categorical features
    categorical_columns = ["merchant", "category", "gender", "job"]
    label_encoders = {}
    for col in categorical_columns:
        le = LabelEncoder()
        data[col] = le.fit_transform(data[col])
        label_encoders[col] = le

    # Separate features and target
    X = data.drop(columns=["is_fraud"])
    X = pd.DataFrame(data=data, columns=["cc_num", "merchant",
            "category",
            "gender",
            "amt",
            "zip", 
            "lat",
            "long",
            "city_pop",
            "job",
            "unix_time",
            "merch_lat",
            "merch_long",
                                         ])
    y = data["is_fraud"]
    print(X.head())

    # Standardize numerical features
    scaler = StandardScaler()
    X = scaler.fit_transform(X)

    return X, y, scaler, label_encoders


def train_model(X, y):
    """
    Train a RandomForestClassifier on the given dataset.
    Args:
        X (np.ndarray): Features array.
        y (np.ndarray): Target array.
    Returns:
        model: Trained RandomForestClassifier.
    """
    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train the model
    model = RandomForestClassifier(random_state=42, n_estimators=100)
    model.fit(X_train, y_train)

    # Evaluate the model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logging.info(f"Model accuracy: {accuracy}")
    logging.info(f"Classification report:\n{classification_report(y_test, y_pred)}")

    return model


def save_model(model, scaler, label_encoders, model_file="models/fraud_model.pkl"):
    """
    Save the trained model, scaler, and encoders to a file.
    Args:
        model: Trained model.
        scaler: Fitted StandardScaler.
        label_encoders (dict): Fitted label encoders.
        model_file (str): Path to save the model.
    """
    with open(model_file, "wb") as f:
        pickle.dump({"model": model, "scaler": scaler, "label_encoders": label_encoders}, f)
    logging.info(f"Model saved to {model_file}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Path to the CSV file
    csv_path = "fraudTest.csv"

    # Preprocess the data
    logging.info("Preprocessing the data...")
    X, y, scaler, label_encoders = preprocess_data(csv_path)

    # Train the model
    logging.info("Training the model...")
    model = train_model(X, y)

    # Save the model
    save_model(model, scaler, label_encoders)
