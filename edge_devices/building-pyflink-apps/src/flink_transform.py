import os
import logging
import json
import pickle
from kafka import KafkaConsumer, KafkaProducer
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
from models import Transaction

# Environment variables
RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")

# Load the trained model
MODEL_PATH = os.getenv("MODEL_PATH", "/tmp/src/fraud_model.pkl")
with open(MODEL_PATH, "rb") as model_file:
    model_data = pickle.load(model_file)

model: RandomForestClassifier = model_data["model"]
scaler: StandardScaler = model_data["scaler"]
label_encoders: dict = model_data["label_encoders"]

logging.info("Model, scaler, and encoders loaded successfully.")


def process_transaction(raw_data: str) -> str:
    """
    Process a single transaction JSON, predict if it's fraudulent, and add the `is_fraud` flag.
    Args:
        raw_data (str): JSON string of a transaction.
    Returns:
        str: JSON string with the `is_fraud` flag.
    """
    try:
        # Parse the raw JSON data
        transaction = Transaction.from_json(raw_data)

        # Prepare the data for prediction
        features = {
            "merchant": transaction.merchant,
            "category": transaction.category,
            "gender": transaction.gender,
            "amt": transaction.amt,
            "lat": transaction.lat,
            "long": transaction.long,
            "city_pop": transaction.city_pop,
            "job": transaction.job,
            "unix_time": transaction.unix_time,
            "merch_lat": transaction.merch_lat,
            "merch_long": transaction.merch_long,
        }

        # Encode categorical features
        for col, encoder in label_encoders.items():
            features[col] = encoder.transform([features[col]])[0]

        # Scale numerical features
        numerical_features = scaler.transform([[features[col] for col in features]])

        # Predict fraud
        is_fraud = int(model.predict(numerical_features)[0])
        transaction.is_fraud = is_fraud

        # Return the JSON with `is_fraud`
        return transaction.to_json()
    except Exception as e:
        logging.error(f"Failed to process transaction: {e}")
        return None


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"RUNTIME_ENV - {RUNTIME_ENV}, BOOTSTRAP_SERVERS - {BOOTSTRAP_SERVERS}")

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(5)

    if RUNTIME_ENV != "docker":
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        jar_files = ["flink-sql-connector-kafka-1.17.1.jar"]
        jar_paths = tuple(
            [f"file://{os.path.join(CURRENT_DIR, 'jars', name)}" for name in jar_files]
        )
        logging.info(f"adding local jars - {', '.join(jar_files)}")
        env.add_jars(*jar_paths)

    # Kafka Consumer configuration (listens to transactions topic)
    consumer = KafkaConsumer(
        "transactions", 
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="transaction-consumer-group",
        value_deserializer=lambda x: x.decode("utf-8")
    )

    # Kafka Producer configuration (produces to flagged-transactions topic)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda x: x.encode("utf-8")
    )

    # Dummy Job: a simple job that processes data and logs it (does nothing)
    def dummy_job():
        while True:
            # Simulate a dummy processing task that does nothing
            logging.info("Dummy job running. Doing nothing.")
            time.sleep(10)

    # Run the dummy job in parallel (you can run it as a background task)
    import threading
    threading.Thread(target=dummy_job, daemon=True).start()

    # Processing stream: process transactions, flag them, and send to Kafka
    def process_and_flag_transactions():
        for raw_data in consumer:
            logging.info("Processing transaction...")
            # Process the transaction
            flagged_transaction = process_transaction(raw_data.value)

            # If the transaction is flagged as fraud, send it to the flagged-transactions topic
            if flagged_transaction:
                flagged_data = json.loads(flagged_transaction)
                if flagged_data.get("is_fraud") == 1:
                    logging.info(f"Flagged transaction: {flagged_data}")
                    producer.send("flagged-transactions", value=flagged_transaction)
                else:
                    logging.info("Transaction is not fraudulent.")

    # Run the transaction processing in parallel
    process_and_flag_transactions()

    logging.info("Fraud Detection Job Started.")
    # Execute the Flink job (although no specific task is being done here, the job is simulated)
    env.execute("Fraud Detection and Flagging Job")


if __name__ == '__main__':
    main()
