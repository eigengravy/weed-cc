import os
import logging
import json
import pickle
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
from models import Transaction
import pandas as pd

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
            "cc_num" : transaction.cc_num,
            "merchant": transaction.merchant,
            "category": transaction.category,
            "gender": transaction.gender,
            "amt": transaction.amt,
            "zip": transaction.zip, 
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

        feature_df = pd.DataFrame([features])

        # Scale numerical features
        scaled_features = scaler.transform(feature_df)

        # Predict fraud
        is_fraud = int(model.predict(scaled_features)[0])
        transaction.is_fraud = is_fraud

        # Return the JSON with `is_fraud`
        logging.info(f"Sent transaction: {transaction.merchant}")
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
    env.set_parallelism(1)

    if RUNTIME_ENV != "docker":
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        jar_files = ["flink-sql-connector-kafka-1.17.1.jar"]
        jar_paths = tuple(
            [f"file://{os.path.join(CURRENT_DIR, 'jars', name)}" for name in jar_files]
        )
        logging.info(f"adding local jars - {', '.join(jar_files)}")
        env.add_jars(*jar_paths)

    # Kafka Source configuration
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_topics("transactions") \
        .set_group_id("transaction-consumer-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Kafka Sink configuration
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("flagged_transactions")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    # Define the stream pipeline
    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="KafkaSource"
    ).map(
        lambda raw_data: process_transaction(raw_data),  # Process the raw transaction data
        output_type=Types.STRING()  # Specify the output type as a string
    ).filter(
        lambda flagged_data: flagged_data is not None and json.loads(flagged_data).get("is_fraud") == 1
    )

    # Add sink to output the flagged transactions
    stream.sink_to(kafka_sink)

    # Execute the Flink job
    env.execute("Fraud Detection and Flagging Job")


if __name__ == '__main__':
    main()
