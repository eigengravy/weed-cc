from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Set up the Table environment
    table_env = StreamTableEnvironment.create(env)
    
    # Define the Kafka source table using SQL DDL
    table_env.execute_sql("""
        CREATE TABLE kafka_source (
            `value` STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'csv-topic',               -- Replace with your Kafka topic name
            'properties.bootstrap.servers' = '192.168.171.249:9093',  -- Replace with your Kafka server address
            'properties.group.id' = 'flink-consumer-group',
            'format' = 'json',                          -- Specify the data format (e.g., 'json')
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

                    # .topic("csv-topic")  # Replace with your Kafka topic
                    # .property("bootstrap.servers", "192.168.171.249:9093")  # Replace with your Kafka server address
    # Read data from Kafka and print to console
    kafka_table = table_env.from_path("kafka_source")
    
    # Process and print data (optional)
    result_table = kafka_table.select(col("value"))
    result_table.execute().print()

if __name__ == '__main__':
    main()
