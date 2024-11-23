package org.example;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Properties;

public class FlinkKafkaConsumerExample {
    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.171.249:9093"); // Kafka broker
        properties.setProperty("group.id", "flink-consumer-group"); // Consumer group ID

        // Create Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("csv-topic") // Replace with your Kafka topic
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize as simple string
                .build();

        // Define the data stream from Kafka source
        DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Process the stream (optional)
        stream.map(value -> "Processed: " + value)
                .print(); // Print to the console, or add further processing

        // Execute the Flink job
        env.execute("Flink Kafka Consumer Job");
    }
}
