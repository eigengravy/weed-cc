package org.weedcc;

import com.opencsv.CSVReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.FileReader;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        String bootstrapServers = "192.168.171.249:9093"; // Update if needed
        String topicName = "card-transactions";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        String csvFilePath = "iot-producer/src/main/resources/dataset.csv"; // Update to the actual path

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties); CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                // Join CSV row into a single string (comma-separated)
                String row = String.join(",", nextLine);

                // Send row to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, row);
                producer.send(record);

                System.out.println("Sent: " + row);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Close Kafka producer

        System.out.println("CSV streaming completed.");
    }
}
