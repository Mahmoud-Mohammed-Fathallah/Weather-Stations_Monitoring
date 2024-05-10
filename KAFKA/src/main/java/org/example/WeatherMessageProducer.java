package org.example;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;

import java.util.Properties;
import java.util.Random;

public class WeatherMessageProducer {
    public static void main(String[] args) throws InterruptedException {
        // Configure Kafka producer properties
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  System.getenv("bootstrap.servers"));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        System.out.println(props);
        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random rand = new Random();
        long s_no=1;
        // Send messages indefinitely
        while (true) {
            // Create Target object
            weather target = new weather(rand.nextInt(100), rand.nextInt(100), rand.nextInt(100));

            // Create MOC message object
            MocMessage mocMessage = new MocMessage(1L,s_no, "low",System.currentTimeMillis(), target);

            // Randomly change battery status
            mocMessage.randomlyChangeBatteryStatus();

            // Serialize MOC message to JSON using Gson
            Gson gson = new Gson();
            String jsonMessage = gson.toJson(mocMessage);
            // Randomly drop messages at a 10% rate
            if (rand.nextDouble() < 0.10) {
                System.out.println("Message dropped: " + jsonMessage);
                continue; // Skip sending this message
            }

            // Construct Kafka message
            ProducerRecord<String, String> kafkaMessage = new ProducerRecord<>("station", "moc-key", jsonMessage);

            // Send Kafka message
            producer.send(kafkaMessage, (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("MOC message sent successfully");
                } else {
                    System.err.println("Error sending MOC message: " + e.getMessage());
                }
            });

            // Sleep for 1 second before sending the next message
            Thread.sleep(1000);
            s_no++;
        }

    }
}
