package org.example;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;

import java.util.Properties;
import java.util.Random;

public class WeatherMessageRemote {
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
            // call the weather API
            String weatherData;
            try {
                weatherData = Adapter.fetchWeatherData();
            
            // Format the weather data
            String formattedMessage = Adapter.formatMessage(weatherData, s_no);
         
        
           
          // Randomly drop messages at a 10% rate
          if ( rand.nextInt(100) < 10) {
            System.out.println("Message dropped shape: " + formattedMessage);

            ProducerRecord<String, String> kafkaMessage = new ProducerRecord<>(System.getenv("Dropped"), "moc-key", formattedMessage);
            // Send Kafka message
            producer.send(kafkaMessage, (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("api dropped message sent successfully");
                } else {
                    System.err.println("Error sending MOC message: " + e.getMessage());
                }
            });

            continue; // Skip sending this message

        }

            // Construct Kafka message
            ProducerRecord<String, String> kafkaMessage = new ProducerRecord<>(System.getenv("TOPIC"), "moc-key", formattedMessage);
            // Send Kafka message
            producer.send(kafkaMessage, (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println("api  message sent successfully");
                    System.out.println("Message api shape: " + formattedMessage);

                } else {
                    System.err.println("Error sending MOC message: " + e.getMessage());
                }
            });

            // Sleep for 1 second before sending the next message
            Thread.sleep(1000);
            s_no++;
        }
     catch (Exception e) {
        System.err.println("Error fetching weather data: " + e.getMessage());
        e.printStackTrace();
    }
    }
}
}