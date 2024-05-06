package org.example.Streaming;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.*;

import java.util.Properties;
import java.util.logging.*;

public class RainDetectorStreamProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "message-printer-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.21.0.3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Consume messages from the input topic
        KStream<String, String> inputStream = builder.stream("my_first");

        inputStream.filter((key, value) -> value.contains("\"humidity\":"))
                .filter((key, value) -> extractHumidity(value) > 70) // Change the condition as needed
                .mapValues(value -> "High humidity detected-it's raining : " + value) // Create new message
                .to("Rain", Produced.with(Serdes.String(), Serdes.String()));

        // Print messages to the console
        inputStream.foreach((key, value) -> System.out.println("Received message: " + value));



        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to cleanly close the Kafka Streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // Method to extract humidity value from the message
    private static int extractHumidity(String message) {
        int startIndex = message.indexOf("\"humidity\":") + "\"humidity\":".length();
        int endIndex = message.indexOf(",", startIndex);
        if (endIndex == -1) {
            endIndex = message.indexOf("}", startIndex);
        }
        return Integer.parseInt(message.substring(startIndex, endIndex).trim());
    }
}


