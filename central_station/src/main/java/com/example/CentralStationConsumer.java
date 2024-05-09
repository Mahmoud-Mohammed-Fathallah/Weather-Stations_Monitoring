package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CentralStationConsumer {

    private static final String BOOTSTRAP_SERVERS = System.getenv("bootstrap.servers");
    private static final String TOPIC_NAME = "my_first";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        List<Long> latencies = new ArrayList<>();

        try {
            while (true) {
                long currentTime1 = System.currentTimeMillis();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String message = record.value();
                    System.out.println("Received message: " + message);
                    long messageTimestamp = record.timestamp();
                    long currentTime = System.currentTimeMillis();
                    long latency = (currentTime - messageTimestamp);
                    latencies.add(latency);
                    System.out.println("Latency for message: " + latency + " ms");
                }
                long currentTime2 = System.currentTimeMillis();
                System.out.println("API response Time: "+(currentTime2-currentTime1)+" ms");
            }
        }catch (Exception e)
        {
            System.out.println(e);
        }
    }
}

