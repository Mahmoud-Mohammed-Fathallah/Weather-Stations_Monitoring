package com.example;

import com.example.BitCaskHandler.BitCask;
import com.example.ParquetHandler.ParquetProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DroppedConsumer {
    private static final String BOOTSTRAP_SERVERS = System.getenv("bootstrap.servers");
    private static final String TOPIC_NAME =  System.getenv("Dropped");

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));


        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String message = record.value();
                    System.out.println("Received Dropped Message : " + message);
                    JSONObject WeatherjsonObject = new JSONObject(message);
                    System.out.println("Dropped WeatherjsonObject: " + WeatherjsonObject);

                    int id = WeatherjsonObject.getInt("id");


                }
            }
        }catch (Exception e)
        {
            consumer.close();
            System.out.println(e);
        }
    }
}
