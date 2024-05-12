package com.example;

import com.example.ParquetHandler.ParquetProcessor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import com.example.BitCaskHandler.BitCask;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class CentralStationConsumer {

    private static final String BOOTSTRAP_SERVERS = System.getenv("bootstrap.servers");
    private static final String TOPIC_NAME =  System.getenv("TOPIC");

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        BitCask bitCask = new BitCask();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String message = record.value();
                    System.out.println("Received message: " + message);
                    JSONObject WeatherjsonObject = new JSONObject(message);
                    System.out.println("WeatherjsonObject: " + WeatherjsonObject);
                    // to access nested object:===> obj.getJSONObject("objectkey")
                    // ex: humidity = weatherjsonObject.getJSONObject("weather").getInt("humidity")
                    int id = WeatherjsonObject.getInt("id");
                    bitCask.writeRecordToActiveFile(id, message);
                    System.out.println("testing reading from bitcask:");
                    System.out.println("the latest value for id = "+id +" is: "+bitCask.readRecordForKey(id));
                    // archive the record in parquet file
                    ParquetProcessor.buildParquetFiles(WeatherjsonObject);

                }
            }
        }catch (Exception e)
        {
            bitCask.close();
            consumer.close();
            System.out.println(e);
        }
    }
}

