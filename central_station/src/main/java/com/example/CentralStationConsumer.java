package com.example;

import com.example.ParquetHandler.ParquetProcessor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.example.BitCaskHandler.BitCask;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class CentralStationConsumer {

    private static final String BOOTSTRAP_SERVERS = System.getenv("bootstrap.servers");
    private static final String TOPIC_NAME =  System.getenv("TOPIC");
    private static final String STREAM_TOPIC_NAME =  System.getenv("Trigger");
    private static final String DROPPPED_TOPIC_NAME =  System.getenv("Dropped");

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (// dropped consumer
             KafkaConsumer<String, String> droppedConsumer = new KafkaConsumer<>(props)) {
            droppedConsumer.subscribe(Collections.singletonList(DROPPPED_TOPIC_NAME));
            try (// trigger consumer
                 KafkaConsumer<String, String> triggerConsumer = new KafkaConsumer<>(props)) {
                triggerConsumer.subscribe(Collections.singletonList(STREAM_TOPIC_NAME));

                // Main consumer
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList(TOPIC_NAME));
                BitCask bitCask = new BitCask();

                try {
                    while (true) {
                        ConsumerRecords<String, String> droppedRecords = droppedConsumer.poll(Duration.ofMillis(1000));
                        for (ConsumerRecord<String, String> record : droppedRecords) {
                            // send the record to elastic search index
                            JSONObject WeatherjsonObject = new JSONObject(record.value());
                            RestClientBuilder builder = RestClient.builder(new HttpHost(System.getenv("ELASTICSEARCH_HOSTS"), 9200, "http"));
                            RestClient restClient = builder.build();
                            Request request = new Request("POST", "/dropped/_doc");
                            request.setJsonEntity(WeatherjsonObject.toString());
                            Response response = restClient.performRequest(request);
                            System.out.println("Elastic response: "+response);
                            restClient.close();



                        }
                        // trigger consumer
                        ConsumerRecords<String, String> triggerRecords = triggerConsumer.poll(Duration.ofMillis(1000));
                        for (ConsumerRecord<String, String> record : triggerRecords) {

                            System.out.println("triggered message: " + record.value());
                        }

                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                        for (ConsumerRecord<String, String> record : records) {
                            String message = record.value();
                            // System.out.println("Received message: " + message);
                            JSONObject WeatherjsonObject = new JSONObject(message);
                            System.out.println("WeatherjsonObject: " + WeatherjsonObject);
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
                    droppedConsumer.close();
                    triggerConsumer.close();
                    System.out.println(e);
                }
            }
        }
    }
}
