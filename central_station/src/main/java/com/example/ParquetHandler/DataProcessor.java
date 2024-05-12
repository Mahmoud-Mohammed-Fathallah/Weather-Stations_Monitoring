// package com.example.ParquetHandler;

// import org.apache.avro.Schema;
// import org.apache.avro.generic.GenericData;
// import org.apache.avro.generic.GenericRecord;
// import org.apache.avro.io.DatumWriter;
// import org.apache.avro.io.Encoder;
// import org.apache.avro.io.EncoderFactory;
// import org.apache.avro.specific.SpecificDatumWriter;
// import org.apache.hadoop.fs.Path;
// import org.apache.parquet.avro.AvroParquetWriter;
// import org.apache.parquet.hadoop.ParquetWriter;
// import org.elasticsearch.action.index.IndexRequest;
// import org.elasticsearch.client.RequestOptions;
// import org.elasticsearch.client.RestHighLevelClient;
// import org.elasticsearch.client.RestClient;
// import org.elasticsearch.client.RestClientBuilder;
// import org.json.JSONObject;

// import java.io.ByteArrayOutputStream;
// import java.io.IOException;

// public class DataProcessor {
//     private static final String ES_INDEX = "your_index_name";
//     private static final String ES_TYPE = "_doc";

//     private static RestHighLevelClient createClient(String[] hosts, int port) {
//         RestClientBuilder builder = RestClient.builder(
//                 new HttpHost(hosts[0], port, "http"),
//                 new HttpHost(hosts[1], port, "http")
//                 // add more hosts if needed
//         );
//         return new RestHighLevelClient(builder);
//     }

//     private static IndexRequest buildIndexRequest(JSONObject json) {
//         return new IndexRequest(ES_INDEX, ES_TYPE).source(json.toString(), XContentType.JSON);
//     }

//     public static byte[] serializeJSONObject(JSONObject jsonObject, Schema schema) throws IOException {
//         GenericRecord record = new GenericData.Record(schema);
//         // Populate the record fields with data from the JSONObject
//         // For example:
//         // record.put("field1", jsonObject.getString("field1"));
//         // record.put("field2", jsonObject.getInt("field2"));
        
//         ByteArrayOutputStream out = new ByteArrayOutputStream();
//         DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
//         Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
//         datumWriter.write(record, encoder);
//         encoder.flush();
//         out.close();
//         return out.toByteArray();
//     }

//     public static void sendToElasticsearch(JSONObject jsonObject) throws IOException {
//         RestHighLevelClient client = createClient(new String[]{"localhost"}, 9200);
//         IndexRequest request = buildIndexRequest(jsonObject);
//         client.index(request, RequestOptions.DEFAULT);
//         client.close();
//     }

//     public static void writeToParquet(byte[] avroData, Schema schema) throws IOException {
//         ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path("path/to/parquet/file"))
//                 .withSchema(schema)
//                 .build();

//         GenericRecord record = new GenericData.Record(schema);
//         // Populate the record fields with data from the Avro data
//         // For example:
//         // record.put("field1", avroData.getField1());
//         // record.put("field2", avroData.getField2());

//         writer.write(record);
//         writer.close();
//     }

//     public static void main(String[] args) throws IOException {
//         // Define your Avro schema
//         Schema schema = new Schema.Parser().parse("your_avro_schema_here");

//         // Example JSONObject
//         JSONObject jsonObject = new JSONObject();
//         jsonObject.put("field1", "value1");
//         jsonObject.put("field2", 123);

//         // Serialize JSONObject to Avro
//         byte[] avroData = serializeJSONObject(jsonObject, schema);

//         // Send to Elasticsearch
//         sendToElasticsearch(jsonObject);

//         // Write to Parquet
//         writeToParquet(avroData, schema);
//     }
// }
