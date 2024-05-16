package com.example.ParquetHandler;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ParquetProcessor {

    private static final int BATCH_SIZE = System.getenv("BATCH_SIZE") != null ? Integer.parseInt(System.getenv("BATCH_SIZE")) : 1000;
    static ConcurrentHashMap<Integer, List<GenericData.Record>> batchMap = new ConcurrentHashMap<>();
    static int size = 0;

    private GenericData.Record serializeJSONObject(JSONObject jsonObject, Schema schema) throws IOException {
        GenericData.Record record = new GenericData.Record(schema);
        //System.out.println("Schema: " + schema.toString());
        for (Schema.Field field : schema.getFields()) {
            Object value = jsonObject.get(field.name());
            if (value instanceof JSONObject && field.schema().getType() == Schema.Type.RECORD) {
                value = serializeJSONObject((JSONObject) value, field.schema());
            }
            record.put(field.name(), value);
        }

        return record;
    }


    private void writeToParquet(Schema schema, String FilesPath) throws IOException {


        String time = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH"));
        for (int stationId : batchMap.keySet()) {

            File dir = new File(FilesPath + "/parquet/station_" + stationId + "/" + time);
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                if (created) {
                    System.out.println("Directory was created!");
                } else {
                    System.out.println("Failed to create directory!");
                }
            }

            Path path = new Path(FilesPath + "/parquet/station_" + stationId + "/" + time + "/file.parquet");
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                    .<GenericData.Record>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build()) {
                for (GenericData.Record record : batchMap.get(stationId)) {
                    System.out.println("Writing record: " + record);

                    // Write the record to the Parquet file
                    writer.write(record);

                }


            }
        }
        batchMap.clear();


    }

    public static void buildParquetFiles(JSONObject jsonObject) throws IOException {

        String FilesPath  = System.getenv("PARQUET_PATH");
//│ WeatherjsonObject: {"batteryStatus":"Medium","statusTimestamp":1715471246772,"s_no":1,"weather":{"temperature":89,"humidity":22,"windSpeed":85},"id":1}                                                                        │
//        // Define your Avro schema
        Schema schema = new Schema.Parser().parse(
                "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"WeatherData\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"s_no\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"batteryStatus\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"statusTimestamp\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"weather\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"record\",\n" +
                "        \"name\": \"weather\",\n" +
                "        \"fields\": [\n" +
                "          {\n" +
                "            \"name\": \"humidity\",\n" +
                "            \"type\": \"int\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"temperature\",\n" +
                "            \"type\": \"int\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"windSpeed\",\n" +
                "            \"type\": \"int\"\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        System.out.println("Schema: " + schema.toString());
        // Create DataProcessor instance
        ParquetProcessor processor = new ParquetProcessor();
        GenericData.Record avroData = processor.serializeJSONObject(jsonObject, schema);
        int stationId = jsonObject.getInt("id");
        List<GenericData.Record> list = batchMap.getOrDefault(stationId, new ArrayList<>());
        list.add(avroData);
        batchMap.put(stationId, list);

        size++;

        if (size >= BATCH_SIZE) {
            processor.writeToParquet(schema, FilesPath);
            size = 0;
        }


    }
}

//    public static void main(String[] args) throws IOException {
//
//        String path = "/home/abdu/BigDataLabs/FinalP/central_station/src/main/java/com/example/ParquetHandler";
//
//        Random random = new Random();
//
//        for (int i = 0; i < 105; i++) {
//            // Generate a random station ID between 1 and 10
//            int stationId = random.nextInt(10) + 1;
//
//            // Create a new JSONObject for each record
//            JSONObject jsonObject = new JSONObject();
//
//            jsonObject.put("id", stationId);
//            jsonObject.put("s_no", i + 1);
//            jsonObject.put("battery_status", "medium");
//            jsonObject.put("status_timestamp", 1681521224 + i);
//            JSONObject weather = new JSONObject();
//            weather.put("humidity", 45 + random.nextInt(10));
//            weather.put("temperature", 75 + random.nextInt(10));
//            weather.put("wind_speed", 10 + random.nextInt(5));
//            jsonObject.put("weather", weather);
//           buildParquetFiles(jsonObject);
//        }
//
////        //for (int i = 1; i <= 10; i++) {
////
////          //  System.out.println("Reading parquet files for station " + i + " at time " + time);
////            ParquetReader<GenericRecord> reader = AvroParquetReader
////                    .<GenericRecord>builder(new Path("/home/abdu/BigDataLabs/FinalP/central_station/src/main/java/com/example/ParquetHandler/data/parquet/station_5/2024-05-12T01"))
////                    .withConf(new Configuration())
////                    .build();
////
////            GenericRecord record;
////            while ((record = reader.read()) != null) {
////                System.out.println(record);
////            }
////            reader.close();
//        }
//    }
////}
///home/abdu/BigDataLabs/FinalP/central_station/src/main/java/com/example/ParquetHandler/data/parquet/station_123/2024-05-11T23