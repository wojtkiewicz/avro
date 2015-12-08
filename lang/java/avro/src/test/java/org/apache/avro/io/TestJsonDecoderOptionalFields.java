package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestJsonDecoderOptionalFields {
    static final Schema SCHEMA = new Schema.Parser().parse("{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"testSchema\",\n" +
            "  \"namespace\" : \"org.avro\",\n" +
            "  \"doc:\" : \"A basic schema for storing messages\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"username\",\n" +
            "    \"type\" : \"string\",\n" +
            "    \"doc\" : \"Name of the user account\"\n" +
            "  }, {\n" +
            "    \"name\" : \"message\",\n" +
            "    \"type\" : \"string\",\n" +
            "    \"doc\" : \"The content of the user's message\"\n" +
            "  }, {\n" +
            "    \"name\" : \"__timestamp\",\n" +
            "    \"type\" : [\"null\", \"long\"],\n" +
            "    \"doc\" : \"Epoch time in milliseconds (UTC)\",\n" +
            "    \"default\": null\n" +
            "  }, {\n" +
            "    \"name\": \"__metadata\",\n" +
            "    \"type\": [\"null\",{\"type\": \"map\", \"values\": \"string\"}],\n" +
            "    \"default\": null\n" +
            "  }]\n" +
            "}");

    JsonConverter converter = new JsonConverter();

    @Test
    public void testAllFieldsProvided() throws IOException {
        // given
        String json = "{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":1234," +
                "\"__metadata\":{\"foo\":\"bar\"}" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA);

        // then
        assertEquals(json, new String(converter.convertToJson(avro, SCHEMA)));
    }

    @Test
    public void testLastFieldMissing() throws IOException {
        // given
        String json = "{\n" +
                "    \"username\": \"mike\",\n" +
                "    \"message\": \"hello\"," +
                "    \"__timestamp\": 1234\n" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA);

        // then
        assertEquals("{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":1234," +
                "\"__metadata\":null" +
                "}", new String(converter.convertToJson(avro, SCHEMA)));
    }

    @Test
    public void testMiddleFieldMissing() throws IOException {
        // given
        String json = "{\n" +
                "    \"username\": \"mike\",\n" +
                "    \"message\": \"hello\"," +
                "    \"__metadata\": {\"yes\": \"123\"}\n" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA);

        // then
        assertEquals("{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":null," +
                "\"__metadata\":{\"yes\":\"123\"}" +
                "}", new String(converter.convertToJson(avro, SCHEMA)));
    }

    @Test
    public void testAllFieldsMissing() throws IOException {
        // given
        String json = "{\n" +
                "    \"username\": \"mike\",\n" +
                "    \"message\": \"hello\"" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA);

        // then
        assertEquals("{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":null," +
                "\"__metadata\":null" +
                "}", new String(converter.convertToJson(avro, SCHEMA)));
    }

    @Test
    public void testOptionalRecordIsMissing() throws IOException {
        // given
        Schema schemaFirstFieldAsOptionalRecord = new Schema.Parser().parse(
                "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"testSchema\",\n" +
                "  \"namespace\" : \"org.avro\",\n" +
                "  \"fields\": [\n" +
                "  {\n" +
                        "    \"default\": null,\n" +
                "    \"name\": \"user\",\n" +
                "    \"type\": [\"null\"," +
                "       {\"type\": \"record\", " +
                "        \"name\" : \"testRecord\",\n" +
                "        \"fields\" : [{\"name\" : \"username\",\"type\" : \"string\"}]}]" +
                "  },{\n" +
                "      \"name\": \"timestamp\",\n" +
                "      \"type\": \"long\"\n" +
                "  }]\n" +
                "}");

        String json = "{\"timestamp\": 1234}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), schemaFirstFieldAsOptionalRecord);

        // then
        assertEquals("{" +
                "\"user\":null," +
                "\"timestamp\":1234" +
                "}", new String(converter.convertToJson(avro, schemaFirstFieldAsOptionalRecord)));
    }

    @Test
    public void testFirstFieldAsOptionalMapIsMissing() throws IOException {
        // given
        Schema schemaFirstFieldOptionalMap = new Schema.Parser().parse(
                "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"testSchema\",\n" +
                "  \"namespace\" : \"org.avro\",\n" +
                "  \"fields\": [\n" +
                "  {\n" +
                "      \"name\": \"__metadata\",\n" +
                "      \"type\": [\"null\",{\"type\": \"map\", \"values\": \"string\"}],\n" +
                "      \"default\": null\n" +
                "  },{\n" +
                "      \"name\": \"timestamp\",\n" +
                "      \"type\": \"long\"\n" +
                "  }]\n" +
                "}");

        String json = "{\"timestamp\": 1234}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), schemaFirstFieldOptionalMap);

        // then
        assertEquals("{" +
                "\"__metadata\":null," +
                "\"timestamp\":1234" +
                "}", new String(converter.convertToJson(avro, schemaFirstFieldOptionalMap)));
    }

    static final Schema SCHEMA_RECORD = new Schema.Parser().parse("{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"testSchema\",\n" +
            "  \"namespace\" : \"org.avro\",\n" +
            "  \"doc:\" : \"A basic schema for storing messages\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"username\",\n" +
            "    \"type\" : \"string\",\n" +
            "    \"doc\" : \"Name of the user account\"\n" +
            "  }, {\n" +
            "    \"name\" : \"message\",\n" +
            "    \"type\" : \"string\",\n" +
            "    \"doc\" : \"The content of the user's message\"\n" +
            "  }, {\n" +
            "    \"name\" : \"__timestamp\",\n" +
            "    \"type\" : [\"null\", \"long\"],\n" +
            "    \"doc\" : \"Epoch time in milliseconds (UTC)\",\n" +
            "    \"default\": null\n" +
            "  }, {\n" +
            "    \"name\": \"__metadata\",\n" +
            "    \"type\": [\"null\"," +
                 "{\"type\": \"record\", " +
            "    \"name\" : \"testRecord\",\n" +
            "      \"fields\" : [{\"name\" : \"username\",\"type\" : \"string\",\"doc\":\"Name of the user account\"\n}]" +
                 "}]" +
            "  }]\n" +
            "}");

    @Test
    public void testRecordSupport() throws IOException {
        // given
        String json = "{" +
                "\"username\":\"mike\"," +
                "\"message\":\"hello\"," +
                "\"__timestamp\":1234," +
                "\"__metadata\":{\"username\":\"bar\"}" +
                "}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA_RECORD);

        // then
        assertEquals(json, new String(converter.convertToJson(avro, SCHEMA_RECORD)));
    }

    static final Schema SCHEMA_ARRAY = new Schema.Parser().parse("{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"testSchema\",\n" +
            "  \"namespace\" : \"org.avro\",\n" +
            "  \"doc:\" : \"A basic schema for storing messages\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\": \"__metadata\",\n" +
            "    \"type\": [\"null\",{\"type\": \"array\", \"items\": {\"type\":\"int\"}}],\n" +
            "    \"default\": null\n" +
            "  }]\n" +
            "}");

    @Test
    public void testArraySupport() throws IOException {
        // given
        String json = "{\"__metadata\":[1,2,3]}";

        // when
        byte[] avro = converter.convertToAvro(json.getBytes(), SCHEMA_ARRAY);

        // then
        assertEquals(json, new String(converter.convertToJson(avro, SCHEMA_ARRAY)));
    }

    class JsonConverter {
        public byte[] convertToAvro(byte[] data, Schema schema) throws IOException {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

            GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
            writer.write(readRecord(data, schema), encoder);
            encoder.flush();
            return outputStream.toByteArray();
        }

        private GenericData.Record readRecord(byte[] data, Schema schema) throws IOException {
            return new JsonGenericRecordReader().read(data, schema);
        }

        public byte[] convertToJson(byte[] avro, Schema schema) throws IOException {
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(avro, null);
            GenericRecord record = new GenericDatumReader<GenericRecord>(schema).read(null, binaryDecoder);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
            new GenericDatumWriter<GenericRecord>(schema).write(record, jsonEncoder);
            jsonEncoder.flush();
            return outputStream.toByteArray();
        }
    }

}
